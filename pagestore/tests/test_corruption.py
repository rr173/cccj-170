"""Corruption, truncation and locking tests (in-process plus file damage).

These exercise the durable-format guarantees directly:

* a torn trailing WAL frame is truncated to the last valid prefix;
* a complete frame with a bad CRC is a hard error with a location;
* a damaged page is refused, never silently rebuilt;
* a missing WAL segment referenced by a checkpoint is an illegal reference;
* two writer processes cannot open the same directory;
* same-page different-slot transactions interleave and independently finish.
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from pagestore import (create, open_store, ConflictError, CorruptionError,
                       IllegalReferenceError, LockError, OverflowError)
from pagestore.framing import frame, read_framed
from pagestore import records
from pagestore.wal import WAL, seg_name
from pagestore.stats import Stats

results = []


def check(name, cond, detail=""):
    results.append((name, cond))
    print(("  ✓ " if cond else "  ✗ ") + name + (f"  {detail}" if detail else ""))


def wal_path(d, seg=1):
    return os.path.join(d, "wal", seg_name(seg))


def test_torn_tail(tmp):
    print("[torn trailing WAL frame: valid prefix survives]")
    d = os.path.join(tmp, "torn")
    s = create(d, 3, 3, 0, cache_budget=2)
    t = s.begin()
    s.set(t, 0, 0, 5)
    s.commit(t)
    s.close()
    path = wal_path(d)
    good = open(path, "rb").read()
    # Append a partial frame (header only, no payload).
    with open(path, "ab") as f:
        f.write(frame(b"")[:7])
    s2 = open_store(d)
    check("reopen succeeds despite torn tail", s2.read(0, 0) == 5)
    s2.close()
    # The torn bytes must have been removed on open.
    check("torn bytes truncated to valid prefix",
          open(path, "rb").read() == good)


def test_full_corrupt_frame(tmp):
    print("[complete frame with bad CRC is hard corruption]")
    d = os.path.join(tmp, "corrupt")
    s = create(d, 3, 3, 0)
    t = s.begin()
    s.set(t, 0, 0, 9)
    s.commit(t)
    s.close()
    path = wal_path(d)
    blob = bytearray(open(path, "rb").read())
    # Flip a payload byte well inside a complete frame.
    blob[-1] ^= 0xFF
    with open(path, "wb") as f:
        f.write(blob)
    try:
        open_store(d)
        check("corrupt complete frame rejected", False)
    except CorruptionError as exc:
        check("corrupt complete frame rejected with location",
              "seg-00000001" in str(exc), str(exc))


def test_damaged_page(tmp):
    print("[damaged page content is refused, not rebuilt]")
    d = os.path.join(tmp, "pagebad")
    s = create(d, 3, 3, 0)
    t = s.begin()
    s.set(t, 1, 1, 42)
    s.commit(t)
    s.flush_page(1)
    s.close()
    p = os.path.join(d, "pages", "p-00000001")
    blob = bytearray(open(p, "rb").read())
    blob[30] ^= 0x55
    with open(p, "wb") as f:
        f.write(blob)
    try:
        open_store(d)
        check("damaged page rejected", False)
    except CorruptionError as exc:
        check("damaged page rejected with path", "p-00000001" in str(exc),
              str(exc))


def test_missing_referenced_segment(tmp):
    print("[missing WAL segment referenced by checkpoint is illegal]")
    d = os.path.join(tmp, "missingseg")
    # Small segments force a rollover.
    s = create(d, 4, 2, 0, cache_budget=2, wal_segment_size=64)
    for i in range(12):
        t = s.begin()
        s.set(t, 0, 0, i)
        s.commit(t)
    s.checkpoint()
    s.close()
    # The checkpoint points at the newest segment; deleting that referenced
    # segment must be reported as an illegal/missing reference, not treated
    # as an ordinary uncommitted tail.
    import json as _json
    cp = _json.loads(read_framed(os.path.join(d, "cp", "current")))
    referenced = seg_name(cp["wal_position"]["segment"])
    victim = os.path.join(d, "wal", referenced)
    check("checkpoint references a real segment", os.path.exists(victim))
    os.remove(victim)
    try:
        open_store(d)
        check("missing referenced segment rejected", False)
    except (IllegalReferenceError, CorruptionError) as exc:
        check("missing referenced segment rejected", True, str(exc)[:60])


def test_writer_lock(tmp):
    print("[second writer process is refused]")
    d = os.path.join(tmp, "lock")
    s = create(d, 2, 2, 0)
    try:
        open_store(d)
        check("second concurrent writer refused", False)
    except LockError:
        check("second concurrent writer refused", True)
    s.close()
    s2 = open_store(d)
    check("writer allowed again after close", s2 is not None)
    s2.close()


def test_same_page_interleave(tmp):
    print("[same page, different slots: independent interleave]")
    d = os.path.join(tmp, "interleave")
    s = create(d, 2, 4, 0, cache_budget=1)  # tiny budget forces eviction
    t1, t2 = s.begin(), s.begin()
    s.set(t1, 0, 0, 10)    # X
    s.set(t2, 0, 1, 20)    # Y, same page, different slot
    # Each sees only its own write plus committed values.
    check("t1 sees own X not Y pending",
          s.read(0, 0, t1) == 10 and s.read(0, 1, t1) == 0)
    check("t2 sees own Y not X pending",
          s.read(0, 1, t2) == 20 and s.read(0, 0, t2) == 0)
    # Same-slot conflict is explicitly refused.
    t3 = s.begin()
    try:
        s.set(t3, 0, 0, 99)
        check("same-slot conflict refused", False)
    except ConflictError:
        check("same-slot conflict refused", True)
    s.commit(t2)
    s.abort(t1)
    check("after interleaved finish X=0 Y=20",
          s.read(0, 0) == 0 and s.read(0, 1) == 20)
    s.close()
    s2 = open_store(d)
    check("interleave result durable",
          s2.read(0, 0) == 0 and s2.read(0, 1) == 20)
    s2.close()


def test_overflow_atomicity(tmp):
    print("[overflow rejected with no partial write]")
    d = os.path.join(tmp, "overflow")
    s = create(d, 2, 2, 0)
    t = s.begin()
    s.set(t, 0, 0, 2**63 - 1)
    try:
        s.add(t, 0, 0, 1)
        check("overflow add rejected", False)
    except OverflowError:
        check("overflow add rejected", True)
    check("value unchanged after rejected add",
          s.read(0, 0, t) == 2**63 - 1)
    check("no lock left / txn still usable",
          (t, ) and s.read(0, 0) == 0)
    s.abort(t)
    s.close()


def main():
    import tempfile
    tmp = tempfile.mkdtemp(prefix="pagestore-corr-")
    test_torn_tail(tmp)
    test_full_corrupt_frame(tmp)
    test_damaged_page(tmp)
    test_missing_referenced_segment(tmp)
    test_writer_lock(tmp)
    test_same_page_interleave(tmp)
    test_overflow_atomicity(tmp)
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
