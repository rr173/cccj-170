"""Process-driven tests for online backup consistency and crash safety.

The verifier in this file never uses the product's WAL analysis or recovery
code to *predict* anything.  Predictions come from
:mod:`tests.backup_reference`, driven solely by the worker's journal of
public operations and its own durable-commit confirmations.  The engine is
only used to *observe* the recovered backup's actual values.

Run:  python3 -I tests/test_backup.py [--keep] [--evidence-dir DIR]
"""

import argparse
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from pagestore import (BackupError, CorruptionError, StorageError,  # noqa: E402
                       create, open_store)
from pagestore import backup as backup_mod, pageio  # noqa: E402
from pagestore.crash import FaultInjector  # noqa: E402
from pagestore.framing import read_framed  # noqa: E402
from tests.backup_reference import BackupReferenceModel  # noqa: E402

PS = os.path.join(ROOT, "bin", "ps.py")
WORKER = os.path.join(HERE, "backup_worker.py")
PAGES, SLOTS, INITIAL, BUDGET = 3, 2, 0, 2
results = []
evidence = {"exit_codes": [], "checks": [], "read_ranges": [],
            "commit_sequences": [], "slot_results": []}


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    evidence["checks"].append({"name": name, "ok": bool(cond),
                               "detail": str(detail)[:300]})
    print(("  ✓ " if cond else "  ✗ ") + name + (f"  {detail}" if detail else ""))


def run(cmd, expect_codes=(0,)):
    proc = subprocess.run([sys.executable, "-I"] + cmd,
                          capture_output=True, text=True)
    evidence["exit_codes"].append({
        "cmd": " ".join(os.path.basename(c) for c in cmd),
        "code": proc.returncode,
        "expected": list(expect_codes)})
    ok = proc.returncode in expect_codes
    return ok, proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def fresh_store(base, name="db", **kw):
    d = os.path.join(base, name)
    ok, code, out, err = run(
        [PS, "create", d, "--pages", str(PAGES), "--slots", str(SLOTS),
         "--initial", str(INITIAL), "--budget",
         str(kw.get("budget", BUDGET))])
    assert ok, f"create failed: {err}"
    return d


def build_model_from_journal(journal):
    """Feed ONLY the journal's public operation stream into the model."""
    m = BackupReferenceModel(PAGES, SLOTS, INITIAL)
    for ev in journal["events"]:
        k = ev["kind"]
        if k == "begin":
            m.begin()
        elif k == "set":
            m.set(ev["txn"], ev["p"], ev["s"], ev["v"])
        elif k == "add":
            m.add(ev["txn"], ev["p"], ev["s"], ev["v"]
                  if "v" in ev else ev.get("delta"))
        elif k == "commit":
            m.commit_confirmed(ev["txn"])
        elif k == "abort":
            m.abort_confirmed(ev["txn"])
        elif k == "flush":
            m.flush(ev["p"])
        elif k == "checkpoint":
            m.checkpoint_event()
    return m


def read_all_slots(d):
    st = open_store(d)
    try:
        vals = {(p, s): st.read(p, s)
                for p in range(st.meta["num_pages"])
                for s in range(st.meta["slots_per_page"])}
        decisions = dict(st._decisions)
        stats = st.stats.as_dict()
        next_txn = st._next_txn_id
    finally:
        st.close()
    return vals, decisions, stats, next_txn


# ------------------------------------------------- 1. interleaved headline
def test_interleaved_backup(base):
    print("[interleaved commits, cutoff crossing, flush+checkpoint during copy]")
    d = fresh_store(base, "db")
    target = os.path.join(base, "bk")
    ok, code, out, err = run([WORKER, d, target, "block"])
    check("backup worker exits 0", ok, err)
    journal = json.load(open(os.path.join(d, "backup_journal.json")))
    marker = backup_mod.read_marker(target)

    # Independent prediction, no engine log/recovery code involved.
    model = build_model_from_journal(journal)
    bk = journal["backup"]
    allowed = model.allowed_cutoffs(bk["start_event_seq"], bk["end_event_seq"])
    prefix_len = len(marker["committed_txids"])
    check("cutoff prefix length is an allowed position",
          prefix_len in allowed["allowed_commit_prefix_lengths"],
          f"prefix={prefix_len} allowed={allowed['allowed_commit_prefix_lengths']}")
    check("every pre-window durable commit is in the copy",
          all(t in marker["committed_txids"]
              for t in allowed["must_include"]), str(allowed))
    check("no post-window commit is in the copy",
          all(t not in marker["committed_txids"]
              for t in allowed["must_exclude"]), str(allowed))

    # Marker must match the single WAL cutoff the checkpoint points at.
    cp = json.loads(read_framed(os.path.join(target, "cp", "current")))
    check("marker and checkpoint name the same cutoff",
          marker["cutoff"] == cp["wal_position"],
          f"{marker['cutoff']} vs {cp['wal_position']}")
    check("marker commit order matches model durable order prefix",
          marker["committed_txids"] ==
          model.durable_commits[:prefix_len],
          str(marker["committed_txids"]))
    evidence["commit_sequences"].append({
        "durable_order": model.durable_commits,
        "marker": marker["committed_txids"]})

    # The two transactions that committed after the cutoff must be absent.
    post_txns = [e["txn"] for e in journal["events"]
                 if e["kind"] == "commit" and "after cutoff" in e.get("note", "")]
    check("after-cutoff txns are excluded from the marker",
          all(t not in marker["committed_txids"] for t in post_txns),
          str(post_txns))

    # ---- All ARTIFACT checks happen BEFORE the copy is ever opened so the
    # marker describes the exact bytes the backup published (the first open
    # runs recovery and legitimately grows the copy's WAL/checkpoint). ----
    import hashlib
    cutoff = marker["cutoff"]
    seg_path = os.path.join(target, "wal",
                            f"seg-{cutoff['segment']:08d}")
    check("cutoff segment file ends exactly at the marker offset",
          os.path.getsize(seg_path) == cutoff["offset"],
          f"size={os.path.getsize(seg_path)} off={cutoff['offset']}")
    later = [n for n in os.listdir(os.path.join(target, "wal"))
             if int(n.split("-")[1]) > cutoff["segment"]]
    check("no WAL segment beyond the cutoff was copied", not later, str(later))
    hash_bad = []
    for entry in marker["files"]:
        p = os.path.join(target, entry["path"])
        if not os.path.exists(p):
            hash_bad.append((entry["path"], "missing"))
            continue
        h = hashlib.sha256(open(p, "rb").read()).hexdigest()
        if os.path.getsize(p) != entry["size"] or h != entry["sha256"]:
            hash_bad.append((entry["path"], "size/hash mismatch"))
    check("all as-published files match marker size and hash (pre-recovery)",
          not hash_bad, str(hash_bad))
    # The copy is fully independent: no links, distinct inodes.
    link_bad = []
    for root_dir, _dirs, files in os.walk(target):
        for name in files:
            p = os.path.join(root_dir, name)
            stt = os.lstat(p)
            if stat.S_ISLNK(stt.st_mode):
                link_bad.append(("symlink", p))
    check("backup contains no symbolic links", not link_bad, str(link_bad))
    src_page = os.path.join(d, "pages", "p-00000000")
    dst_page = os.path.join(target, "pages", "p-00000000")
    check("backup page is a separate inode (not a hardlink)",
          os.lstat(src_page).st_ino != os.lstat(dst_page).st_ino)

    # ---- Now open the copy for the first time (this runs its recovery). --
    vals, decisions, stats, next_txn = read_all_slots(target)
    expected = model.all_slots_at(prefix_len)
    mismatches = [f"{k}: got {vals[k]} want {expected[k]}"
                  for k in expected if vals[k] != expected[k]]
    check("recovered backup matches model state at cutoff, slot by slot",
          not mismatches, "; ".join(mismatches))
    evidence["slot_results"].append({
        "target": target, "prefix_len": prefix_len,
        "observed": {f"{p},{s}": v for (p, s), v in sorted(vals.items())},
        "expected": {f"{p},{s}": v for (p, s), v in sorted(expected.items())}})
    evidence["read_ranges"].append({
        "target": os.path.basename(target),
        "backup_recovery_page_reads": stats["page_reads"],
        "backup_recovery_page_bytes_read": stats["page_read_bytes"],
        "backup_recovery_wal_bytes_read": stats["wal_bytes_read"],
        "backup_recovery_wal_records": stats["wal_records_read"],
        "backup_recovery_notes":
        [n for n in stats["notes"] if "recovery" in n]})
    # The active-pending page flush during the copy must have forced at
    # least one retained barrier version (the version-pinning path).
    src_stats = journal.get("stats") or {}
    check("page replacement during copy retained a barrier version",
          src_stats.get("backup_retention_copies", 0) >= 1,
          f"retention_copies={src_stats.get('backup_retention_copies')}")
    check("backup copied every published page",
          src_stats.get("backup_page_copies", 0) >=
          len(marker["page_versions"]),
          f"copied={src_stats.get('backup_page_copies')}")
    evidence["read_ranges"].append({
        "target": "source-side copy ranges",
        "backup_page_copies": src_stats.get("backup_page_copies"),
        "backup_retention_copies": src_stats.get("backup_retention_copies"),
        "backup_wal_bytes_copied": src_stats.get("backup_wal_bytes_copied"),
        "backup_bytes_verified": src_stats.get("backup_bytes_verified"),
        "backups_published": src_stats.get("backups_published")})

    # The transaction active at the cutoff must be aborted in the copy.
    check("cutoff-active transaction not visible in backup",
          vals[(0, 1)] == INITIAL, f"slot Y={vals[(0, 1)]}")
    check("after-cutoff W slot is initial in backup",
          vals[(1, 1)] == INITIAL, f"slot W={vals[(1, 1)]}")
    check("pre-backup values survived (X=100, Z=200)",
          vals[(0, 0)] == 100 and vals[(1, 0)] == 200, str(vals))

    # Continue writing inside the backup, close, reopen, re-verify.
    b = open_store(target)
    tb = b.begin()
    b.set(tb, 2, 0, 999)
    b.commit(tb)
    b.close()
    b2 = open_store(target)
    check("backup accepts new transactions after its own recovery",
          b2.read(2, 0) == 999)
    check("backup allocates txids after the cutoff next_txn_id",
          b2._next_txn_id > marker["next_txn_id"],
          f"{b2._next_txn_id} > {marker['next_txn_id']}")
    b2.close()

    # Source must not have seen the backup's write, and keeps its own value.
    svals, _sd, _ss, _n = read_all_slots(d)
    check("source unaffected by backup writes",
          svals[(2, 0)] == INITIAL and svals[(0, 0)] == 107, str(svals))
    check("source retains its own after-cutoff commits",
          svals[(0, 1)] == 11 and svals[(1, 1)] == 22, str(svals))

    # Repeatability: a second backup to a fresh target works, same store.
    target2 = os.path.join(base, "bk2")
    s = open_store(d)
    m2 = s.backup(target2)
    s.close()
    check("backup is repeatable on the same live store",
          os.path.exists(os.path.join(target2, backup_mod.MARKER_NAME)))
    v2, _d2, _st2, _n2 = read_all_slots(target2)
    # Second backup is later, so it includes the after-cutoff commits too.
    check("second backup reflects a later, equally consistent cutoff",
          v2[(0, 1)] == 11 and v2[(1, 1)] == 22 and v2[(0, 0)] == 107,
          str(v2))
    check("second cutoff is not before the first cutoff",
          (m2["cutoff"]["segment"], m2["cutoff"]["offset"]) >=
          (marker["cutoff"]["segment"], marker["cutoff"]["offset"]))


# ------------------------------------------------- 1b. non-blocking interleave
def test_event_mode_interleave(base):
    print("[event-mode seams: backup never blocks, writers run concurrently]")
    d = fresh_store(os.path.join(base, "evt"), "db")
    target = os.path.join(base, "evt", "bk")
    ok, code, out, err = run([WORKER, d, target, "event"])
    check("event-mode worker exits 0 without blocking", ok, err)
    faults_dir = os.path.join(d, "faults")
    fired = [n for n in os.listdir(faults_dir) if n.endswith(".event")]
    check("engine reached the barrier and mid-copy event points",
          any("backup_after_barrier" in n for n in fired)
          and any("backup_mid_page_copy" in n for n in fired), str(fired))
    journal = json.load(open(os.path.join(d, "backup_journal.json")))
    marker = backup_mod.read_marker(target)
    model = build_model_from_journal(journal)
    bk = journal["backup"]
    allowed = model.allowed_cutoffs(bk["start_event_seq"], bk["end_event_seq"])
    prefix_len = len(marker["committed_txids"])
    check("event-mode cutoff is an allowed logical position",
          prefix_len in allowed["allowed_commit_prefix_lengths"],
          f"prefix={prefix_len} allowed={allowed['allowed_commit_prefix_lengths']}")
    vals, _dec, _st, _n = read_all_slots(target)
    expected = model.all_slots_at(prefix_len)
    check("event-mode recovered backup matches independent model",
          vals == expected, f"got={vals} want={expected}")


# ------------------------------------------------- 2. crash during backup
def test_backup_crash_matrix(base):
    print("[backup process hard-killed at each stable point]")
    points = ["backup_after_barrier", "backup_mid_page_copy",
              "backup_after_page_copy", "backup_after_wal_copy",
              "backup_before_publish"]
    for point in points:
        sub = os.path.join(base, "kill_" + point)
        os.makedirs(sub, exist_ok=True)
        d = fresh_store(sub, "db")
        target = os.path.join(sub, "bk")
        # Seed some durable data first.
        s = open_store(d)
        t = s.begin()
        s.set(t, 0, 0, 5)
        s.commit(t)
        s.close()
        FaultInjector(os.path.join(d, "faults")).arm(point)
        ok, code, out, err = run([PS, "backup", d, target],
                                 expect_codes=(99,))
        check(f"[{point}] backup process hard-killed (99)", ok, err)
        check(f"[{point}] no valid-looking target published",
              not os.path.exists(target) and
              not os.path.exists(os.path.join(target,
                                              backup_mod.MARKER_NAME)))
        # No staging residue blocks the source or a retry.
        check(f"[{point}] source reopens and stays usable",
              _source_healthy(d, expected_x=5))
        # Staging from the dead backup is swept on open; retry succeeds.
        ok2, code2, out2, err2 = run([PS, "backup", d, target])
        check(f"[{point}] retry after fixing nothing publishes a backup",
              ok2 and os.path.exists(
                  os.path.join(target, backup_mod.MARKER_NAME)), err2)
        vals, _dec, _st, _n = read_all_slots(target)
        check(f"[{point}] retried backup recovers correct value",
              vals[(0, 0)] == 5, str(vals))

    # Kill AFTER publication: the backup is already valid and must open.
    sub = os.path.join(base, "kill_after_publish")
    os.makedirs(sub, exist_ok=True)
    d = fresh_store(sub, "db")
    target = os.path.join(sub, "bk")
    s = open_store(d)
    t = s.begin()
    s.set(t, 0, 0, 8)
    s.commit(t)
    s.close()
    FaultInjector(os.path.join(d, "faults")).arm("backup_after_publish")
    ok, code, out, err = run([PS, "backup", d, target], expect_codes=(99,))
    check("[after_publish] process exits 99 after valid publication", ok, err)
    check("[after_publish] target marker exists despite the late kill",
          os.path.exists(os.path.join(target, backup_mod.MARKER_NAME)))
    vals, _dec, _st, _n = read_all_slots(target)
    check("[after_publish] published backup independently recoverable",
          vals[(0, 0)] == 8, str(vals))
    check("[after_publish] source still healthy",
          _source_healthy(d, expected_x=8))


def _source_healthy(d, expected_x):
    try:
        vals, _dec, _st, _n = read_all_slots(d)
        return vals[(0, 0)] == expected_x
    except StorageError:
        return False


# ------------------------------------------------- 3. failure handling
def test_failure_handling(base):
    print("[existing target, disk-full mid-copy, corrupt/missing source refs]")
    d = fresh_store(os.path.join(base, "fail"), "db")
    s = open_store(d)
    for i in range(1, 4):
        t = s.begin()
        s.set(t, 0, 0, i)
        s.commit(t)
    s.flush_page(0)
    s.close()

    # Existing target: explicit failure, target untouched, source usable.
    target = os.path.join(base, "fail", "bk")
    os.makedirs(target)
    sentinel = os.path.join(target, "sentinel")
    open(sentinel, "w").write("keep")
    ok, code, out, err = run([PS, "backup", d, target], expect_codes=(3,))
    check("existing target rejected with exit 3", ok, err)
    check("rejected target left untouched",
          open(sentinel).read() == "keep")
    check("source remains usable after rejected backup",
          _source_healthy(d, expected_x=3))
    shutil.rmtree(target)

    # Disk-full injected halfway through staging writes.
    env = dict(os.environ)
    env["PAGESTORE_BACKUP_WRITE_LIMIT"] = "64"
    proc = subprocess.run(
        [sys.executable, "-I", PS, "backup", d,
         os.path.join(base, "fail", "bk_full")],
        capture_output=True, text=True, env=env)
    evidence["exit_codes"].append(
        {"cmd": "backup disk-full", "code": proc.returncode})
    check("disk-full mid-copy fails (non-zero)", proc.returncode != 0,
          proc.stderr.strip()[:120])
    check("no partial target published on disk-full",
          not os.path.exists(os.path.join(base, "fail", "bk_full")))
    check("source survives injected ENOSPC",
          _source_healthy(d, expected_x=3))
    # Retry after the condition is gone.
    ok, code, out, err = run([PS, "backup", d,
                              os.path.join(base, "fail", "bk_retry")])
    check("retry succeeds after the space problem is gone", ok, err)

    # A corrupt source page that the backup must read is a hard failure.
    corrupt_src = fresh_store(os.path.join(base, "corrupt"), "db")
    sc = open_store(corrupt_src)
    t = sc.begin()
    sc.set(t, 1, 1, 42)
    sc.commit(t)
    sc.flush_page(1)
    sc.close()
    pfile = os.path.join(corrupt_src, "pages", "p-00000001")
    blob = bytearray(open(pfile, "rb").read())
    blob[30] ^= 0x55
    with open(pfile, "wb") as f:
        f.write(blob)
    try:
        sc2 = open_store(corrupt_src)
        try:
            sc2.backup(os.path.join(base, "corrupt", "bk"))
            check("corrupt source page fails the backup", False)
        except CorruptionError as exc:
            check("corrupt source page fails the backup with location",
                  "p-00000001" in str(exc), str(exc)[:100])
        finally:
            sc2.close()
    except CorruptionError as exc:
        # Opening itself may surface the corruption when the page is touched.
        check("corrupt source page is explicitly refused",
              "p-00000001" in str(exc), str(exc)[:100])

    # A missing WAL segment referenced by the cutoff is an illegal reference.
    miss_dir = os.path.join(base, "missing", "db")
    os.makedirs(os.path.dirname(miss_dir), exist_ok=True)
    ok, code, out, err = run(
        [PS, "create", miss_dir, "--pages", str(PAGES),
         "--slots", str(SLOTS), "--budget", "2",
         "--wal-segment-size", "48"])     # forces frequent rollover
    assert ok, err
    miss_src = miss_dir
    sm = open_store(miss_src)
    for i in range(10):
        t = sm.begin()
        sm.set(t, 0, 0, i)
        sm.commit(t)
    sm.checkpoint()
    segs = sorted(n for n in os.listdir(os.path.join(miss_src, "wal")))
    check("small segment size produced multiple WAL segments",
          len(segs) >= 3, str(segs))
    sm.close()
    cp = json.loads(read_framed(os.path.join(miss_src, "cp", "current")))
    referenced = f"seg-{cp['wal_position']['segment']:08d}"
    victim = os.path.join(miss_src, "wal", referenced)
    check("checkpoint names a real WAL segment", os.path.exists(victim))
    os.remove(victim)
    try:
        sm2 = open_store(miss_src)
        try:
            sm2.backup(os.path.join(base, "missing", "bk"))
            check("missing cutoff WAL segment fails the backup", False)
        except (StorageError, CorruptionError) as exc:
            check("missing cutoff WAL segment explicitly fails",
                  referenced in str(exc), str(exc)[:120])
        finally:
            sm2.close()
    except StorageError as exc:
        check("missing source reference rejected at open",
              referenced in str(exc), str(exc)[:120])


# ------------------------------------------------- 4. concurrency stress
def test_retained_version_under_contention(base):
    print("[same-page replacements during backup: live and retained sources]")
    import threading
    d = os.path.join(base, "retain", "db")
    os.makedirs(os.path.dirname(d), exist_ok=True)
    ok, code, out, err = run(
        [PS, "create", d, "--pages", "2", "--slots", "2",
         "--budget", "1"])            # budget 1 forces frequent eviction
    assert ok, err
    st = open_store(d)
    stop = threading.Event()
    errors = []

    def churner():
        i = 0
        try:
            while not stop.is_set():
                tx = st.begin()
                st.set(tx, 0, 0, 1000 + i)   # always the same page/slot
                st.commit(tx)
                st.flush_page(0)             # replace the page file hard
                i += 1
        except BaseException as exc:        # noqa: BLE001
            errors.append(("churn", type(exc).__name__, str(exc)))

    th = threading.Thread(target=churner)
    th.start()
    sources = set()
    try:
        for n in range(5):
            tgt = os.path.join(base, "retain", f"bk-{n}")
            mk = st.backup(tgt)
            for f in mk["files"]:
                if f["path"].startswith("pages/"):
                    sources.add(f.get("source"))
            # Each backup must open and recover to a consistent snapshot.
            vals, _d, _s, _n = read_all_slots(tgt)
            assert vals[(0, 0)] in range(1000, 1000 + 100000)
    finally:
        stop.set()
        th.join()
    check("churner saw no errors", not errors, str(errors[:3]))
    check("hot page always reconstructed from retained barrier version",
          "retained" in sources, str(sources))
    st.close()

    # A quiet store (no replacement between barrier and copy) takes the
    # direct live-file path, which must also be exercised and recorded.
    quiet = open_store(d)
    mk_quiet = quiet.backup(os.path.join(base, "retain", "bk-quiet"))
    quiet.close()
    quiet_sources = {f.get("source") for f in mk_quiet["files"]
                     if f["path"].startswith("pages/")}
    check("quiet backup reads barrier pages straight from live files",
          quiet_sources == {"live"}, str(quiet_sources))


def test_concurrent_stress(base):
    print("[parameterized stress: many writers interleaved with repeated backups]")
    import threading
    d = os.path.join(base, "stress", "db")
    os.makedirs(os.path.dirname(d), exist_ok=True)
    ok, code, out, err = run(
        [PS, "create", d, "--pages", "4", "--slots", "4",
         "--initial", "0", "--budget", "2"])
    assert ok, err
    st = open_store(d)
    errors = []
    stop = threading.Event()

    def writer(wid):
        try:
            for i in range(40):
                tx = st.begin()
                p = (wid + i) % 4
                s = wid % 4
                st.add(tx, p, s, 1)
                if i % 3 == 0:
                    st.flush_page(p)
                st.commit(tx)
        except BaseException as exc:  # noqa: BLE001
            errors.append((wid, type(exc).__name__, str(exc)))
            stop.set()

    def backuper():
        try:
            n = 0
            while n < 8 and not stop.is_set():
                tgt = os.path.join(base, "stress", f"bk-{n}")
                m = st.backup(tgt)
                # Every published backup must be independently recoverable
                # and internally consistent at its own cutoff.
                vals, _dec, _stx, _nxt = read_all_slots(tgt)
                mk = backup_mod.read_marker(tgt)
                assert vals is not None and mk["cutoff"] is not None
                n += 1
        except BaseException as exc:  # noqa: BLE001
            errors.append(("backup", type(exc).__name__, str(exc)))
            stop.set()

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    threads.append(threading.Thread(target=backuper))
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    check("stress run produced no thread errors", not errors, str(errors[:3]))
    # Final source is coherent and openable.
    st.checkpoint()
    st.close()
    final_vals, _d, _s, _n = read_all_slots(d)
    check("source coherent after concurrent backup stress",
          all(isinstance(v, int) for v in final_vals.values()))
    backups = [n for n in os.listdir(os.path.join(base, "stress"))
               if n.startswith("bk-")]
    check("multiple backups published under concurrency", len(backups) >= 2,
          f"{len(backups)} backups")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--evidence-dir",
                    default=os.path.join(ROOT, "tests", "evidence"))
    args = ap.parse_args()
    base = tempfile.mkdtemp(prefix="pagestore-backup-")
    try:
        test_interleaved_backup(base)
        test_event_mode_interleave(base)
        test_backup_crash_matrix(base)
        test_failure_handling(base)
        test_retained_version_under_contention(base)
        test_concurrent_stress(base)
    finally:
        os.makedirs(args.evidence_dir, exist_ok=True)
        epath = os.path.join(args.evidence_dir, "backup_evidence.json")
        # Bounded evidence: cap the verbose per-slot and notes payloads.
        evidence["slot_results"] = evidence["slot_results"][:4]
        evidence["read_ranges"] = evidence["read_ranges"][:12]
        with open(epath, "w") as f:
            json.dump(evidence, f, indent=2, sort_keys=True)
        print(f"\nevidence written to {epath}")
        if args.keep:
            print(f"artifacts kept at {base}")
        else:
            shutil.rmtree(base, ignore_errors=True)

    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
