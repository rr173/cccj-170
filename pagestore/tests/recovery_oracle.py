#!/usr/bin/env python3
"""Replay-only test oracle for this engine format (no engine imports).

The reclaim acceptance tests must not use the product's recovery or reclaim
decision functions to *predict* results, but they still need to prove that
an engine directory after a crash is recoverable and slot-identical to an
independently computed expectation.  This oracle does exactly that:

* it parses frames/records/pages itself (stdlib only), including the
  checkpoint's slot images, treating them as *data* of the durable format;
* it is **replay based**: it seeds the durable page files, overlays the
  checkpoint's own slot images (the statement of state at the checkpoint
  position, which is what makes reclamation safe), then replays every
  surviving WAL frame in order.  It never calls the product's recovery,
  checkpoint-selection or reclaim decision code;
* given an independently computed expectation mapping it returns every
  mismatch instead of an exception, and it reports which ``REC_ABORT``
  decisions recovery wrote so the driver can re-feed that public fact into
  its own reference model.

It is deliberately *not* the engine: it re-implements the durable format
semantics (last committed write wins, decisions gate visibility, undo of
undecided pending slots from the page's ``prev``) so that a bug shared with
``pagestore.recovery`` cannot hide behind it.
"""

import json
import os
import re
import zlib

MAGIC = 0x50534D47
PAGE_MAGIC = b"PSPG"
SLOT_BYTES = 42
TXN_BEGIN, SLOT_WRITE, COMMIT, ABORT, REC_ABORT = 1, 2, 3, 4, 5
_SEG_RE = re.compile(r"^seg-(\d+)$")


class OracleError(Exception):
    pass


def frame(payload: bytes) -> bytes:
    head = MAGIC.to_bytes(4, "big") + len(payload).to_bytes(4, "big")
    crc = zlib.crc32(head + payload) & 0xFFFFFFFF
    return head + crc.to_bytes(4, "big") + payload


def _parse(blob, off, loc, stats):
    if len(blob) - off < 12:
        return None
    magic = int.from_bytes(blob[off:off + 4], "big")
    length = int.from_bytes(blob[off + 4:off + 8], "big")
    crc = int.from_bytes(blob[off + 8:off + 12], "big")
    end = off + 12 + length
    if magic != MAGIC or len(blob) < end:
        return None
    payload = blob[off + 12:end]
    if zlib.crc32(blob[off:off + 8] + payload) & 0xFFFFFFFF != crc:
        raise OracleError(f"bad frame CRC at {loc}:{off}")
    stats["frames"] += 1
    return end, payload


def _read_framed(path):
    with open(path, "rb") as f:
        blob = f.read()
    got = _parse(blob, 0, path, {"frames": 0})
    if got is None or got[0] != len(blob):
        raise OracleError(f"bad framed file {path}")
    return got[1]


def _segments(wal_dir):
    return sorted(int(m.group(1)) for m in
                  (_SEG_RE.match(n) for n in os.listdir(wal_dir)) if m)


def scan_wal(wal_dir, start_seg=None):
    """Return ``(records, start_seg_used, end)`` with a strict prefix.

    ``start_seg=None`` scans from the lowest present segment (full replay).
    A hole above ``start_seg`` is an error; a missing start segment is an
    error.
    """
    segs = _segments(wal_dir)
    if not segs:
        raise OracleError("no WAL segments")
    if start_seg is None:
        start_seg = segs[0]
    if start_seg not in segs:
        raise OracleError(f"WAL start seg-{start_seg} missing from {segs}")
    wanted = [s for s in segs if s >= start_seg]
    for prev, cur in zip(wanted, wanted[1:]):
        if cur != prev + 1:
            raise OracleError(f"WAL hole between seg-{prev} and seg-{cur}")
    recs = []
    stats = {"frames": 0}
    for si in wanted:
        path = os.path.join(wal_dir, f"seg-{si:08d}")
        off = 0
        with open(path, "rb") as f:
            blob = f.read()
        while off < len(blob):
            got = _parse(blob, off, path, stats)
            if got is None:
                break
            off = got[0]
            body = got[1]
            t = body[0]
            tx = int.from_bytes(body[1:9], "big")
            if t == SLOT_WRITE:
                recs.append((t, tx, int.from_bytes(body[9:13], "big"),
                             int.from_bytes(body[13:17], "big"),
                             int.from_bytes(body[17:25], "big", signed=True)))
            elif t in (TXN_BEGIN, COMMIT, ABORT, REC_ABORT):
                recs.append((t, tx))
            else:
                raise OracleError(f"unknown record type {t}")
    return recs, start_seg, stats["frames"]


def read_checkpoint(cp_dir):
    for name in ("current",):
        p = os.path.join(cp_dir, name)
        if os.path.exists(p):
            return json.loads(_read_framed(p)), p
    names = [n for n in os.listdir(cp_dir) if n.startswith("cp-")
             and not n.endswith(".new")]
    if not names:
        raise OracleError("no checkpoint")
    names.sort()
    return json.loads(_read_framed(os.path.join(cp_dir, names[-1]))), names[-1]


def read_page(pages_dir, pid):
    path = os.path.join(pages_dir, f"p-{pid:08d}")
    payload = _read_framed(path)
    if payload[:4] != PAGE_MAGIC:
        raise OracleError(f"bad page magic {path}")
    n = int.from_bytes(payload[10:14], "big")
    body = payload[14:]
    if len(body) != n * SLOT_BYTES:
        raise OracleError(f"bad page size {path}")
    out = []
    for i in range(n):
        b = body[i * SLOT_BYTES:(i + 1) * SLOT_BYTES]
        out.append({
            "state": b[0],
            "pending_owner": int.from_bytes(b[2:10], "big", signed=True),
            "last_committer": int.from_bytes(b[10:18], "big", signed=True),
            "committed": int.from_bytes(b[18:26], "big", signed=True),
            "pending": int.from_bytes(b[26:34], "big", signed=True),
            "prev": int.from_bytes(b[34:42], "big", signed=True),
        })
    return out


def _seed_from_checkpoint(cp, pages_dir, num_pages, slots_per_page, initial):
    """Initial world from the checkpoint's own durable slot images.

    The reclaimer removes the WAL prefix, so after a reclaim the checkpoint
    is the durable statement of the committed world at its position.  This
    oracle consumes it as *data* (it re-implements the format), never by
    calling product recovery/reclaim code.  Images may be full slot dicts
    (format v2) or legacy committed ints (format v1).
    """
    world = {}
    owner = {}
    prev = {}
    for p in range(num_pages):
        try:
            slots = read_page(pages_dir, p)
        except FileNotFoundError:
            slots = None
        for sid in range(slots_per_page):
            if slots is not None:
                world[(p, sid)] = slots[sid]["committed"]
                owner[(p, sid)] = slots[sid]["pending_owner"]
                prev[(p, sid)] = slots[sid]["prev"]
            else:
                world[(p, sid)] = initial
                owner[(p, sid)] = 0
                prev[(p, sid)] = initial
    # Checkpoint slot images describe the exact state at the checkpoint
    # position and override whatever the durable page held then.
    images = cp.get("dirty_slot_images") or {}
    for pid_str, per_page in images.items():
        pid = int(pid_str)
        if not (0 <= pid < num_pages):
            raise OracleError(f"checkpoint image for out-of-range page {pid}")
        for sid_str, img in per_page.items():
            sid = int(sid_str)
            if not (0 <= sid < slots_per_page):
                raise OracleError("checkpoint image sid out of range")
            key = (pid, sid)
            if isinstance(img, dict):
                if set(img) != {"state", "pending_owner", "last_committer",
                                "committed", "pending", "prev"}:
                    raise OracleError("checkpoint slot image fields differ")
                world[key] = int(img["committed"])
                owner[key] = int(img["pending_owner"])
                prev[key] = int(img["prev"])
                if int(img["state"]) == 1:
                    world[key] = int(img["pending"])
            else:
                world[key] = int(img)
    return world, owner, prev


def recovered_world(store_dir, expected_slots, num_pages, slots_per_page,
                    initial=0):
    """Independently replay the surviving log on top of the checkpoint.

    Returns ``{"mismatches", "recovery_aborts", "durable_decisions",
    "wal_first_segment", "frames", "world"}``.  ``expected_slots`` maps
    ``(page, slot) -> int`` and must come from the driver's own reference
    model (public operations + durable commit confirmations).
    """
    wal_dir = os.path.join(store_dir, "wal")
    cp_dir = os.path.join(store_dir, "cp")
    cp, cp_path = read_checkpoint(cp_dir)
    start = int(cp["wal_position"]["segment"])
    recs, first_seg, frames = scan_wal(wal_dir, start)

    world, owner, prev = _seed_from_checkpoint(
        cp, os.path.join(store_dir, "pages"), num_pages, slots_per_page,
        initial)
    decisions = {int(t): d for t, d in cp.get("decisions", {}).items()}
    recovery_aborts = []
    for rec in recs:
        if rec[0] == SLOT_WRITE:
            _, tx, pid, sid, value = rec
            key = (pid, sid)
            if decisions.get(tx) == "aborted":
                continue
            if owner.get(key, 0) == 0:
                prev[key] = world[key]
            owner[key] = tx
            world[key] = value
        elif rec[0] in (COMMIT, ABORT, REC_ABORT):
            tx = rec[1]
            if tx in decisions:
                continue
            decided = "committed" if rec[0] == COMMIT else "aborted"
            decisions[tx] = decided
            if rec[0] == REC_ABORT:
                recovery_aborts.append(tx)
            for key in list(owner):
                if owner[key] == tx:
                    if decided == "aborted":
                        world[key] = prev[key]
                    owner[key] = 0
    # Undecided owners (still active at the end of the log) are aborted by
    # recovery; their pending values must not become visible.
    for key, o in owner.items():
        if o != 0 and o not in decisions:
            world[key] = prev[key]
            owner[key] = 0

    mismatches = []
    for p in range(num_pages):
        for sid in range(slots_per_page):
            key = (p, sid)
            want = expected_slots.get(key, initial)
            got = world.get(key, initial)
            if got != want:
                mismatches.append({"slot": [p, sid], "got": got, "want": want})
    return {
        "mismatches": mismatches,
        "recovery_aborts": recovery_aborts,
        "durable_decisions": decisions,
        "wal_first_segment": first_seg,
        "checkpoint": {"path": cp_path, "seq": cp.get("seq"),
                       "wal_position": cp["wal_position"],
                       "reclaim": cp.get("reclaim")},
        "frames": frames,
        "world": world,
    }
