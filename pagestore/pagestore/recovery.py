"""Crash recovery: analysis, per-slot undo/redo, idempotent re-entry.

Recovery never rebuilds the database and never replays the whole history.
It starts from one complete checkpoint and only scans the WAL suffix the
checkpoint points at, then resolves exactly the pages referenced by active
transactions, dirty-page lists or post-checkpoint records.

Idempotence across a crash *during* recovery relies on two facts:

1. A recovery-written abort is a durable ``REC_ABORT`` decision; a second
   open sees it as an already-published decision, not as new business work.
2. Redo only changes a slot when its ``last_committer`` is older than the
   committed transaction (or when the slot still carries that transaction's
   pending value).  Re-running the same records therefore changes nothing a
   second time - in particular an ``add`` can never be applied twice.
"""

import os

from . import checkpoint as cp_mod
from . import pageio, records
from .errors import CorruptionError, IllegalReferenceError
from .framing import read_framed


def run_recovery(store_dir, wal, pages_dir, slot_count, initial_value,
                 stats, faults, write_abort_record, publish_checkpoint):
    """Execute recovery and return the state needed to reopen for business.

    ``write_abort_record(txid)`` and ``publish_checkpoint(payload)`` are
    callbacks supplied by :class:`pagestore.store.Storage`, keeping the
    page/wal ownership in one place.
    """
    cp_dir = os.path.join(store_dir, "cp")
    selected = cp_mod.select_latest(cp_dir, stats)
    if selected is None:
        raise CorruptionError("no checkpoint available; database not openable",
                              cp_dir)
    cp, cp_path = selected
    stats.notes.append(f"recovery_entry={cp_path}")
    start = cp["wal_position"]
    stats.notes.append(
        f"wal_scan_start=seg{start['segment']}:{start['offset']}")

    # --- Analysis: seed state from the checkpoint -----------------------
    active = {int(t): set(map(tuple, slots))
              for t, slots in cp["active"].items()}
    # Lock map: (page, slot) -> txid, rebuilt from checkpoint lock lists.
    locks = {}
    for txid, slot_list in active.items():
        for key in slot_list:
            locks[key] = txid
    decisions = {int(t): d for t, d in cp["decisions"].items()}
    dirty_pages = set(cp["dirty_pages"])
    last_writes = {}          # (page, slot) -> (txid, full value)
    next_txn_id = int(cp["next_txn_id"])
    # Committed slot images carried inside the checkpoint itself, letting
    # recovery materialize a never-flushed page without reading the WAL
    # prefix that precedes the checkpoint position.
    cp_images = cp.get("dirty_slot_images", {})

    recs, end_pos = wal.scan(start["segment"], start["offset"])
    stats.notes.append(
        f"wal_scan_end=seg{end_pos['segment']}:{end_pos['offset']} "
        f"records={len(recs)}")

    for body, seg, frame_end in recs:
        try:
            r = records.decode(body)
        except ValueError as exc:
            raise CorruptionError(f"malformed record: {exc}",
                                  f"seg{seg}:{frame_end}")
        t, rt = r["txid"], r["type"]
        next_txn_id = max(next_txn_id, t + 1)
        if rt == records.TXN_BEGIN:
            if t in decisions:
                stats.wal_records_skipped += 1
            else:
                active.setdefault(t, set())
                stats.wal_records_applied += 1
        elif rt == records.SLOT_WRITE:
            key = (r["page_id"], r["slot_id"])
            if t in decisions and decisions[t] == "aborted":
                # An aborted transaction's late/torn write never installs.
                stats.wal_records_skipped += 1
            else:
                last_writes[key] = (t, r["value"])
                active.setdefault(t, set()).add(key)
                locks.setdefault(key, t)
                dirty_pages.add(r["page_id"])
                stats.wal_records_applied += 1
        elif rt in (records.COMMIT, records.ABORT, records.REC_ABORT):
            if t in decisions:
                # Re-entering recovery after a mid-recovery crash: the
                # decision is already durable, this record changes nothing.
                stats.wal_records_skipped += 1
            else:
                decisions[t] = ("committed" if rt == records.COMMIT
                                else "aborted")
                active.pop(t, None)
                stats.wal_records_applied += 1

    faults.reach("recovery_after_record_scan")

    # --- Decision phase: anything still active never decided -> abort ---
    for txid in sorted(active):
        if txid not in decisions:
            write_abort_record(txid)
            decisions[txid] = "aborted"
            stats.notes.append(f"recovery_abort=tx{txid}")
        else:  # decision exists but a stale lock list lingered
            active.pop(txid, None)
    # Locks belonging to decided transactions are released.
    for key in list(locks):
        if locks[key] in decisions:
            del locks[key]

    faults.reach("recovery_mid_cleanup")

    # --- Resolve every page that may hold unresolved state --------------
    affected = set(dirty_pages)
    for (pid, _sid) in list(last_writes):
        affected.add(pid)
    for slot_list in cp["active"].values():
        for pid, _sid in map(tuple, slot_list):
            affected.add(pid)
    # Belt-and-braces: inspect every durable page header.  A page can carry
    # a pending slot even if no checkpoint named it (the process died after
    # flushing the page but before publishing the next checkpoint); the
    # page format itself is the authoritative source for that state.
    for name in os.listdir(pages_dir):
        if not (name.startswith("p-") and not name.endswith(".tmp")):
            continue
        try:
            pid = int(name[2:])
        except ValueError:
            continue
        _n, slots = pageio.read_page(pages_dir, pid, stats)
        if any(s["state"] == pageio.STATE_PENDING for s in slots):
            affected.add(pid)

    pages_written_this_pass = 0
    for pid in sorted(affected):
        _resolve_page(pid, pages_dir, slot_count, initial_value, stats,
                      decisions, last_writes, write_abort_record,
                      cp_images.get(str(pid), {}))
        pages_written_this_pass += 1
        # A crash here leaves later pages untouched; the next open resolves
        # them identically and the already-written ones become no-ops.
        if pages_written_this_pass == max(1, len(affected) // 2):
            faults.reach("recovery_mid_page_write")

    # --- Publish a new complete checkpoint at the resolved WAL position -
    active_after = {t: sorted(s) for t, s in active.items() if t not in decisions}
    new_cp = {
        "version": 1,
        "seq": int(cp["seq"]) + 1,
        "wal_position": end_pos,
        "next_txn_id": next_txn_id,
        "active": {str(t): [list(k) for k in slots]
                   for t, slots in active_after.items()},
        "decisions": {str(t): d for t, d in sorted(decisions.items())},
        "dirty_pages": sorted(active_page_set(active_after)),
        # Recovery has resolved every dirty page to a clean durable copy,
        # so no slot images remain outstanding.
        "dirty_slot_images": {},
    }
    publish_checkpoint(new_cp)
    stats.notes.append(f"recovery_complete pages_resolved={len(affected)}")
    return {
        "next_txn_id": next_txn_id,
        "decisions": decisions,
        "wal_position": end_pos,
        "pages_resolved": len(affected),
    }


def active_page_set(active):
    out = set()
    for slots in active.values():
        for pid, _sid in slots:
            out.add(pid)
    return out


def _resolve_page(pid, pages_dir, slot_count, initial_value, stats,
                  decisions, last_writes, write_abort_record, cp_image):
    path = os.path.join(pages_dir, pageio.page_filename(pid))
    page_existed = os.path.exists(path)
    if page_existed:
        _n, slots = pageio.read_page(pages_dir, pid, stats)
    else:
        # A page referenced by the checkpoint may never have been
        # published; seed it from initial values then the checkpoint's own
        # committed slot images, without touching the pre-checkpoint WAL.
        slots = [_clean_slot(initial_value) for _ in range(slot_count)]
        for sid_str, value in cp_image.items():
            sid = int(sid_str)
            if 0 <= sid < slot_count:
                slots[sid]["committed"] = int(value)
                slots[sid]["pending"] = int(value)
                slots[sid]["prev"] = int(value)
    changed = not page_existed
    for sid, s in enumerate(slots):
        key = (pid, sid)
        owner = s["pending_owner"]
        if s["state"] == pageio.STATE_PENDING and owner != 0:
            decision = decisions.get(owner)
            if decision == "committed":
                if s["last_committer"] < owner:
                    s["committed"] = s["pending"]
                    s["last_committer"] = owner
                    changed = True
                s["state"] = pageio.STATE_CLEAN
                s["pending_owner"] = 0
                s["pending"] = s["committed"]
            elif decision == "aborted":
                s["committed"] = s["prev"]
                s["state"] = pageio.STATE_CLEAN
                s["pending_owner"] = 0
                s["pending"] = s["prev"]
                changed = True
            else:
                # Pending owner with no durable decision: make the abort
                # durable first, then undo this single slot.
                write_abort_record(owner)
                decisions[owner] = "aborted"
                s["committed"] = s["prev"]
                s["state"] = pageio.STATE_CLEAN
                s["pending_owner"] = 0
                s["pending"] = s["prev"]
                changed = True
        # Redo from post-checkpoint log: full-value, idempotent install.
        if key in last_writes:
            txid, value = last_writes[key]
            if decisions.get(txid) == "committed":
                if s["last_committer"] < txid and not (
                        s["state"] == pageio.STATE_PENDING
                        and s["pending_owner"] == txid):
                    s["committed"] = value
                    s["last_committer"] = txid
                    changed = True
                elif (s["state"] == pageio.STATE_PENDING
                      and s["pending_owner"] == txid):
                    # Handled above; mark nothing extra.
                    pass
        elif not page_existed and str(sid) in cp_image:
            # Already seeded from the checkpoint image; ensure the slot is
            # marked as a real installed value.
            s["last_committer"] = max(s["last_committer"], 0)
    if changed or not page_existed:
        pageio.write_page(pages_dir, pid, slot_count, slots, stats)


def _clean_slot(initial: int):
    return {
        "state": pageio.STATE_CLEAN,
        "pending_owner": 0,
        "last_committer": 0,
        "committed": initial,
        "pending": initial,
        "prev": initial,
    }
