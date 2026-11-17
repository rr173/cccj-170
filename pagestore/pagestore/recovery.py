"""Crash recovery: analysis, per-slot undo/redo, idempotent re-entry.

Recovery never rebuilds the database and never replays the whole history.
It starts from one complete, *usable* checkpoint and only scans the WAL
suffix the checkpoint points at, then resolves exactly the pages referenced
by active transactions, dirty-page lists, post-checkpoint records or durable
pages that still carry a pending slot.

A checkpoint is self-contained with respect to its own WAL position:

* ``dirty_slot_images`` stores the **complete slot state** (pending owner,
  last committer, committed, pending, undo ``prev``) of every slot on every
  page that may have a stale durable copy - pages with committed but
  unflushed values, pages carrying uncommitted pending values (flushed or
  still in cache), and pages locked by an active transaction;
* consequently a record in the reclaimed WAL prefix can never be required
  again: not a committed write whose SET predates the checkpoint, and not
  the undo image of a still-active transaction.  One old transaction only
  pins the current image of its own slots, never the whole history.

A checkpoint whose own WAL reference names a segment that is gone is not
usable.  Selection never falls back "past" reclamation into an older
checkpoint that would depend on deleted segments and silently drop data;
when nothing usable remains it refuses to open.

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
from .errors import CorruptionError

SLOT_FIELDS = ("state", "pending_owner", "last_committer", "committed",
               "pending", "prev")


def run_recovery(store_dir, wal, pages_dir, slot_count, initial_value,
                 stats, faults, write_abort_record, publish_checkpoint):
    """Execute recovery and return the state needed to reopen for business.

    ``write_abort_record(txid)`` and ``publish_checkpoint(payload)`` are
    callbacks supplied by :class:`pagestore.store.Storage`, keeping the
    page/wal ownership in one place.
    """
    cp_dir = os.path.join(store_dir, "cp")
    selected = cp_mod.select_latest(cp_dir, stats, wal=wal)
    if selected is None:
        raise CorruptionError("no usable checkpoint available; database not "
                              "openable", cp_dir)
    cp, cp_path = selected
    stats.notes.append(f"recovery_entry={cp_path}")
    start = cp["wal_position"]
    stats.notes.append(
        f"wal_scan_start=seg{start['segment']}:{start['offset']}")

    # --- Analysis: seed state from the checkpoint -----------------------
    active = {int(t): set(map(tuple, slots))
              for t, slots in cp["active"].items()}
    locks = {}
    for txid, slot_list in active.items():
        for key in slot_list:
            locks[key] = txid
    decisions = {int(t): d for t, d in cp["decisions"].items()}
    dirty_pages = set(cp["dirty_pages"])
    last_writes = {}          # (page, slot) -> (txid, full value)
    next_txn_id = int(cp["next_txn_id"])
    cp_images = cp.get("dirty_slot_images", {})
    if not isinstance(cp_images, dict):
        raise CorruptionError("checkpoint dirty_slot_images is malformed",
                              cp_path)

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
    for key in list(locks):
        if locks[key] in decisions:
            del locks[key]

    faults.reach("recovery_mid_cleanup")

    # --- Resolve every page that may hold unresolved state --------------
    affected = set(dirty_pages)
    for (pid, _sid) in last_writes:
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
        _resolve_page(pid, cp_images.get(str(pid)), pages_dir, slot_count,
                      initial_value, stats, decisions, last_writes,
                      write_abort_record)
        pages_written_this_pass += 1
        # A crash here leaves later pages untouched; the next open resolves
        # them identically and the already-written ones become no-ops.
        if pages_written_this_pass == max(1, len(affected) // 2):
            faults.reach("recovery_mid_page_write")

    # --- Publish a new complete checkpoint at the resolved WAL position -
    active_after = {t: sorted(s) for t, s in active.items()
                    if t not in decisions}
    reclaim = cp.get("reclaim")
    if not isinstance(reclaim, dict):
        reclaim = None
    else:
        reclaim = {"epoch": int(reclaim.get("epoch", 0)),
                   "floor": int(reclaim.get("floor", 1))}
    if reclaim is not None:
        # Recovery performs no deletion, so it carries the published floor
        # forward verbatim.  (Advancing it here would claim a prefix had been
        # removed when it still exists; the next reclaim decides afresh.)
        reclaim = {"epoch": reclaim["epoch"], "floor": reclaim["floor"]}
    new_cp = {
        "version": 2,
        "seq": int(cp["seq"]) + 1,
        "wal_position": end_pos,
        "next_txn_id": next_txn_id,
        "active": {str(t): [list(k) for k in slots]
                   for t, slots in active_after.items()},
        "decisions": {str(t): d for t, d in sorted(decisions.items())},
        "dirty_pages": sorted(active_page_set(active_after)),
        # Every affected page was resolved to a clean durable copy.
        "dirty_slot_images": {},
        "reclaim": reclaim,
    }
    publish_checkpoint(new_cp)
    stats.notes.append(f"recovery_complete pages_resolved={len(affected)}")
    return {
        "next_txn_id": next_txn_id,
        "decisions": decisions,
        "wal_position": end_pos,
        "pages_resolved": len(affected),
        "reclaim": reclaim,
    }


def active_page_set(active):
    out = set()
    for slots in active.values():
        for pid, _sid in slots:
            out.add(pid)
    return out


def _clean_slot(initial: int):
    return {
        "state": pageio.STATE_CLEAN,
        "pending_owner": 0,
        "last_committer": 0,
        "committed": initial,
        "pending": initial,
        "prev": initial,
    }


def _valid_full_image(img, slot_count, path):
    if not isinstance(img, dict):
        return False
    keys = set(img)
    if keys != set(SLOT_FIELDS):
        raise CorruptionError("checkpoint slot image has wrong fields", path)
    if img["state"] not in (pageio.STATE_CLEAN, pageio.STATE_PENDING):
        raise CorruptionError("checkpoint slot image illegal state", path)
    if img["state"] == pageio.STATE_CLEAN and img["pending_owner"] != 0:
        raise CorruptionError("checkpoint slot image clean with owner", path)
    return True


def _base_slots(pid, image, pages_dir, slot_count, initial_value, stats):
    """Return ``(slots, overrode_disk)`` for one page.

    A version-2 full slot image is authoritative for the page at the
    checkpoint position; the WAL suffix is replayed on top afterwards.  A
    legacy integer image only carries ``committed`` and is overlaid on the
    durable page (or used to materialise a missing one).  Without an image
    the durable page is authoritative.
    """
    disk = None
    path = os.path.join(pages_dir, pageio.page_filename(pid))
    if os.path.exists(path):
        _n, disk = pageio.read_page(pages_dir, pid, stats)

    if image is None:
        if disk is not None:
            return [dict(s) for s in disk], False
        return [_clean_slot(initial_value) for _ in range(slot_count)], True

    if isinstance(image, dict) and any(
            isinstance(v, dict) for v in image.values()):
        slots = []
        for sid in range(slot_count):
            img = image.get(str(sid))
            if img is None:
                base = disk[sid] if disk is not None else _clean_slot(
                    initial_value)
                slots.append(dict(base))
                continue
            _valid_full_image(img, slot_count, path)
            slots.append({k: int(img[k]) for k in SLOT_FIELDS})
        return slots, True

    # Legacy image: sid -> committed int.
    slots = ([dict(s) for s in disk] if disk is not None
             else [_clean_slot(initial_value) for _ in range(slot_count)])
    for sid_str, value in image.items():
        try:
            sid = int(sid_str)
            val = int(value)
        except (TypeError, ValueError):
            raise CorruptionError("checkpoint slot image is malformed", path)
        if not (0 <= sid < slot_count):
            raise CorruptionError("checkpoint slot image sid out of range",
                                  path)
        s = slots[sid]
        if s["state"] == pageio.STATE_CLEAN:
            s["committed"] = val
            s["pending"] = val
            s["prev"] = val
            s["last_committer"] = 0
        else:
            s["committed"] = val
    return slots, True


def _resolve_page(pid, image, pages_dir, slot_count, initial_value, stats,
                  decisions, last_writes, write_abort_record):
    slots, changed = _base_slots(pid, image, pages_dir, slot_count,
                                 initial_value, stats)
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
                if s["last_committer"] < txid:
                    s["committed"] = value
                    s["last_committer"] = txid
                    changed = True
    if changed:
        pageio.write_page(pages_dir, pid, slot_count, slots, stats)
