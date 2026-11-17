"""Online reclamation of engine-managed history (WAL segments + old
checkpoints), and the storage API built on it.

Two operations, one decision function:

``Storage.reclaim_plan()`` (preview)
    Read-only.  Takes the store lock only for a short snapshot, publishes no
    checkpoint, writes no file, deletes no file.  Reports per engine-owned
    file whether it could be removed now and, when not, exactly what pins it:
    active-transaction undo images, a running backup's cutoff and barrier
    checkpoint, or the current self-contained recovery basis.  The CLI
    ``reclaim --preview`` variant takes no lock at all (see
    :func:`preview_directory`).

``Storage.reclaim()`` (execute)
    Re-decides every dependency from scratch under the store lock - it never
    consumes a previously previewed file list, and a stale preview can never
    authorise a deletion.  Order:

    1. **New basis first.**  One short locked section: fsync the WAL and
       publish a fresh checkpoint at the exact current append position.  It
       carries the complete slot images needed to recover without any older
       record, plus an incremented *retention epoch*.  Nothing is deleted
       before this replacement basis is durable.  A long-running transaction
       does not block this: only its own slots' undo image is pinned, never
       the whole historical log.
    2. **Manifest next.**  The exact names, sizes, basis and epoch of this
       run are persisted durably (``reclaim.json``) before the first unlink.
       The checkpoint itself carries the incremented epoch, so a crash can
       never be read as "an epoch that was never published authorised these
       deletions"; the manifest's ``status`` only distinguishes prepared
       from complete.
    3. **Unlink last, lock released.**  All deletions run without the store
       lock; commit, checkpoint, page flush and new backup registration keep
       working.  A controllable pause point sits inside this phase.  Unlinks
       are real ``os.unlink`` calls followed by directory fsyncs - never a
       move to a growing trash directory, never a rebuild.

Only ``wal/seg-NNNNNNNN`` and ``cp/cp-NNNNNNNN`` names are ever considered.
Data pages, ``meta``, unknown files in the store directory and published
backups are never touched.
"""

import json
import os

from . import checkpoint as cp_mod
from .framing import CorruptionError, read_framed, write_framed
from .wal import seg_index, seg_name

MANIFEST = "reclaim.json"
DEFAULT_CP_KEEP = 2


# --------------------------------------------------------------- helpers
def _manifest_path(store_dir):
    return os.path.join(store_dir, MANIFEST)


def read_manifest(store_dir):
    try:
        body = read_framed(_manifest_path(store_dir))
    except (FileNotFoundError, OSError):
        return None
    return json.loads(body.decode("utf-8"))


def _write_manifest(store_dir, payload):
    write_framed(_manifest_path(store_dir),
                 json.dumps(payload, sort_keys=True).encode("utf-8"))


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return None


def _digest_prefix(path, n=4096):
    import hashlib
    try:
        with open(path, "rb") as f:
            return hashlib.sha256(f.read(n)).hexdigest()[:16]
    except OSError:
        return None


# ------------------------------------------------------------- snapshot
def _snapshot(store):
    """Short locked snapshot of every input to the decision."""
    with store._txn_lock:
        store._check_ready()
        active = {}
        for t in store._txns.values():
            if t.state == "active" and t.writes:
                active[t.txid] = sorted(t.writes)
        cp_dir = os.path.join(store.dir, "cp")
        cps = cp_mod.list_complete(cp_dir)
        current = None
        cur_path = os.path.join(cp_dir, cp_mod.CURRENT)
        if os.path.exists(cur_path):
            try:
                current = cp_mod.decode(read_framed(cur_path))
            except CorruptionError:
                current = None
        backups = []
        for bid, info in store._active_backups.items():
            backups.append({
                "backup_id": bid,
                "cutoff": dict(info.get("cutoff") or {}),
                "phase": info.get("phase"),
                "needs_pages": sorted(info.get("needs_pages") or []),
                "cp_file": info.get("cp_file"),
                "retention_dir": info.get("retention"),
            })
        return {
            "wal_position": store._wal.position(),
            "segments": store._wal.on_disk(),
            "active": active,
            "backups": backups,
            "checkpoints": cps,
            "current": current,
            "decisions": {str(t): d for t, d in sorted(
                store._decisions.items())},
            "reclaim": dict(store._reclaim),
        }


def _cp_ref_usable(path, segments):
    try:
        payload = cp_mod.decode(read_framed(path))
    except (CorruptionError, OSError):
        return False
    return _usable_chain(segments, int(payload["wal_position"]["segment"]))


def _usable_chain(segments, start_seg):
    """True when ``start_seg`` is present and every later present segment is
    contiguous with it.  A *hole* is real loss, not reclamation."""
    if start_seg not in segments:
        return False
    wanted = [s for s in segments if s >= start_seg]
    return all(b == a + 1 for a, b in zip(wanted, wanted[1:]))


def _decide(store_dir, snap, newest_cp, keep_cp=DEFAULT_CP_KEEP):
    """Pure planning function: performs no writes and no unlinks.

    ``newest_cp`` is the checkpoint that will serve as the recovery basis
    (either the one already on disk, for a preview, or the fresh one just
    published, for an execution).
    """
    wal_dir = os.path.join(store_dir, "wal")
    cp_dir = os.path.join(store_dir, "cp")
    pos = snap["wal_position"]
    app_seg = int(pos["segment"])
    segments = list(snap["segments"])
    current = snap["current"]

    # ---- what pins WAL ------------------------------------------------
    need_from = app_seg
    pins = []

    def pin_from(seg, reason, detail):
        nonlocal need_from
        if seg < need_from:
            need_from = seg
        pins.append({"reason": reason, "detail": detail, "segment": seg})

    pin_from(app_seg, "append_tail",
             f"active WAL segment seg-{app_seg:08d} is appended to")
    basis_seg = None
    if newest_cp is not None:
        basis_seg = int(newest_cp["wal_position"]["segment"])
        basis_pos = newest_cp["wal_position"]
        if _usable_chain(segments, basis_seg):
            pin_from(basis_seg, "recovery_basis",
                     f"checkpoint seq {newest_cp['seq']} starts at "
                     f"seg-{basis_seg:08d}:{basis_pos['offset']}")
        else:
            pins.append({
                "reason": "recovery_basis_unusable",
                "detail": f"checkpoint seq {newest_cp['seq']} references a "
                          f"missing segment; the directory will be refused "
                          f"at open rather than silently losing data",
                "segment": basis_seg})
    else:
        pins.append({"reason": "no_checkpoint", "segment": None,
                     "detail": "no complete checkpoint exists"})

    # Active transactions pin only their own undo images (carried in the
    # basis checkpoint), never a log position.
    active_pins = {}
    for txid, keys in sorted(snap["active"].items()):
        active_pins[str(txid)] = {
            "slots": [[p, s] for (p, s) in keys],
            "pinned_by": "active_transaction_undo_in_checkpoint"}
        pins.append({
            "reason": "active_transaction",
            "detail": f"txid {txid} holds {len(keys)} slot(s); its undo "
                      f"image is in recovery checkpoint "
                      f"{newest_cp['seq'] if newest_cp else None}",
            "segment": None})

    # A running backup pins its cutoff segment until it unregisters (success
    # or failure).  It does not pin anything below that segment.
    backup_pins = []
    for bk in snap["backups"]:
        c = bk.get("cutoff") or {}
        cseg = int(c.get("segment") or 0)
        entry = {"backup_id": bk["backup_id"], "phase": bk.get("phase"),
                 "cutoff": c or None,
                 "pages_retained": len(bk.get("needs_pages") or [])}
        if cseg >= 1:
            pin_from(cseg, "backup_cutoff",
                     f"backup {bk['backup_id']} began before reclaim and "
                     f"reads WAL from seg-{cseg:08d}")
            entry["pins_wal_from"] = seg_name(cseg)
        backup_pins.append(entry)

    floor = max(1, min(need_from, app_seg))
    # The floor is also the lowest segment still on disk; if the tail was
    # already trimmed, do not resurrect numbers.
    if segments:
        floor = max(floor, segments[0])

    wal_entries = []
    for idx in segments:
        path = os.path.join(wal_dir, seg_name(idx))
        if idx < floor:
            wal_entries.append({
                "kind": "wal", "name": seg_name(idx), "segment": idx,
                "size": _size(path), "digest": _digest_prefix(path),
                "action": "remove", "reason": f"below retention floor "
                                              f"seg-{floor:08d}"})
        else:
            why = [p["reason"] for p in pins
                   if p.get("segment") is not None
                   and p["segment"] == floor]
            if idx == app_seg:
                why = ["append_tail"] + [w for w in why if w != "append_tail"]
            wal_entries.append({
                "kind": "wal", "name": seg_name(idx), "segment": idx,
                "size": _size(path), "digest": _digest_prefix(path),
                "action": "keep",
                "retained_because": sorted(set(why)) or ["recovery_basis"]})

    # ---- checkpoints --------------------------------------------------
    cp_entries = []
    seqs = [seq for seq, _p in snap["checkpoints"]]
    keep = set(sorted(seqs, reverse=True)[:keep_cp])
    if newest_cp is not None:
        keep.add(int(newest_cp["seq"]))
    if current is not None and int(current["seq"]) in seqs:
        keep.add(int(current["seq"]))
    paths = dict(snap["checkpoints"])
    # A running backup copies its barrier checkpoint file until it finishes
    # (success or failure); until then it must stay.
    backup_cp_roles = {}
    for bk in snap["backups"]:
        if bk.get("cp_file"):
            backup_cp_roles.setdefault(bk["cp_file"], []).append(
                f"backup {bk['backup_id']} barrier checkpoint")
    for seq in sorted(seqs, reverse=True):
        path = paths[seq]
        name = os.path.basename(path)
        role = []
        if newest_cp is not None and seq == int(newest_cp["seq"]):
            role.append("recovery_basis")
        if current is not None and seq == int(current["seq"]):
            role.append("current_pointer")
        if seq in keep and not role:
            role.append("checkpoint_history")
        if name in backup_cp_roles:
            role.append("backup_barrier_checkpoint")
        if role:
            # A kept checkpoint whose own log reference is already reclaimed
            # cannot serve as a recovery entry; keep it only while it is the
            # basis/current or an active backup still copies it, otherwise
            # it is dead weight (and misleading) and goes with the rest.
            # Judge the checkpoint against the segments that will survive
            # this very run, so a preview's verdict matches execution.
            surviving = [x for x in segments if x >= floor]
            ref_ok = _cp_ref_usable(paths[seq], surviving)
            if not ref_ok and not any(r in role for r in (
                    "recovery_basis", "current_pointer",
                    "backup_barrier_checkpoint")):
                cp_entries.append({
                    "kind": "checkpoint", "name": name, "seq": seq,
                    "size": _size(path), "action": "remove",
                    "digest": _digest_prefix(path),
                    "reason": "superseded checkpoint depends on reclaimed "
                              "WAL"})
                continue
            cp_entries.append({"kind": "checkpoint",
                               "name": name, "seq": seq,
                               "size": _size(path), "action": "keep",
                               "retained_because": role,
                               "backup_detail": backup_cp_roles.get(name)})
        else:
            cp_entries.append({"kind": "checkpoint",
                               "name": os.path.basename(path), "seq": seq,
                               "size": _size(path), "action": "remove",
                               "digest": _digest_prefix(path),
                               "reason": "superseded checkpoint outside "
                                         "the retained keep window"})

    decisions = (newest_cp or {}).get("decisions") or {}
    decision_meta = {
        "decisions": len(decisions),
        "decision_metadata_bytes": len(
            json.dumps(decisions, sort_keys=True).encode("utf-8")),
        "note": "necessary metadata retained for historical transaction "
                "decisions; reclaim never deletes decisions",
    }

    wal_remove = [e for e in wal_entries if e["action"] == "remove"]
    cp_remove = [e for e in cp_entries if e["action"] == "remove"]
    return {
        "retention_floor": floor,
        "wal_position": pos,
        "wal": wal_entries,
        "checkpoints": cp_entries,
        "backup_pins": backup_pins,
        "active_pins": active_pins,
        "pins": pins,
        "removable": {
            "wal_segments": [e["name"] for e in wal_remove],
            "checkpoints": [e["name"] for e in cp_remove],
            "bytes": sum((e["size"] or 0) for e in wal_remove + cp_remove),
        },
        "recoverable_entry": {
            "checkpoint_seq": int(newest_cp["seq"]) if newest_cp else None,
            "checkpoint_wal_position": (newest_cp["wal_position"]
                                        if newest_cp else None),
            "wal_scan_from": seg_name(int(newest_cp["wal_position"]["segment"]))
            if newest_cp else None,
            "append_position": pos,
        },
        "keep_wal_range": {"from": seg_name(floor), "to": seg_name(app_seg)},
        "decision_metadata": decision_meta,
    }


def _public(plan):
    """Trim internal-only fields and add human-facing retention summary."""
    out = dict(plan)
    kept_wal = [e for e in plan["wal"] if e["action"] == "keep"]
    kept_cp = [e for e in plan["checkpoints"] if e["action"] == "keep"]
    out["kept"] = {
        "wal_segments": [e["name"] for e in kept_wal],
        "checkpoints": [e["name"] for e in kept_cp],
    }
    out["kept_detail"] = [
        {"kind": e["kind"], "name": e["name"], "size": e.get("size"),
         "retained_because": e.get("retained_because") or [e.get("reason")]}
        for e in plan["wal"] + plan["checkpoints"] if e["action"] == "keep"]
    out["retention"] = {
        "wal_bytes": sum((e.get("size") or 0) for e in kept_wal),
        "checkpoint_bytes": sum((e.get("size") or 0) for e in kept_cp),
        "wal_files": len(kept_wal),
        "checkpoint_files": len(kept_cp),
        "decision_metadata_bytes":
            plan.get("decision_metadata", {}).get("decision_metadata_bytes"),
        "decision_count":
            plan.get("decision_metadata", {}).get("decisions"),
    }
    return out


# ------------------------------------------------------------- preview
def _decision_map_from_checkpoints(store_dir):
    """Decisions as published by the newest readable checkpoint (file facts)."""
    cp_dir = os.path.join(store_dir, "cp")
    for _seq, path in sorted(cp_mod.list_complete(cp_dir), reverse=True):
        try:
            payload = cp_mod.decode(read_framed(path))
        except CorruptionError:
            continue
        if cp_mod.wal_reference_ok(_ReadOnlyWalView(store_dir),
                                   payload["wal_position"]):
            return payload.get("decisions") or {}
    return {}


class _ReadOnlyWalView:
    """Minimal ``wal`` interface for reference checks without a writer."""
    def __init__(self, store_dir):
        from .wal import on_disk_segments
        self.dir = os.path.join(store_dir, "wal")
        self._segments = on_disk_segments(self.dir)

    def on_disk(self):
        return list(self._segments)


def _snapshot_directory(store_dir, wal, reclaim_record):
    """Snapshot for a preview with *no* store lock and *no* truncation.

    Active transactions and in-memory page versions are inherently
    unknowable without the writer (a second process cannot see them); the
    preview therefore reports file facts only, and an operator who needs
    live-transaction pins uses the API preview against the open store.
    """
    cp_dir = os.path.join(store_dir, "cp")
    cps = cp_mod.list_complete(cp_dir)
    current = None
    cur_path = os.path.join(cp_dir, cp_mod.CURRENT)
    if os.path.exists(cur_path):
        try:
            current = cp_mod.decode(read_framed(cur_path))
        except CorruptionError:
            current = None
    return {
        "wal_position": wal.position(),
        "segments": wal.on_disk(),
        "active": {},
        "backups": [],
        "checkpoints": cps,
        "current": current,
        "decisions": _decision_map_from_checkpoints(store_dir),
        "reclaim": dict(reclaim_record),
    }


def _read_reclaim_record(store_dir):
    """Read ``reclaim`` from the newest usable checkpoint, if any."""
    cp_dir = os.path.join(store_dir, "cp")
    for _seq, path in sorted(cp_mod.list_complete(cp_dir), reverse=True):
        try:
            payload = cp_mod.decode(read_framed(path))
        except CorruptionError:
            continue
        rec = payload.get("reclaim")
        if isinstance(rec, dict):
            return {"epoch": int(rec.get("epoch", 0)),
                    "floor": int(rec.get("floor", 1))}
    return {"epoch": 0, "floor": 1}


def preview_directory(store_dir, wal, stats):
    """CLI/operator preview: read-only, lock-free, writes nothing."""
    snap = _snapshot_directory(store_dir, wal, _read_reclaim_record(store_dir))
    plan = _preview_plan(store_dir, snap, wal)
    plan["mode"] = "preview"
    plan["read_only_open"] = True
    plan["notes"].append(
        "opened without the writer lock: active transactions and in-memory "
        "page versions are not visible; WAL/checkpoint file facts are exact")
    return plan


def _preview_plan(store_dir, snap, wal):
    """Shared planner for the locked (API) and lock-free (CLI) previews."""
    newest_on_disk = None
    for _seq, path in sorted(snap["checkpoints"], reverse=True):
        try:
            d = cp_mod.decode(read_framed(path))
        except CorruptionError:
            continue
        if cp_mod.wal_reference_ok(wal, d["wal_position"]):
            newest_on_disk = d
            break
    seqs = [seq for seq, _p in snap["checkpoints"]]
    predicted = {
        "seq": (max(seqs) if seqs else 0) + 1,
        "wal_position": dict(snap["wal_position"]),
        "decisions": snap["decisions"],
    }
    # Model the state *after* executing: the fresh basis exists under its
    # deterministic name and is the new ``current``.  That keeps the preview's
    # per-file verdicts identical to what execution will do.
    snap["current"] = predicted
    snap["checkpoints"] = ([(predicted["seq"],
                             os.path.join(store_dir, "cp",
                                          cp_mod.cp_filename(
                                              predicted["seq"])))]
                           + list(snap["checkpoints"]))
    plan = _decide(store_dir, snap, predicted)
    plan["mode"] = "preview"
    plan["epoch"] = int(snap["reclaim"].get("epoch", 0))
    plan["predicted_epoch"] = plan["epoch"] + 1
    plan["published_floor"] = int(snap["reclaim"].get("floor", 1))
    plan["preview_basis"] = predicted
    plan["current_usable_basis"] = (
        {"seq": int(newest_on_disk["seq"]),
         "wal_position": newest_on_disk["wal_position"]}
        if newest_on_disk else None)
    plan["ok"] = True
    plan["notes"] = [
        "read-only: no checkpoint published, no file created or removed",
        f"execution would first publish checkpoint seq {predicted['seq']} at "
        f"seg{predicted['wal_position']['segment']}:"
        f"{predicted['wal_position']['offset']} as the replacement basis"]
    return _public(plan)


def plan_reclaim(store):
    """Read-only preview against the *open* store (short locked snapshot).

    No checkpoint is published, no file is written, no file is deleted.
    Unlike the lock-free CLI preview this one can also see live transactions,
    in-memory page versions and in-process online backups.
    """
    snap = _snapshot(store)
    return _preview_plan(store.dir, snap, store._wal)


# ------------------------------------------------------------ execution
def run_reclaim(store, keep_cp=DEFAULT_CP_KEEP, pause=None):
    """Re-decide dependencies now and physically free what is safe."""
    store._check_ready()
    # Stable point before the replacement basis is durable: a crash here
    # must leave the directory exactly as it was (nothing published, no
    # epoch bumped, no file named for deletion).
    store._faults.reach("reclaim_before_basis")
    with store._txn_lock:
        store._check_ready()
        # 1. Snapshot the current state (we hold the lock, so no other thread
        #    can append to the WAL or register a backup until we are done).
        snap = _snapshot(store)
        seqs = [seq for seq, _p in snap["checkpoints"]]
        # 2. The *predicted* replacement basis: same WAL position, next seq,
        #    with the decision map the fresh checkpoint will carry.  Planning
        #    against it means the floor we publish is the floor we enforce.
        predicted = {
            "seq": (max(seqs) if seqs else 0) + 1,
            "wal_position": dict(snap["wal_position"]),
            "decisions": snap["decisions"],
        }
        snap["current"] = predicted
        snap["checkpoints"] = ([(predicted["seq"],
                                 os.path.join(store.dir, "cp",
                                              cp_mod.cp_filename(
                                                  predicted["seq"])))]
                               + list(snap["checkpoints"]))
        plan = _decide(store.dir, snap, predicted, keep_cp=keep_cp)
        epoch = int(snap["reclaim"].get("epoch", 0)) + 1
        # 3. Make the replacement basis durable.  Its payload carries the
        #    incremented epoch and the new floor; it is self-contained for
        #    every record below it.  Nothing has been deleted yet.
        old_reclaim = dict(store._reclaim)
        store._reclaim = {"epoch": epoch,
                          "floor": int(plan["retention_floor"])}
        try:
            store._wal.fsync()
            basis_path = store._publish_checkpoint()
        except BaseException:
            # Nothing was deleted; keep memory consistent with what is
            # durable on disk so a retry cannot inherit a phantom floor.
            store._reclaim = old_reclaim
            raise
        basis = cp_mod.decode(read_framed(basis_path))
        if basis["wal_position"] != predicted["wal_position"]:
            raise CorruptionError(
                "WAL advanced while publishing the reclaim basis; refusing "
                "to delete against a stale plan")
        if int(basis.get("reclaim", {}).get("floor", -1)) != int(
                plan["retention_floor"]):
            raise CorruptionError(
                "reclaim basis did not persist the retention floor")
        manifest = {
            "version": 1,
            "epoch": epoch,
            "status": "prepared",
            "basis": {"seq": int(basis["seq"]),
                      "file": cp_mod.cp_filename(int(basis["seq"])),
                      "wal_position": basis["wal_position"]},
            "retention_floor": plan["retention_floor"],
            "keep_checkpoints": [e["name"] for e in plan["checkpoints"]
                                 if e["action"] == "keep"],
            "delete": [{"kind": e["kind"], "name": e["name"],
                        "size": e.get("size")}
                       for e in plan["wal"] + plan["checkpoints"]
                       if e["action"] == "remove"],
            "backups": [b["backup_id"] for b in snap["backups"]],
            "active_transactions": sorted(int(t) for t in snap["active"]),
        }
        # 4. Manifest next: the authorised names are durable before unlink.
        _write_manifest(store.dir, manifest)
        store._wal.set_retention_floor(int(plan["retention_floor"]))
        entries = [e for e in plan["wal"] + plan["checkpoints"]
                   if e["action"] == "remove"]
        # The delete loop consults this list on every file; tests may append
        # callbacks (e.g. to run an external probe mid-batch) without holding
        # the lock.  Production leaves it empty.
        hooks = list(store._reclaim_test_hooks)
    # Both seams are outside the store lock: a paused/crashing reclaim must
    # never hold the business lock during the bulk delete phase.
    store._faults.reach("reclaim_basis_published")
    store._faults.reach("reclaim_before_unlink")

    # 4. Delete unlocked.  A controllable pause point lets a test stop the
    #    batch and prove other threads can commit and checkpoint meanwhile.
    removed, failed = [], []
    for i, e in enumerate(entries):
        if pause is not None:
            pause(i, entries)
        for hook in hooks:
            hook(i, entries, e)
        path = _entry_path(store.dir, e)
        if path is None:
            continue
        try:
            size = os.path.getsize(path)
        except FileNotFoundError:
            continue
        except OSError as exc:
            failed.append({"kind": e["kind"], "name": e["name"],
                           "error": f"{type(exc).__name__}: {exc}"})
            continue
        # Re-verify identity/evidence immediately before the unlink: a file
        # that no longer matches what was planned (replaced, truncated) is
        # not removed under a stale decision.  Segments and checkpoints are
        # append-once/immutable, so only their genuinely stale prefix can
        # trigger this.
        if e.get("digest") and _digest_prefix(path) != e["digest"]:
            failed.append({"kind": e["kind"], "name": e["name"],
                           "error": "file changed since planning; not "
                                    "removed under a stale decision"})
            continue
        try:
            os.unlink(path)
        except FileNotFoundError:
            continue
        except OSError as exc:
            failed.append({"kind": e["kind"], "name": e["name"],
                           "error": f"{type(exc).__name__}: {exc}"})
            continue
        removed.append({"kind": e["kind"], "name": e["name"], "bytes": size,
                        "reason": e.get("reason")})
        if e["kind"] == "wal":
            store.stats.wal_segments_removed += 1
            store.stats.wal_segments_removed_bytes += size
        else:
            store.stats.checkpoints_removed += 1
            store.stats.checkpoints_removed_bytes += size
        store.stats.reclaim_unlinks += 1
        if i != len(entries) - 1:
            # Stable point: some, not all, planned files are gone.
            store._faults.reach("reclaim_mid_unlink")
    for sub in ("wal", "cp"):
        try:
            _fsync_dir(os.path.join(store.dir, sub))
        except OSError:
            pass
    store.stats.reclaim_runs += 1
    store._faults.reach("reclaim_after_unlinks")

    # 5. Record outcome.  Failure is reported per file, never as success.
    with store._txn_lock:
        status = "complete" if not failed else "partial"
        manifest = dict(manifest)
        manifest["status"] = status
        manifest["removed"] = removed
        manifest["failed"] = failed
        try:
            _write_manifest(store.dir, manifest)
        except OSError:
            pass

    result = _public(plan)
    result.update({
        "ok": not failed,
        "mode": "execute",
        "epoch": epoch,
        "status": status,
        "removed": {
            "wal_segments": [r for r in removed if r["kind"] == "wal"],
            "checkpoints": [r for r in removed if r["kind"] == "checkpoint"],
            "bytes": sum(r["bytes"] for r in removed),
        },
        "failed": failed,
        "total_wal_bytes": sum(
            (e.get("size") or 0) for e in plan["wal"] if e["action"] == "keep"),
        "notes": [
            "replacement recovery basis published before any unlink",
            "deletions ran with the store lock released",
        ],
    })
    if failed:
        result["notes"].append(
            f"{len(failed)} file(s) could not be removed; permissions/storage "
            f"conditions are reported per file and the run is not a full "
            f"success - fix and re-run")
    return result


def _entry_path(store_dir, e):
    if e["kind"] == "wal" and seg_index(e["name"]) > 0:
        return os.path.join(store_dir, "wal", e["name"])
    if e["kind"] == "checkpoint" and cp_mod.cp_seq_of(e["name"]) is not None:
        return os.path.join(store_dir, "cp", e["name"])
    return None


# ------------------------------------------------------ startup handling
def on_open(store_dir, stats, notes):
    """Read a reclaim manifest (if any) and annotate the open.

    The authoritative marker for "this directory was legitimately reclaimed"
    is the ``reclaim`` record inside the selected checkpoint, not the
    manifest: a crash may leave the manifest ``prepared`` while every unlink
    it named has completed.  What must still be a hard error is a *hole* in
    the surviving WAL range or a checkpoint referencing a missing segment;
    ``select_latest``/``wal.scan`` refuse those.
    """
    try:
        manifest = read_manifest(store_dir)
    except (CorruptionError, ValueError) as exc:
        raise CorruptionError(f"reclaim manifest invalid: {exc}",
                              _manifest_path(store_dir))
    if manifest is not None:
        notes.append(f"reclaim_manifest_epoch={manifest.get('epoch')} "
                     f"status={manifest.get('status')}")
    try:
        os.remove(_manifest_path(store_dir) + ".tmp")
    except FileNotFoundError:
        pass
    return manifest
