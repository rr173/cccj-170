"""Online, repeatable, crash-safe backup of a live PageStore directory.

A backup exports the store into an *independent* directory that any process
can open with :func:`pagestore.open_store`, run recovery on, write new
transactions into, close and reopen.  It never freezes the source: writer
threads keep committing, flushing pages and taking checkpoints while the
copy proceeds.  The only short critical sections happen at the *barrier*;
all byte copying runs without holding the store transaction lock.

Logical cutoff (one single point in time, never a Frankenstein mix)
-------------------------------------------------------------------

1. The backup registers itself in the store.  From that moment every page
   publication that *replaces* a page file preserves the previous immutable
   version in a per-backup retention directory (``_publish_page`` hook in
   :class:`pagestore.store.Storage`).  Page versions are monotonic integers.
2. At the barrier, under the store lock, the WAL is fsynced and a normal
   checkpoint is published at the resulting WAL position ``(seg, off)``.
   Because every WAL append and every commit fsync take the same lock,
   ``(seg, off)`` is always an exact frame boundary and every durable
   COMMIT/ABORT record is fully inside the prefix.  The backup pins:
     * the checkpoint sequence/file and the cutoff position,
     * the per-page version number current at the barrier,
     * the ordered durable decisions and the next txid.
3. File copying happens lock-free afterwards.  A page whose version still
   equals the pinned version is copied from the live file (it can never be
   overwritten while the version matches - replacements bump the version);
   a page replaced after the barrier is reconstructed from its retained
   barrier version.  Pages born after the barrier are simply absent and the
   copy's own recovery materializes them from the checkpoint images.
4. Only WAL segments up to the cutoff segment are copied; the cutoff
   segment is truncated to exactly ``off`` bytes.  No record committed after
   the barrier can therefore enter the copy's recovery.
5. Everything is checksum-verified inside the unpublished staging area;
   only then is the marker written and the whole directory atomically
   renamed to the target path.  A crash or failure before that rename leaves
   no valid-looking backup anywhere.

The marker (``backup.marker`` in the copy root) records the cutoff, the
ordered durable commit sequence and per-file hashes, so an independent
driver can map the copy to the commit confirmations it observed.
"""

import hashlib
import json
import os
import shutil
import threading

from . import checkpoint as cp_mod
from . import pageio, records
from .errors import BackupError, CorruptionError, IllegalReferenceError
from .framing import read_framed, write_framed, parse_frame
from .wal import seg_name, seg_index

STAGING_ROOT = ".backup-staging"
MARKER_NAME = "backup.marker"
SWEEP_ROOT = STAGING_ROOT
META_NAME = "meta"
# Test seam: when set in the environment, staging writes fail with ENOSPC
# after this many cumulative bytes, emulating a disk-full mid-copy.
WRITE_LIMIT_ENV = "PAGESTORE_BACKUP_WRITE_LIMIT"

_write_budget_lock = threading.Lock()
_write_budget_used = 0


def _reset_write_budget():
    global _write_budget_used
    with _write_budget_lock:
        _write_budget_used = 0


def _account_write(n):
    """Raise ENOSPC once the externally configured byte budget is spent."""
    limit = os.environ.get(WRITE_LIMIT_ENV)
    if not limit:
        return
    global _write_budget_used
    with _write_budget_lock:
        _write_budget_used += n
        if _write_budget_used > int(limit):
            raise OSError(28, "No space left on device (injected backup"
                              " write limit)")


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def sweep_staging(store_dir):
    """Remove staging directories left by backups whose process died.

    A freshly opened store has no in-memory active backup, so anything under
    ``<store>/.backup-staging`` belongs to a dead backup and can never be
    published; it is safe to delete.  Called from create/open.
    """
    root = os.path.join(store_dir, STAGING_ROOT)
    if os.path.isdir(root):
        shutil.rmtree(root, ignore_errors=True)


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _copy_bytes(src, dst, max_bytes=None):
    """Copy a regular file byte-for-byte into staging, honoring the seam."""
    with open(src, "rb") as f:
        blob = f.read() if max_bytes is None else f.read(max_bytes)
    if max_bytes is not None and len(blob) != max_bytes:
        raise BackupError(
            f"source file shrank below cutoff offset while copying: {src}")
    _account_write(len(blob))
    with open(dst, "wb") as f:
        f.write(blob)
        f.flush()
        os.fsync(f.fileno())
    return len(blob)


def run_online_backup(store, target_path):
    """Copy ``store`` to ``target_path`` without pausing source writers.

    Returns the cutoff marker dict (also persisted as ``backup.marker``
    inside the target).  Raises :class:`BackupError` on any failure; the
    source stays usable and no partial target is ever published.
    """
    store._check_ready()
    target_path = os.path.abspath(target_path)
    parent = os.path.dirname(target_path) or "."
    if not os.path.isdir(parent):
        raise BackupError(f"target parent directory does not exist: {parent}")
    if os.path.exists(target_path) or os.path.islink(target_path):
        raise BackupError(
            f"backup target already exists; remove it before retrying: "
            f"{target_path}")

    # Serialize backups aiming at the same target name and make staging
    # cleanup deterministic even for two concurrent backup() callers.
    lock_path = os.path.join(parent, "." + os.path.basename(target_path)
                             + ".backup.lock")
    import fcntl
    lock_fh = open(lock_path, "a+")
    try:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        if os.path.exists(target_path):
            raise BackupError(
                f"backup target already exists; remove it before retrying: "
                f"{target_path}")

        bid = store._register_backup()
        staging = os.path.join(store.dir, STAGING_ROOT, bid)
        try:
            _reset_write_budget()
            pinned = _barrier(store, staging)
            store._faults.reach("backup_after_barrier")
            file_manifest = _copy_all(store, staging, pinned)
            marker = _verify_and_mark(store, staging, pinned, file_manifest)
            store._faults.reach("backup_before_publish")
            _publish(staging, target_path, parent)
            store.stats.backups_published += 1
            store._faults.reach("backup_after_publish")
            return marker
        except BaseException:
            # Any failure (OSError, CorruptionError, ...): never leave a
            # published target.  All staging/retention cleanup for this
            # backup id is centralized in _unregister_backup in finally.
            raise
        finally:
            store._unregister_backup(bid)
    finally:
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        finally:
            lock_fh.close()
        # The lock file is only useful while a backup runs; remove it once
        # ours is released.  A concurrent backup holding its own handle on
        # the same path makes this removal a harmless no-op for it.
        try:
            os.remove(lock_path)
        except OSError:
            pass


# ------------------------------------------------------------------ barrier
def _barrier(store, staging):
    """Short locked section: pin one consistent logical point in time."""
    with store._txn_lock:
        # Make sure pages published by earlier processes have a version
        # number before we pin the per-page barrier version.
        store.published_page_ids()
        # Persist every record already appended so the cutoff position is a
        # durable frame boundary covering all decisions confirmed so far.
        store._wal.fsync()
        cp_path = store._publish_checkpoint()
        pos = dict(store._wal_pos)
        pinned_versions = dict(store._page_versions)
        committed = [t for t, d in store._decisions.items()
                     if d == "committed"]
        aborted = [t for t, d in store._decisions.items()
                   if d == "aborted"]
        next_txn_id = store._next_txn_id
        cp_seq = store._cp_seq
    os.makedirs(os.path.join(staging, "pages"))
    os.makedirs(os.path.join(staging, "wal"))
    os.makedirs(os.path.join(staging, "cp"))
    _fsync_dir(staging)
    return {
        "cp_path": cp_path,
        "cp_seq": cp_seq,
        "cutoff": pos,
        "page_versions": pinned_versions,
        "committed_txids": committed,
        "aborted_txids": aborted,
        "next_txn_id": next_txn_id,
        "bid": os.path.basename(staging),
    }


# --------------------------------------------------------------------- copy
def _copy_all(store, staging, pinned):
    """Lock-free bulk copy of meta, pages, checkpoint and cutoff WAL."""
    files = []

    # ---- meta (immutable for the life of a store) ----------------------
    meta_src = os.path.join(store.dir, META_NAME)
    meta_dst = os.path.join(staging, META_NAME)
    size = _copy_bytes(meta_src, meta_dst)
    files.append({"path": META_NAME, "size": size,
                  "sha256": _sha256_file(meta_dst)})

    # ---- pages: pinned barrier version per page ------------------------
    retention_root = store._retention_dir(pinned["bid"])
    page_entries = sorted(pinned["page_versions"].items())
    total = len(page_entries)
    # Deterministic "halfway through the page files" seam, reachable even
    # when the store currently has a single published page.
    mid_index = min(total - 1, max(0, total // 2 - 1)) if total else -1
    for index, (pid, pinned_ver) in enumerate(page_entries):
        name = pageio.page_filename(pid)
        dst = os.path.join(staging, "pages", name)
        live_src = os.path.join(store._pages_dir, name)
        retained_src = os.path.join(retention_root, str(pid),
                                    f"v-{pinned_ver:010d}")
        # Optimistically copy the still-current live file without holding
        # the store lock, then re-check the version.  If a replacement
        # landed during the copy, discard it and use the immutable retained
        # barrier version (published by _publish_page before any swap).
        with store._txn_lock:
            cur_ver = store._page_versions.get(pid, 0)
        used = "live" if cur_ver == pinned_ver else "retained"
        src = live_src if used == "live" else retained_src
        _copy_bytes(src, dst)
        with store._txn_lock:
            cur_ver_after = store._page_versions.get(pid, 0)
        if used == "live" and cur_ver_after != pinned_ver:
            if not os.path.exists(retained_src):
                raise IllegalReferenceError(
                    "barrier page version missing from backup retention",
                    retained_src)
            _copy_bytes(retained_src, dst)
            used = "retained"
        elif used == "retained" and not os.path.exists(retained_src):
            raise IllegalReferenceError(
                "barrier page version missing from backup retention",
                retained_src)
        size = os.path.getsize(dst)
        store.stats.backup_page_copies += 1
        files.append({"path": os.path.join("pages", name), "size": size,
                      "sha256": _sha256_file(dst), "source": used})
        if index == mid_index:
            store._faults.reach("backup_mid_page_copy")
    _fsync_dir(os.path.join(staging, "pages"))
    store._faults.reach("backup_after_page_copy")

    # ---- checkpoint: the immutable pinned file + republished current ---
    cp_name = os.path.basename(pinned["cp_path"])
    cp_dst = os.path.join(staging, "cp", cp_name)
    size = _copy_bytes(pinned["cp_path"], cp_dst)
    files.append({"path": os.path.join("cp", cp_name), "size": size,
                  "sha256": _sha256_file(cp_dst)})
    # 'current' is an atomic same-content reference inside the copy.
    cp_payload = read_framed(cp_dst)
    write_framed(os.path.join(staging, "cp", cp_mod.CURRENT), cp_payload)
    files.append({"path": os.path.join("cp", cp_mod.CURRENT),
                  "size": os.path.getsize(
                      os.path.join(staging, "cp", cp_mod.CURRENT)),
                  "sha256": _sha256_file(
                      os.path.join(staging, "cp", cp_mod.CURRENT))})
    _fsync_dir(os.path.join(staging, "cp"))

    # ---- WAL: every segment up to the cutoff, prefix length exactly off -
    cutoff_seg = pinned["cutoff"]["segment"]
    cutoff_off = pinned["cutoff"]["offset"]
    wal_src_dir = os.path.join(store.dir, "wal")
    on_disk = sorted(seg_index(n) for n in os.listdir(wal_src_dir)
                     if seg_index(n) > 0)
    wanted = [s for s in on_disk if s <= cutoff_seg]
    if not wanted or wanted[0] != 1 or wanted[-1] != cutoff_seg:
        raise IllegalReferenceError(
            "cutoff WAL segment not present in source",
            os.path.join(wal_src_dir, seg_name(cutoff_seg)))
    for prev, cur in zip(wanted, wanted[1:]):
        if cur != prev + 1:
            raise IllegalReferenceError(
                "WAL segment sequence has a hole during backup",
                os.path.join(wal_src_dir, seg_name(prev + 1)))
    wal_copied = 0
    for si in wanted:
        src = os.path.join(wal_src_dir, seg_name(si))
        dst = os.path.join(staging, "wal", seg_name(si))
        if si == cutoff_seg:
            src_size = os.path.getsize(src)
            if src_size < cutoff_off:
                raise IllegalReferenceError(
                    "cutoff offset beyond current segment size", src)
            size = _copy_bytes(src, dst, max_bytes=cutoff_off)
        else:
            size = _copy_bytes(src, dst)
        wal_copied += size
        files.append({"path": os.path.join("wal", seg_name(si)),
                      "size": size, "sha256": _sha256_file(dst)})
    store.stats.backup_wal_bytes_copied += wal_copied
    _fsync_dir(os.path.join(staging, "wal"))
    store._faults.reach("backup_after_wal_copy")
    return files


# ------------------------------------------------------------------ verify
def _verify_and_mark(store, staging, pinned, files):
    """Validate the whole staging tree before it may become a backup."""
    # 1. No links: every entry must be a fresh regular file.
    for entry in files:
        p = os.path.join(staging, entry["path"])
        st = os.lstat(p)
        import stat as _stat
        if not _stat.S_ISREG(st.st_mode):
            raise CorruptionError("backup entry is not a regular file", p)
        sp = os.path.join(store.dir, entry["path"])
        if os.path.exists(sp) and not os.path.islink(sp):
            sst = os.lstat(sp)
            if (sst.st_dev, sst.st_ino) == (st.st_dev, st.st_ino):
                raise CorruptionError(
                    "backup entry shares inode with source (link, not copy)",
                    p)

    # 2. Every page parses with its frame CRC and structural validation.
    page_dir = os.path.join(staging, "pages")
    for name in sorted(os.listdir(page_dir)):
        pid = int(name[2:])
        pageio.read_page(page_dir, pid, store.stats)

    # 3. Checkpoint matches the barrier and points exactly at the cutoff.
    cp = cp_mod.decode(read_framed(os.path.join(staging, "cp",
                                                cp_mod.CURRENT)))
    if int(cp["seq"]) != pinned["cp_seq"]:
        raise CorruptionError("staged checkpoint seq differs from barrier")
    if cp["wal_position"] != pinned["cutoff"]:
        raise CorruptionError(
            "staged checkpoint WAL position differs from backup cutoff")
    cp_bytes = open(os.path.join(
        staging, "cp", cp_mod.cp_filename(pinned["cp_seq"])), "rb").read()
    cur_bytes = open(os.path.join(staging, "cp", cp_mod.CURRENT), "rb").read()
    if cp_bytes != cur_bytes:
        raise CorruptionError("staged current does not match pinned cp")

    # 4. WAL prefix: frames must parse and end exactly at the cutoff, with
    #    no record beyond it.  This is an independent byte-level pass.
    end_seg, end_off, decoded_records = _scan_staged_wal(
        os.path.join(staging, "wal"), pinned["cutoff"])
    if (end_seg, end_off) != (pinned["cutoff"]["segment"],
                              pinned["cutoff"]["offset"]):
        raise CorruptionError(
            f"staged WAL ends at seg{end_seg}:{end_off}, cutoff is "
            f"{pinned['cutoff']}")

    # 5. The durable decision SET inside the cutoff prefix must agree with
    #    the barrier snapshot.  Order here is compared as sets: the WAL scan
    #    yields records in log order, while the barrier snapshot preserves
    #    the store's durable-confirmation (insertion) order; with interleaved
    #    concurrent transactions these two orderings can legitimately differ
    #    even though they describe the same single cutoff point.  The marker
    #    itself keeps the confirmation order the independent driver expects.
    seen_commits, seen_aborts, max_txid = [], [], 0
    for rec in decoded_records:
        max_txid = max(max_txid, rec["txid"])
        if rec["type"] == records.COMMIT:
            seen_commits.append(rec["txid"])
        elif rec["type"] in (records.ABORT, records.REC_ABORT):
            seen_aborts.append(rec["txid"])
    if sorted(seen_commits) != sorted(pinned["committed_txids"]):
        raise CorruptionError(
            f"commit set in cutoff prefix {sorted(seen_commits)} differs "
            f"from barrier snapshot {sorted(pinned['committed_txids'])}")
    if sorted(seen_aborts) != sorted(pinned["aborted_txids"]):
        raise CorruptionError("abort set mismatch between WAL and barrier")
    if max_txid + 1 > pinned["next_txn_id"]:
        raise CorruptionError("staged WAL references txid beyond next_txn_id")

    # 6. File hashes/sizes in the manifest all match.
    for entry in files:
        p = os.path.join(staging, entry["path"])
        if os.path.getsize(p) != entry["size"]:
            raise CorruptionError("staged file size mismatch", p)
        if _sha256_file(p) != entry["sha256"]:
            raise CorruptionError("staged file checksum mismatch", p)
        store.stats.backup_bytes_verified += entry["size"]

    marker = {
        "version": 1,
        "backup_id": pinned["bid"],
        "cutoff": pinned["cutoff"],
        "checkpoint_seq": pinned["cp_seq"],
        "next_txn_id": pinned["next_txn_id"],
        "committed_txids": list(pinned["committed_txids"]),
        "aborted_txids": list(pinned["aborted_txids"]),
        "page_versions": {str(p): v for p, v in
                          sorted(pinned["page_versions"].items())},
        "files": sorted(files, key=lambda e: e["path"]),
    }
    marker_path = os.path.join(staging, MARKER_NAME)
    write_framed(marker_path, json.dumps(marker, sort_keys=True).encode())
    _fsync_dir(staging)
    return marker


def _scan_staged_wal(wal_dir, cutoff):
    """Parse staged WAL frames independently; return end position + records."""
    on_disk = sorted(seg_index(n) for n in os.listdir(wal_dir)
                     if seg_index(n) > 0)
    all_records = []
    end_seg, end_off = on_disk[0] if on_disk else 1, 0
    for si in on_disk:
        path = os.path.join(wal_dir, seg_name(si))
        with open(path, "rb") as f:
            blob = f.read()
        rel = 0
        while rel < len(blob):
            new_rel, payload = parse_frame(blob, rel, path)
            all_records.append(records.decode(payload))
            rel = new_rel
        end_seg, end_off = si, rel
    if not on_disk:
        end_seg = cutoff["segment"]
    return end_seg, end_off, all_records


def read_marker(target_path):
    """Read and validate the marker of a published backup (for drivers)."""
    path = os.path.join(target_path, MARKER_NAME)
    body = read_framed(path)
    marker = json.loads(body.decode("utf-8"))
    for key in ("version", "cutoff", "checkpoint_seq", "next_txn_id",
                "committed_txids", "aborted_txids", "files"):
        if key not in marker:
            raise CorruptionError(f"backup marker missing {key}", path)
    return marker


# ----------------------------------------------------------------- publish
def _publish(staging, target_path, parent):
    """Atomically move the verified staging tree to its final name."""
    os.rename(staging, target_path)
    _fsync_dir(parent)
