"""Page-based transactional storage engine (stdlib only).

Public API: :func:`create`, :func:`open_store`, and the returned
:class:`Storage` with ``begin/read/set/add/commit/abort/flush_page/
checkpoint/close`` plus ``decision``, ``stats`` and ``inspect_durable_page``.

Concurrency model: one writer process per directory (an ``flock``-based
lock refuses a second writer), while many transactions interleave inside
that process.  Conflicts are per slot, never per page: two transactions
writing different slots of the same page both proceed and independently
commit or abort.
"""

import fcntl
import os
import shutil
import threading

from . import backup as backup_mod
from . import checkpoint as cp_mod
from . import pageio, records, reclaim as reclaim_mod, recovery
from .crash import FaultInjector
from .errors import (ConflictError, LockError,
                     StorageClosedError, OverflowError)
from .framing import read_framed, write_framed
from .stats import Stats
from .wal import WAL

INT_MIN = -(1 << 63)
INT_MAX = (1 << 63) - 1
META_NAME = "meta"
LOCK_NAME = "writer.lock"


class Transaction:
    def __init__(self, txid):
        self.txid = txid
        # (page, slot) -> slot value last written inside this transaction
        self.writes = {}
        self.state = "active"          # active | committed | aborted


def create(store_dir, num_pages, slots_per_page, initial_value=0,
           cache_budget=4, wal_segment_size=1 << 20):
    """Create a fresh store and return it open (exclusive writer lock)."""
    os.makedirs(store_dir, exist_ok=True)
    if os.listdir(store_dir):
        raise FileExistsError(f"store directory not empty: {store_dir}")
    if num_pages <= 0 or slots_per_page <= 0:
        raise ValueError("num_pages and slots_per_page must be positive")
    if cache_budget < 1:
        raise ValueError("cache_budget must be at least one page")
    if not (INT_MIN <= initial_value <= INT_MAX):
        raise OverflowError("initial_value outside signed 64-bit range")
    for sub in ("pages", "wal", "cp", "faults"):
        os.makedirs(os.path.join(store_dir, sub))
    _fsync_dir(store_dir)
    meta = {
        "version": 1,
        "num_pages": num_pages,
        "slots_per_page": slots_per_page,
        "initial_value": initial_value,
        "wal_segment_size": wal_segment_size,
        "cache_budget": cache_budget,
    }
    write_framed(os.path.join(store_dir, META_NAME),
                 _encode_meta(meta))
    store = Storage(store_dir, meta, Stats(), FaultInjector(
        os.path.join(store_dir, "faults")))
    store._acquire_lock()
    store._wal = WAL(os.path.join(store_dir, "wal"), wal_segment_size,
                     store.stats)
    store._wal.open_for_append()
    # Bootstrap checkpoint at WAL position (segment 1, offset 0): recovery
    # always has a complete, published starting point.
    store._publish_checkpoint()
    store._ready = True
    return store


def open_store(store_dir, cache_budget=None, enable_faults=True):
    """Open an existing store, running crash recovery before any reads."""
    meta = _decode_meta(read_framed(os.path.join(store_dir, META_NAME)))
    budget = meta["cache_budget"] if cache_budget is None else cache_budget
    store = Storage(store_dir, meta, Stats(), FaultInjector(
        os.path.join(store_dir, "faults"), enabled=enable_faults))
    # The writer lock must be taken BEFORE sweeping: a staging tree may
    # belong to a backup running in the process that currently holds the
    # lock.  Only after we own the directory can any staging residue be
    # proven dead (a previous process died mid-backup and its unpublished
    # staging can never be promoted).
    store._acquire_lock()
    backup_mod.sweep_staging(store_dir)
    store._wal = WAL(os.path.join(store_dir, "wal"),
                     meta["wal_segment_size"], store.stats)
    # WAL must be openable for appending (recovery may write REC_ABORT).
    store._wal.open_for_append()
    from . import checkpoint as _cp
    reclaim_mod.on_open(store_dir, store.stats, store.stats.notes)
    selected = _cp.select_latest(os.path.join(store_dir, "cp"), store.stats,
                                 wal=store._wal)
    if selected is None:
        from .framing import CorruptionError
        raise CorruptionError("no checkpoint available",
                              os.path.join(store_dir, "cp"))
    # Align the in-memory sequence so recovery's new checkpoint gets the
    # exact seq number its payload declares.
    store._cp_seq = int(selected[0]["seq"])
    result = recovery.run_recovery(
        store_dir, store._wal, store._pages_dir, meta["slots_per_page"],
        meta["initial_value"], store.stats, store._faults,
        store._write_abort_record, store._publish_checkpoint)
    store._cp_seq = int(store._cp_seq)  # callback already incremented it
    store._next_txn_id = result["next_txn_id"]
    store._decisions = result["decisions"]
    store._wal_pos = result["wal_position"]
    rec = result.get("reclaim")
    if isinstance(rec, dict):
        store._reclaim = {"epoch": int(rec.get("epoch", 0)),
                          "floor": int(rec.get("floor", 1))}
    store._wal.set_retention_floor(store._reclaim["floor"])
    store._ready = True
    return store


class Storage:
    def __init__(self, store_dir, meta, stats, faults):
        self.dir = store_dir
        self.meta = meta
        self.stats = stats
        self._faults = faults
        self._pages_dir = os.path.join(store_dir, "pages")
        self._lock_fh = None
        self._wal = None
        self._cache = {}              # page_id -> slot list (mutable dicts)
        self._cache_order = []        # LRU: least recently used first
        self._txns = {}               # txid -> Transaction
        self._locks = {}              # (page, slot) -> txid
        # Pages whose durable copy may differ from the latest state.  A page
        # enters on first write and only leaves when a clean (no pending
        # slot) version is published.  Crucially it survives the commit of a
        # transaction whose page was never flushed, so checkpoints keep
        # pointing recovery at that missing/stale page.
        self._durable_dirty = set()
        self._pending_pages = set()   # pages carrying any pending slot
        self._decisions = {}
        self._next_txn_id = 1
        self._cp_seq = 0
        self._wal_pos = {"segment": 1, "offset": 0}
        self._ready = False
        self._closed = False
        # Concurrency boundary: every mutation of the structures above takes
        # this reentrant lock.  Online backups take it only for short
        # bookkeeping sections (register/barrier/unregister); all byte
        # copying happens without it, so commit/flush/checkpoint interleave
        # freely with an in-progress backup.
        self._txn_lock = threading.RLock()
        # Notified whenever the active-backup set changes, so close() can
        # wait without spinning.
        self._backup_done = threading.Condition(self._txn_lock)
        # Monotonic per-page version: bumped on every atomic page
        # publication.  An online backup pins each page's version at its
        # barrier and either reads the still-current file or a retained
        # previous version, guaranteeing one coherent point in time.
        self._page_versions = {}
        # bid -> {"retention": dir, "cutoff": pos, "phase": str,
        #          "needs_pages": set}; only present while a backup runs.
        self._active_backups = {}
        self._backup_seq = 0
        # Published retention epoch/floor (see pagestore.reclaim).  Recovered
        # from the selected checkpoint on open.
        self._reclaim = {"epoch": 0, "floor": 1}
        # Test seam: callables appended here are invoked before each unlink
        # in the (unlocked) delete phase of reclaim.run_reclaim.
        self._reclaim_test_hooks = []

    # ------------------------------------------------------------- locking
    def _acquire_lock(self):
        path = os.path.join(self.dir, LOCK_NAME)
        fh = open(path, "a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fh.close()
            raise LockError(f"store already open for writing: {self.dir}")
        self._lock_fh = fh

    def _check_ready(self):
        if self._closed:
            raise StorageClosedError("storage is closed")
        if not self._ready:
            raise StorageClosedError("recovery has not completed")

    # ------------------------------------------------------------- txn API
    def begin(self):
        with self._txn_lock:
            self._check_ready()
            txid = self._next_txn_id
            self._next_txn_id += 1
            self._txns[txid] = Transaction(txid)
            self._wal.append(records.encode(records.TXN_BEGIN, txid))
            # The begin record only needs durability before the commit record;
            # batching here is safe because commit fsyncs the whole prefix.
            return txid

    def read(self, page_id, slot_id, txn=None):
        """Read a slot.

        With ``txn`` given and active, that transaction sees its own writes;
        without a transaction (or from another transaction) only the last
        committed value is visible - pending values of active writers are
        never exposed.
        """
        with self._txn_lock:
            self._check_ready()
            self._validate_addr(page_id, slot_id)
            if txn is not None:
                t = self._txns.get(txn)
                if t is None:
                    raise StorageClosedError(f"unknown transaction {txn}")
                if t.state != "active":
                    raise StorageClosedError(
                        f"transaction {txn} already {t.state}")
                if (page_id, slot_id) in t.writes:
                    return t.writes[(page_id, slot_id)]
            slots = self._get_page(page_id)
            slot = slots[slot_id]
            return slot["committed"]

    def set(self, txn, page_id, slot_id, value):
        with self._txn_lock:
            self._write(txn, page_id, slot_id, value)

    def add(self, txn, page_id, slot_id, delta):
        with self._txn_lock:
            current = self.read(page_id, slot_id, txn)
            new_value = current + delta
            if new_value < INT_MIN or new_value > INT_MAX:
                # Rejected before any write: no half mutation, no log record.
                raise OverflowError(
                    f"add would overflow int64 ({current} + {delta})")
            self._write(txn, page_id, slot_id, new_value)

    def commit(self, txn):
        with self._txn_lock:
            self._check_ready()
            t = self._require_active(txn)
            # WAL decision first (durability rule), page cleaning follows lazily.
            self._wal.append(records.encode(records.COMMIT, txn))
            self._faults.reach("commit_before_decision_fsync")
            self._wal.fsync()
            self._faults.reach("commit_after_decision_fsync")
            t.state = "committed"
            self._decisions[txn] = "committed"
            # Install committed state into cached pages; pages not cached keep
            # their on-disk pending state and are resolved on next load/recovery.
            for (pid, sid), value in t.writes.items():
                if pid in self._cache:
                    slot = self._cache[pid][sid]
                    if slot["state"] == pageio.STATE_PENDING \
                            and slot["pending_owner"] == txn:
                        slot["committed"] = value
                        slot["last_committer"] = txn
                        slot["state"] = pageio.STATE_CLEAN
                        slot["pending_owner"] = 0
                        slot["pending"] = value
                self._release_lock(pid, sid, txn)
            self._recompute_pending_pages()
            return txn

    def abort(self, txn):
        with self._txn_lock:
            self._check_ready()
            t = self._require_active(txn)
            self._wal.append(records.encode(records.ABORT, txn))
            self._faults.reach("abort_before_decision_fsync")
            self._wal.fsync()
            self._undo_transaction(t)
            t.state = "aborted"
            self._decisions[txn] = "aborted"
            return txn

    def decision(self, txn):
        """Return the durable decision for a txid, or None if unknown."""
        with self._txn_lock:
            return self._decisions.get(txn)

    # ----------------------------------------------------------- page ops
    def flush_page(self, page_id):
        """Force one page to durable storage, pending values included."""
        with self._txn_lock:
            self._check_ready()
            if page_id not in self._cache:
                self._get_page(page_id)  # materialize then publish unchanged
            self._publish_page(page_id)

    def checkpoint(self):
        """Publish a restartable checkpoint while transactions stay active."""
        with self._txn_lock:
            self._check_ready()
            self._faults.reach("checkpoint_before_publish")
            self._wal.fsync()
            return self._publish_checkpoint()

    def backup(self, target_path):
        """Export an independent, fully recoverable copy of this store.

        The copy represents exactly one logical cutoff point taken during
        the call: every transaction whose commit was durable at that point
        is visible in the copy, later commits are not, and transactions
        still active at the point are aborted by the copy's own recovery.
        Source writers keep running throughout - the store is never frozen
        for the duration of the directory copy.  Returns the cutoff marker
        dict (also persisted as ``backup.marker`` inside the target).
        """
        self._check_ready()
        return backup_mod.run_online_backup(self, target_path)

    def close(self):
        """Clean shutdown.  Crash tests must kill the process instead."""
        with self._txn_lock:
            if self._closed:
                return
            self._check_ready()
            # A close must not strand a backup mid-copy: wait for in-progress
            # backups (they never hold this lock while copying).  Condition
            # wait releases the RLock fully and re-acquires it atomically.
            self._backup_done.wait_for(lambda: not self._active_backups)
            # Flush every cached page so a clean close leaves no pending on-disk
            # state; this is NOT required for correctness, only tidiness.
            for pid in list(self._cache):
                self._publish_page(pid)
            self._wal.fsync()
            self._publish_checkpoint()
            self._wal.close()
            if self._lock_fh is not None:
                fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)
                self._lock_fh.close()
                self._lock_fh = None
            self._closed = True
            self._ready = False

    # ------------------------------------------------------------- innards
    def _write(self, txn, page_id, slot_id, value):
        self._check_ready()
        self._validate_addr(page_id, slot_id)
        if not (INT_MIN <= value <= INT_MAX):
            raise OverflowError("value outside signed 64-bit range")
        t = self._require_active(txn)
        key = (page_id, slot_id)
        owner = self._locks.get(key)
        if owner is not None and owner != txn:
            raise ConflictError(
                f"slot ({page_id},{slot_id}) locked by txn {owner}")
        slots = self._get_page(page_id)
        slot = slots[slot_id]
        first_write = key not in t.writes
        if first_write:
            # First touch: lock the slot and remember the undo value.  The
            # WAL record carries the full post-image (idempotent replay).
            self._locks[key] = txn
            t.writes[key] = value
            slot["prev"] = slot["committed"]
            slot["pending"] = value
            slot["pending_owner"] = txn
            slot["state"] = pageio.STATE_PENDING
            self._pending_pages.add(page_id)
            self._durable_dirty.add(page_id)
            self._wal.append(records.encode(
                records.SLOT_WRITE, txn, page_id, slot_id, value))
        else:
            # Same transaction, same slot: update working value only.  No new
            # lock and (optionally) no extra record - the full final value is
            # what matters.  We do append it so a crash between writes still
            # finds the latest full value, and redo takes the last one.
            t.writes[key] = value
            slot["pending"] = value
            self._wal.append(records.encode(
                records.SLOT_WRITE, txn, page_id, slot_id, value))
        self._touch_lru(page_id)

    def _undo_transaction(self, t):
        for (pid, sid) in list(t.writes):
            if pid in self._cache:
                slot = self._cache[pid][sid]
                if slot["state"] == pageio.STATE_PENDING \
                        and slot["pending_owner"] == t.txid:
                    slot["committed"] = slot["prev"]
                    slot["state"] = pageio.STATE_CLEAN
                    slot["pending_owner"] = 0
                    slot["pending"] = slot["prev"]
            self._release_lock(pid, sid, t.txid)
        self._recompute_pending_pages()

    def _release_lock(self, pid, sid, txn):
        if self._locks.get((pid, sid)) == txn:
            del self._locks[(pid, sid)]

    def _require_active(self, txn):
        t = self._txns.get(txn)
        if t is None:
            raise StorageClosedError(f"unknown transaction {txn}")
        if t.state != "active":
            raise StorageClosedError(
                f"transaction {txn} already {t.state}")
        return t

    def _validate_addr(self, page_id, slot_id):
        if not (0 <= page_id < self.meta["num_pages"]):
            raise IndexError(f"page_id {page_id} out of range")
        if not (0 <= slot_id < self.meta["slots_per_page"]):
            raise IndexError(f"slot_id {slot_id} out of range")

    # --------------------------------------------------------------- cache
    def _get_page(self, page_id):
        if page_id in self._cache:
            self._touch_lru(page_id)
            return self._cache[page_id]
        self._enforce_budget_for_load()
        path = os.path.join(self._pages_dir, pageio.page_filename(page_id))
        if os.path.exists(path):
            _n, slots = pageio.read_page(
                self._pages_dir, page_id, self.stats)
            # Lazy resolution: a pending slot whose owner is already decided
            # must be reconciled before the page joins the cache.
            for slot in slots:
                if slot["state"] == pageio.STATE_PENDING:
                    owner = slot["pending_owner"]
                    d = self._decisions.get(owner)
                    if d == "committed":
                        slot["committed"] = slot["pending"]
                        slot["last_committer"] = owner
                        slot["state"] = pageio.STATE_CLEAN
                        slot["pending_owner"] = 0
                        slot["pending"] = slot["committed"]
                    elif d == "aborted":
                        slot["committed"] = slot["prev"]
                        slot["state"] = pageio.STATE_CLEAN
                        slot["pending_owner"] = 0
                        slot["pending"] = slot["prev"]
        else:
            iv = self.meta["initial_value"]
            slots = [{
                "state": pageio.STATE_CLEAN, "pending_owner": 0,
                "last_committer": 0, "committed": iv,
                "pending": iv, "prev": iv,
            } for _ in range(self.meta["slots_per_page"])]
        self._cache[page_id] = slots
        self._cache_order.append(page_id)
        return slots

    def _enforce_budget_for_load(self):
        budget = self.meta["cache_budget"]
        while len(self._cache) >= budget:
            victim = self._cache_order[0]
            self._evict(victim)

    def _evict(self, page_id):
        # Even pages with uncommitted pending state are publishable: the
        # page format stores pending separately from committed values.
        self._publish_page(page_id)
        del self._cache[page_id]
        self._cache_order.remove(page_id)

    def _touch_lru(self, page_id):
        if page_id in self._cache_order:
            self._cache_order.remove(page_id)
        self._cache_order.append(page_id)

    def _publish_page(self, page_id):
        slots = self._cache[page_id]
        old_version = self._page_versions.get(page_id, 0)
        # Retain the about-to-be-replaced immutable version for every backup
        # that may still need it.  Retention is a regular file copy into the
        # backup's private staging subtree, never a link; it is removed when
        # the backup unregisters.  No retention is needed for the first
        # publication (nothing existed to pin at any earlier barrier).
        if old_version and self._active_backups:
            src = os.path.join(self._pages_dir,
                               pageio.page_filename(page_id))
            if os.path.exists(src):
                for bid in self._active_backups:
                    rdir = os.path.join(
                        self._retention_dir(bid), str(page_id))
                    os.makedirs(rdir, exist_ok=True)
                    dst = os.path.join(
                        rdir, f"v-{old_version:010d}")
                    if not os.path.exists(dst):
                        shutil.copy2(src, dst)
                        self.stats.backup_retention_copies += 1
        pageio.write_page(self._pages_dir, page_id,
                          self.meta["slots_per_page"], slots, self.stats)
        self._page_versions[page_id] = old_version + 1
        has_pending = any(
            s["state"] == pageio.STATE_PENDING for s in slots)
        if not has_pending:
            self._pending_pages.discard(page_id)
            # A fully clean durable copy now exists; the page no longer
            # needs checkpoint-based rediscovery.
            self._durable_dirty.discard(page_id)

    def _recompute_pending_pages(self):
        self._pending_pages = {
            pid for pid, slots in self._cache.items()
            if any(s["state"] == pageio.STATE_PENDING for s in slots)
        }
        # Pages evicted while pending are tracked via the lock map instead.
        for (pid, _sid) in self._locks:
            self._pending_pages.add(pid)

    # ---------------------------------------------------------- checkpoint
    def _publish_checkpoint(self, explicit_payload=None):
        if explicit_payload is not None:
            # Recovery hands us a fully built snapshot (resolved WAL end
            # position, post-cleanup active set); publish it verbatim and
            # keep the in-memory sequence aligned.
            payload = dict(explicit_payload)
            self._cp_seq = int(payload["seq"])
            pos = payload["wal_position"]
        else:
            active = {}
            for t in self._txns.values():
                if t.state == "active":
                    active[t.txid] = sorted(t.writes.keys())
            self._cp_seq += 1
            pos = self._wal.position()
            # `_slot_images` persists the complete slot state (committed and
            # undo values) of every page carrying unresolved or unflushed
            # state, so the checkpoint fully describes recovery's starting
            # world.  That is what lets reclaim delete every older record.
            dirty = sorted(self._dirty_page_set())
            payload = {
                "version": 2,
                "seq": self._cp_seq,
                "wal_position": pos,
                "next_txn_id": self._next_txn_id,
                "active": {str(t): [list(k) for k in slots]
                           for t, slots in active.items()},
                "decisions": {str(t): d for t, d in
                              sorted(self._decisions.items())},
                "dirty_pages": dirty,
                "dirty_slot_images": self._slot_images(dirty),
                "reclaim": dict(self._reclaim),
            }
        self._wal_pos = pos
        return cp_mod.publish(os.path.join(self.dir, "cp"),
                              int(payload["seq"]), payload, self.stats,
                              faults=self._faults)

    def _dirty_page_set(self):
        """Pages whose durable copy may be stale or hold unresolved state.

        This includes pages carrying pending slots (cache-resident or
        evicted, tracked via the lock map) and committed-but-never-flushed
        pages, so recovery materializes/repairs every one even when all the
        relevant WAL records sit before the checkpoint position.
        """
        pages = set(self._durable_dirty)
        pages.update(self._pending_pages)
        for (pid, _sid) in self._locks:
            pages.add(pid)
        for pid, slots in self._cache.items():
            if any(s["state"] == pageio.STATE_PENDING for s in slots):
                pages.add(pid)
        return pages

    def _slot_images(self, dirty_pages):
        """Complete self-contained slot state for every checkpointed page.

        Each entry holds state/owner/last_committer/committed/pending/prev.
        That makes the checkpoint sufficient on its own for recovery *and*
        for reclaim: no committed-but-unflushed write and no active
        transaction's undo value ever needs a WAL record below the
        checkpoint's own position.  Bounded by the dirty-page set, not by
        history length.
        """
        images = {}
        for pid in sorted(dirty_pages):
            slots = self._get_page(pid)  # reconciles decided pending slots
            per_page = {}
            for sid, s in enumerate(slots):
                per_page[str(sid)] = {
                    "state": int(s["state"]),
                    "pending_owner": int(s["pending_owner"]),
                    "last_committer": int(s["last_committer"]),
                    "committed": int(s["committed"]),
                    "pending": int(s["pending"]),
                    "prev": int(s["prev"]),
                }
            images[str(pid)] = per_page
        return images

    # ---------------------------------------------------- recovery helpers
    def _write_abort_record(self, txid):
        self._wal.append(records.encode(records.REC_ABORT, txid))
        self._wal.fsync()

    # ------------------------------------------------------------- inspect
    def inspect_durable_page(self, page_id):
        """Read the *on-disk* page, bypassing cache (for crash evidence)."""
        with self._txn_lock:
            return pageio.inspect_page(self._pages_dir, page_id, self.stats)

    def durable_page_exists(self, page_id):
        with self._txn_lock:
            return os.path.exists(os.path.join(
                self._pages_dir, pageio.page_filename(page_id)))

    # ------------------------------------------------------- online backup
    def _retention_dir(self, bid):
        # Kept OUTSIDE the staging tree itself: staging is renamed wholesale
        # into the published target, and retained page versions must never
        # travel with it.  Both live under the swept staging root.
        return os.path.join(self.dir, backup_mod.STAGING_ROOT,
                            "retention", bid)

    def _register_backup(self):
        """Create a backup session and start retaining replaced page files."""
        with self._txn_lock:
            self._check_ready()
            self._backup_seq += 1
            bid = f"bk-{os.getpid()}-{self._backup_seq:08d}"
            os.makedirs(self._retention_dir(bid), exist_ok=True)
            # ``needs_pages`` lists the pages whose barrier version the
            # backup may still have to read from the source; it shrinks to
            # empty once every page file is copied.  ``cutoff`` and
            # ``cp_file`` are filled at the barrier.  Reclaim holds the
            # cutoff segment and the barrier checkpoint file, and the page
            # version-retention hook guarantees pinned versions are never
            # lost (reclaim itself never touches ``pages/``).
            self._active_backups[bid] = {
                "retention": self._retention_dir(bid),
                "cutoff": None,
                "phase": "registered",
                "needs_pages": set(),
            }
            return bid

    def _backup_barrier_record(self, bid, cutoff, page_versions, phase,
                               cp_file=None):
        """Attach the barrier cutoff and pinned page set to a session.

        Reclaim reads exactly this record: the cutoff segment and the pinned
        checkpoint file are protected until the backup unregisters (whether it
        succeeds or fails), and nothing below the cutoff is.
        """
        with self._txn_lock:
            info = self._active_backups.get(bid)
            if info is None:
                return
            info["cutoff"] = dict(cutoff)
            info["page_versions"] = dict(page_versions)
            info["needs_pages"] = set(page_versions)
            info["cp_file"] = cp_file
            info["phase"] = phase

    def _unregister_backup(self, bid):
        """Drop retention for a finished/failed backup and delete its files."""
        with self._txn_lock:
            self._active_backups.pop(bid, None)
            root = os.path.join(self.dir, backup_mod.STAGING_ROOT)
            # The published target was renamed out of this path; what remains
            # (retention copies plus the backup's staging subtree) is private
            # to the now-finished backup.  Remove its pieces first.
            shutil.rmtree(os.path.join(root, "retention", bid),
                          ignore_errors=True)
            shutil.rmtree(os.path.join(root, bid), ignore_errors=True)
            # Remove now-empty intermediate directories.  os.rmdir only
            # succeeds on an empty dir, so a concurrent backup's subtree
            # keeps these paths in place and these calls are no-ops then.
            for d in (os.path.join(root, "retention"), root):
                try:
                    os.rmdir(d)
                except OSError:
                    pass
            self._backup_done.notify_all()

    # ------------------------------------------------------------- reclaim
    def reclaim_plan(self):
        """Read-only preview of reclaimable history (no writes, no deletes).

        Safe to call while transactions and online backups are running: it
        takes only a short snapshot under the transaction lock and reports
        both removable files and the exact requirement that pins every file
        it says must stay.
        """
        return reclaim_mod.plan_reclaim(self)

    def reclaim(self, keep_checkpoints=reclaim_mod.DEFAULT_CP_KEEP, pause=None):
        """Physically free engine-managed history that is no longer needed.

        Dependencies are re-decided from scratch at execution time.  See
        :mod:`pagestore.reclaim` for the ordering guarantees: replacement
        recovery basis durable first, then a durable manifest, then unlocked
        real unlinks.  Returns the lists of actually removed WAL segments and
        checkpoints with byte counts, plus keep reasons, the recoverable
        entry and the WAL range that must stay.
        """
        return reclaim_mod.run_reclaim(self, keep_cp=keep_checkpoints,
                                       pause=pause)

    def published_page_ids(self):
        """Page ids with at least one durable publication (for snapshots)."""
        with self._txn_lock:
            # Reconcile with the directory: pages published by a previous
            # process and untouched in this one exist on disk even though the
            # in-memory version map starts empty.
            ids = set(self._page_versions)
            if os.path.isdir(self._pages_dir):
                for name in os.listdir(self._pages_dir):
                    if name.startswith("p-") and not name.endswith(".tmp"):
                        try:
                            ids.add(int(name[2:]))
                        except ValueError:
                            continue
            for pid in ids:
                self._page_versions.setdefault(pid, 1)
            return sorted(ids)


def _encode_meta(meta: dict) -> bytes:
    import json
    return json.dumps(meta, sort_keys=True).encode("utf-8")


def _decode_meta(body: bytes) -> dict:
    import json
    from .framing import CorruptionError
    try:
        d = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CorruptionError(f"meta invalid: {exc}", META_NAME)
    for k in ("num_pages", "slots_per_page", "initial_value",
              "wal_segment_size", "cache_budget", "version"):
        if k not in d:
            raise CorruptionError(f"meta missing {k}", META_NAME)
    return d


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
