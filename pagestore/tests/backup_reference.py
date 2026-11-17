"""Independent in-memory model for *backup* consistency checks.

Like :mod:`tests.reference`, this imports nothing from the engine's WAL,
page, checkpoint or recovery code.  Its only inputs are

* the public operation stream (begin/set/add/commit/abort/flush/checkpoint/
  backup lifecycle events) recorded by the driver process, and
* the driver's own notion of which commit confirmations were *durable*
  (recorded after ``commit()`` returned, which is exactly when the engine
  has fsynced the decision).

It enumerates the cutoff positions that a concurrent backup may legally
land at and predicts every slot value for each position, so the verifier
can slot-by-slot compare a recovered backup against a model that never
looked at the product's log analysis or recovery code.
"""

INT_MIN = -(1 << 63)
INT_MAX = (1 << 63) - 1


class BackupModelError(Exception):
    pass


class BackupReferenceModel:
    def __init__(self, num_pages, slots_per_page, initial_value=0):
        self.num_pages = num_pages
        self.slots_per_page = slots_per_page
        self.initial = initial_value
        self.next_txn = 1
        self.working = {}             # txn -> {(p,s): value}
        self.locks = {}               # (p,s) -> txn
        # Durable, ordered facts observed by the driver itself.
        self.durable_commits = []     # txids in durable-confirmation order
        self.commit_index = {}        # txid -> 0-based position in that list
        self.durable_aborts = []
        self.decisions = {}           # txid -> "committed"/"aborted"
        self.committed = {}           # (p,s) -> last durable committed value
        # Bounded event log mirroring the driver journal.
        self.events = []

    # ------------------------------------------------------------- ops
    def begin(self):
        t = self.next_txn
        self.next_txn += 1
        self.working[t] = {}
        self.events.append(("begin", t))
        return t

    def _write(self, txn, p, s, value):
        if not (INT_MIN <= value <= INT_MAX):
            raise BackupModelError("value out of int64 range")
        owner = self.locks.get((p, s))
        if owner is not None and owner != txn:
            raise BackupModelError(f"slot ({p},{s}) locked by {owner}")
        self.locks[(p, s)] = txn
        self.working[txn][(p, s)] = value

    def set(self, txn, p, s, value):
        self.events.append(("set", txn, p, s, value))
        self._write(txn, p, s, value)

    def add(self, txn, p, s, delta):
        cur = self.working[txn].get((p, s),
                                    self.committed.get((p, s), self.initial))
        new = cur + delta
        if not (INT_MIN <= new <= INT_MAX):
            raise BackupModelError("add overflow")
        self.events.append(("add", txn, p, s, delta, new))
        self._write(txn, p, s, new)

    def commit_confirmed(self, txn):
        """Record a *durable* commit confirmation (engine call returned)."""
        for (p, s), value in self.working[txn].items():
            self.committed[(p, s)] = value
            if self.locks.get((p, s)) == txn:
                del self.locks[(p, s)]
        self.commit_index[txn] = len(self.durable_commits)
        self.durable_commits.append(txn)
        self.decisions[txn] = "committed"
        self.working[txn] = {}
        self.events.append(("commit", txn))

    def abort_confirmed(self, txn):
        for key in list(self.working[txn]):
            if self.locks.get(key) == txn:
                del self.locks[key]
        self.durable_aborts.append(txn)
        self.decisions[txn] = "aborted"
        self.working[txn] = {}
        self.events.append(("abort", txn))

    def flush(self, p):
        self.events.append(("flush", p))

    def checkpoint_event(self):
        self.events.append(("checkpoint",))

    # ---------------------------------------------------- cutoff reasoning
    def allowed_cutoffs(self, backup_start_event, backup_end_event):
        """Every legal number of durable commits at the backup cutoff.

        The backup starts at ``backup_start_event`` and returns at
        ``backup_end_event`` (indices into ``events``).  Its single cutoff
        may include any prefix of the durable commits that were confirmed
        within that window, but must include every commit confirmed before
        the window started and must exclude every commit confirmed after
        the window ended.  Aborts are not part of the commit sequence but
        are checked separately against the marker.
        """
        before = set()
        inside = []
        after = set()
        for idx, txid in enumerate(self.durable_commits):
            ev_pos = self._event_pos_of_decision(txid)
            if ev_pos < backup_start_event:
                before.add(txid)
            elif ev_pos <= backup_end_event:
                inside.append(txid)
            else:
                after.add(txid)
        base = len(before)
        allowed = set(range(base, base + len(inside) + 1))
        return {
            "must_include": sorted(before),
            "window_commits": inside,
            "must_exclude": sorted(after),
            "allowed_commit_prefix_lengths": sorted(allowed),
        }

    def _event_pos_of_decision(self, txid):
        for i, ev in enumerate(self.events):
            if ev[0] in ("commit", "abort") and ev[1] == txid:
                return i
        raise BackupModelError(f"no decision event for txn {txid}")

    def state_at_prefix(self, prefix_len):
        """Predicted committed slot world after the first ``prefix_len``
        durable commits (and all durable aborts known up to that point)."""
        world = {}
        for p in range(self.num_pages):
            for s in range(self.slots_per_page):
                world[(p, s)] = self.initial
        # Replay the durable commit prefix in confirmation order.  The
        # committed value of each transaction is its final working value,
        # reconstructed below from the operation stream independently.
        writes = self._final_writes_by_txn()
        for txid in self.durable_commits[:prefix_len]:
            for (p, s), value in writes[txid].items():
                world[(p, s)] = value
        return world

    def _final_writes_by_txn(self):
        """Re-derive each transaction's final full post-image from the
        public operation stream, without touching engine state."""
        final = {}
        scratch = {}
        lock_owner = {}
        next_t = 1
        for ev in self.events:
            if ev[0] == "begin":
                scratch[ev[1]] = {}
            elif ev[0] == "set":
                _, t, p, s, v = ev
                owner = lock_owner.get((p, s))
                if owner is not None and owner != t:
                    raise BackupModelError("conflict in model replay")
                lock_owner[(p, s)] = t
                scratch[t][(p, s)] = v
            elif ev[0] == "add":
                _, t, p, s, delta, newv = ev
                lock_owner[(p, s)] = t
                scratch[t][(p, s)] = newv
            elif ev[0] == "commit":
                final[ev[1]] = dict(scratch[ev[1]])
                for key in list(lock_owner):
                    if lock_owner[key] == ev[1]:
                        del lock_owner[key]
                scratch[ev[1]] = {}
            elif ev[0] == "abort":
                for key in list(lock_owner):
                    if lock_owner[key] == ev[1]:
                        del lock_owner[key]
                scratch[ev[1]] = {}
        return final

    def all_slots_at(self, prefix_len):
        return self.state_at_prefix(prefix_len)

    def marker_prediction(self, prefix_len):
        """What the backup marker must say for a cutoff at this prefix."""
        included = self.durable_commits[:prefix_len]
        max_included = included[-1] if included else 0
        # next_txn_id must be greater than every txid visible at the cutoff.
        next_txn = 1
        for ev in self.events:
            if ev[0] == "begin":
                # A txid may have been allocated even without a durable
                # decision; the cutoff's next_txn_id dominates them all up
                # to the barrier point, validated loosely by the verifier.
                next_txn = max(next_txn, ev[1] + 1)
        return {
            "committed_txids": list(included),
            "last_commit_txid": max_included,
            "next_txn_id_lower_bound": next_txn,
        }
