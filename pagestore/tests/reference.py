"""Independent in-memory reference model.

This model re-derives the correct slot contents purely from the public
transactional operation stream (begin/set/add/commit/abort) plus, for crash
scenarios, which transactions had a *durable* decision at crash time.  It
deliberately imports nothing from the engine's WAL, page or recovery code,
so tests compare two genuinely independent implementations slot by slot.

Durability rule mirrored here: a commit/abort only counts once its decision
is durable.  The test driver knows that boundary from explicit fsync
completion signals, not from engine internals.
"""

INT_MIN = -(1 << 63)
INT_MAX = (1 << 63) - 1


class RefOverflow(Exception):
    pass


class RefConflict(Exception):
    pass


class ReferenceModel:
    def __init__(self, num_pages, slots_per_page, initial_value=0):
        self.num_pages = num_pages
        self.slots_per_page = slots_per_page
        self.initial = initial_value
        # committed[(p,s)] and pending[(txn,(p,s))] working values
        self.committed = {}
        self.working = {}          # txid -> {(p,s): value}
        self.locks = {}            # (p,s) -> txid
        self.active = set()
        self.next_txn = 1
        # Decisions that are *durable*; mirror of what recovery must honor.
        self.durable_decisions = {}

    def begin(self):
        t = self.next_txn
        self.next_txn += 1
        self.active.add(t)
        self.working[t] = {}
        return t

    def _read_committed(self, p, s):
        return self.committed.get((p, s), self.initial)

    def read(self, p, s, txn=None):
        if txn is not None and (p, s) in self.working.get(txn, {}):
            return self.working[txn][(p, s)]
        return self._read_committed(p, s)

    def _write(self, txn, p, s, value):
        if not (INT_MIN <= value <= INT_MAX):
            raise RefOverflow("value out of range")
        owner = self.locks.get((p, s))
        if owner is not None and owner != txn:
            raise RefConflict(f"slot locked by {owner}")
        self.locks[(p, s)] = txn
        self.working[txn][(p, s)] = value

    def set(self, txn, p, s, value):
        self._write(txn, p, s, value)

    def add(self, txn, p, s, delta):
        cur = self.read(p, s, txn)
        new = cur + delta
        if not (INT_MIN <= new <= INT_MAX):
            # Rejected before any state change.
            raise RefOverflow("add overflow")
        self._write(txn, p, s, new)

    def commit(self, txn, durable=True):
        for (p, s), value in self.working[txn].items():
            self.committed[(p, s)] = value
            del self.locks[(p, s)]
        self.active.discard(txn)
        if durable:
            self.durable_decisions[txn] = "committed"
        self.working[txn] = {}

    def abort(self, txn, durable=True):
        for key in list(self.working[txn]):
            if self.locks.get(key) == txn:
                del self.locks[key]
        self.active.discard(txn)
        if durable:
            self.durable_decisions[txn] = "aborted"
        self.working[txn] = {}

    def crash(self, durable_txn_ids, final_working=None):
        """Return the post-recovery committed world given durable decisions.

        Transactions without a durable decision never take effect; their
        writes are discarded wholesale.  Committed transactions install their
        final full working value (idempotent: one install, never re-added).
        ``final_working`` supplies the last working set for a transaction
        whose in-model commit ran, mirroring a decision that became durable
        just before the process died while still holding the transaction.
        """
        final_working = final_working or {}
        result = ReferenceModel(self.num_pages, self.slots_per_page,
                                self.initial)
        # Start from values committed durably before the crash window.
        result.committed = dict(self.committed)
        # Apply only transactions whose decision is durable, in txid order.
        for t in sorted(durable_txn_ids):
            decision = self.durable_decisions.get(t)
            if decision is None:
                continue
            if decision == "committed":
                writes = final_working.get(t)
                if writes:
                    for (p, s), value in writes.items():
                        result.committed[(p, s)] = value
                result.durable_decisions[t] = "committed"
            else:
                result.durable_decisions[t] = "aborted"
        return result

    def all_slots(self):
        out = {}
        for p in range(self.num_pages):
            for s in range(self.slots_per_page):
                out[(p, s)] = self._read_committed(p, s)
        return out
