"""Independent expectations for the reclaim acceptance suite.

This module imports **nothing** from the engine or from ``tests.recovery_oracle``.
It re-derives, from the public operation stream a driver chose to perform
plus its own durable-commit confirmations, exactly what every slot must hold
and which decisions must be queryable.  The reclaimer must never change any
of it.

It is intentionally tiny and replay-based:

* a commit makes that transaction's final full post-image visible;
* an abort (or an undecided transaction) never installs anything;
* ``add`` is recorded as a concrete resulting value, so double-application is
  impossible to hide;
* transaction ids come from the driver's ``begin`` order, so "new ids never
  go backwards" is checkable without asking the engine.
"""


class Expect:
    def __init__(self, num_pages, slots_per_page, initial=0):
        self.pages = num_pages
        self.slots = slots_per_page
        self.initial = initial
        self.next_txn = 1
        self.working = {}       # txn -> {(p,s): value}
        self.committed = {}     # (p,s) -> value
        self.decisions = {}     # txn -> "committed"|"aborted"
        self.order = []         # durable confirmation order

    # ---- public operations the driver performed -----------------------
    def begin(self):
        t = self.next_txn
        self.next_txn += 1
        self.working[t] = {}
        return t

    def set(self, t, p, s, value):
        self.working[t][(p, s)] = value

    def add(self, t, p, s, delta):
        cur = self.working[t].get((p, s), self.committed.get((p, s),
                                                             self.initial))
        self.working[t][(p, s)] = cur + delta
        return cur + delta

    def commit_confirmed(self, t):
        for key, value in self.working[t].items():
            self.committed[key] = value
        self.decisions[t] = "committed"
        self.order.append(t)
        self.working[t] = {}

    def abort_confirmed(self, t):
        self.decisions[t] = "aborted"
        self.order.append(t)
        self.working[t] = {}

    def flush(self, p):
        """Public operation: durability of the page, not a decision."""

    def checkpoint(self):
        """Public operation: a recovery entry point, not a decision."""

    def abandon(self, t):
        """A transaction that never got a durable decision (crash)."""
        self.decisions[t] = "aborted"
        self.working[t] = {}

    # ---- predictions ---------------------------------------------------
    def slots_now(self):
        world = {}
        for p in range(self.pages):
            for s in range(self.slots):
                world[(p, s)] = self.committed.get((p, s), self.initial)
        return world

    def decisions_now(self):
        return dict(self.decisions)

    def next_txn_lower_bound(self):
        return self.next_txn
