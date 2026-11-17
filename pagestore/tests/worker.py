"""Standalone process that executes one scenario and records a manifest.

The manifest is the bridge to the independent reference model: it lists the
public operations and the set of transactions whose decision was durable at
the moment the process died.  Durability is observed through the engine's
stable fault points (which sit exactly at the fsync boundaries), never by
peeking at the recovery code.

Because an armed fault terminates the process with ``os._exit`` (no
``finally``), the manifest is re-persisted after every operation, so the
last fully written manifest always reflects the real durable boundary.
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from pagestore import open_store  # noqa: E402
from pagestore.crash import FaultInjector  # noqa: E402
from tests.scenarios import SCENARIOS, write_manifest  # noqa: E402


def main():
    store_dir = sys.argv[1]
    scenario = sys.argv[2]
    st = open_store(store_dir)
    ops = []
    durable = set()
    faults_dir = os.path.join(store_dir, "faults")

    def persist():
        write_manifest(store_dir, {
            "ops": list(ops),
            "durable_at_crash": sorted(durable),
        })

    def log(kind, **kw):
        ops.append({"kind": kind, **kw})
        persist()  # durable manifest before any potentially fatal next step

    # An armed AFTER-fsync fault means the decision will be durable even
    # though the process dies before replying.  Record that boundary in the
    # manifest so the independent model predicts commit, not rollback.
    reply_fault_armed = os.path.exists(
        os.path.join(faults_dir,
                     "commit_after_decision_fsync.fault"))

    class Recorder:
        def __init__(self, s):
            self.s = s

        def begin(self):
            t = self.s.begin()
            log("begin", txn=t)
            return t

        def set(self, t, p, slot, v):
            self.s.set(t, p, slot, v)
            log("set", txn=t, p=p, s=slot, v=v)

        def add(self, t, p, slot, delta):
            self.s.add(t, p, slot, delta)
            log("add", txn=t, p=p, s=slot, v=delta)

        def commit(self, t):
            if reply_fault_armed:
                # The fault sits after the decision fsync: durability is
                # guaranteed, so publish that fact before the fatal call.
                durable.add(t)
                log("commit", txn=t, durable=True)
                self.s.commit(t)          # hard-exits after fsync
            else:
                log("commit_attempt", txn=t)  # not durable yet
                self.s.commit(t)              # may hard-exit pre-fsync
                durable.add(t)
                ops.pop()
                log("commit", txn=t, durable=True)

        def abort(self, t):
            log("abort_attempt", txn=t)
            self.s.abort(t)
            durable.add(t)
            ops.pop()
            log("abort", txn=t, durable=True)

        def flush_page(self, p):
            self.s.flush_page(p)

        def checkpoint(self):
            self.s.checkpoint()

        def hard_exit(self, code):
            # Persist the manifest first so the reference model sees exactly
            # which operations existed, then emulate a process crash.
            persist()
            os._exit(code)

    rec = Recorder(st)
    SCENARIOS[scenario](rec)
    persist()
    st.close()


if __name__ == "__main__":
    main()
