"""Scenario workers run as separate processes for real crash semantics.

Each scenario is a function receiving an open :class:`Storage`.  Crash
points are armed through the stable fault-injection markers; the worker is
then launched as its own OS process so ``os._exit`` is a genuine process
death rather than an in-process exception.  A companion manifest (JSON)
records the operation stream so the independent reference model can predict
the expected post-recovery slot values without touching engine internals.
"""

import json
import os

from pagestore import create, open_store
from pagestore.crash import FaultInjector
from pagestore.errors import ConflictError, OverflowError

MANIFEST = "manifest.json"


def _manifest_path(store_dir):
    return os.path.join(store_dir, MANIFEST)


def write_manifest(store_dir, data):
    path = _manifest_path(store_dir)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_manifest(store_dir):
    with open(_manifest_path(store_dir)) as f:
        return json.load(f)


# ------------------------------------------------------------- scenarios
def scenario_long_txn(st):
    """A transaction spans multiple checkpoints and many other commits."""
    tlong = st.begin()
    st.set(tlong, 0, 0, 111)
    for i in range(5):
        tk = st.begin()
        st.set(tk, 1, 0, 100 + i)
        st.commit(tk)
        st.checkpoint()                 # long txn stays active across
    st.set(tlong, 0, 1, 222)
    return {"tlong": tlong}


def scenario_partial_flush(st):
    """Multi-page transaction with only some pages flushed before crash."""
    t = st.begin()
    st.set(t, 0, 0, 1)
    st.set(t, 1, 0, 2)
    st.set(t, 2, 0, 3)
    st.flush_page(0)                    # only page 0 durable with pending
    st.flush_page(1)                    # page 2 never written
    return {"t": t}


def scenario_abort_interleave(st):
    """Explicit abort interleaved with another committer on the same page."""
    t1 = st.begin()
    st.set(t1, 0, 0, 9)                # X
    t2 = st.begin()
    st.set(t2, 0, 1, 8)                # Y same page
    st.commit(t2)
    st.flush_page(0)
    st.abort(t1)                        # X must undo, Y survive
    t3 = st.begin()
    st.add(t3, 0, 0, 5)                # X from initial 0
    st.commit(t3)
    return {"t1": t1, "t2": t2, "t3": t3}


def scenario_commit_reply_crash(st):
    """Decision durable, reply not delivered."""
    t = st.begin()
    st.add(t, 0, 0, 7)
    st.commit(t)                        # fault fires after decision fsync
    return {"t": t, "note": "should never print: process dies first"}


def scenario_commit_predecision_crash(st):
    """Crash before the commit decision is fsynced: must not take effect."""
    t = st.begin()
    st.set(t, 0, 0, 77)
    st.commit(t)                        # fault fires before decision fsync
    return {"t": t}


def scenario_cp_switch_crash(st):
    """Crash between promoting a checkpoint and switching 'current'."""
    t = st.begin()
    st.set(t, 0, 0, 44)
    st.commit(t)
    st.checkpoint()                     # fault during the switch
    return {"t": t}


def scenario_partial_flush_kill(rec):
    """Multi-page pending txn, two pages flushed, then hard kill (no close)."""
    t = rec.begin()
    rec.set(t, 0, 0, 1)
    rec.set(t, 1, 0, 2)
    rec.set(t, 2, 0, 3)
    rec.flush_page(0)
    rec.flush_page(1)
    rec.hard_exit(97)


SCENARIOS = {
    "long_txn": scenario_long_txn,
    "partial_flush": scenario_partial_flush,
    "partial_flush_kill": scenario_partial_flush_kill,
    "abort_interleave": scenario_abort_interleave,
    "commit_reply_crash": scenario_commit_reply_crash,
    "commit_predecision_crash": scenario_commit_predecision_crash,
    "cp_switch_crash": scenario_cp_switch_crash,
}
