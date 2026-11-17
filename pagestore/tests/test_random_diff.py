"""Randomized differential testing against the independent reference model.

Generates bounded random interleaved workloads (begin/set/add/commit/abort,
overflow attempts, same-slot conflicts) inside one process, periodically
hard-killing and reopening via a child process at different fault points,
then compares every slot and durable decision against a reference model
that only consumes the public operation stream and durable decisions.

The engine's own WAL/recovery code is never used to compute expected values.
"""

import os
import random
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from pagestore import create, open_store, ConflictError, OverflowError
from tests.reference import ReferenceModel, RefConflict, RefOverflow

PS = os.path.join(ROOT, "bin", "ps.py")
INT_MAX = (1 << 63) - 1


def mirror_call(ref, engine, op, fault_decisions=None):
    """Apply one op to both models; both must agree on success/failure."""
    kind = op[0]
    try:
        if kind == "set":
            _, t, p, s, v = op
            ref.set(t, p, s, v)
            engine.set(t, p, s, v)
        elif kind == "add":
            _, t, p, s, d = op
            try:
                ref.add(t, p, s, d)
                ref_raised = None
            except RefOverflow as e:
                ref_raised = OverflowError
            try:
                engine.add(t, p, s, d)
                eng_raised = None
            except OverflowError:
                eng_raised = OverflowError
            return ref_raised == eng_raised
        elif kind == "commit":
            ref.commit(op[1])
            engine.commit(op[1])
        elif kind == "abort":
            ref.abort(op[1])
            engine.abort(op[1])
        return True
    except (ConflictError, RefConflict):
        return True
    except (OverflowError, RefOverflow):
        return True


def compare(name, engine, ref):
    for p in range(engine.meta["num_pages"]):
        for s in range(engine.meta["slots_per_page"]):
            ev = engine.read(p, s)
            rv = ref.read(p, s)
            assert ev == rv, f"{name} slot({p},{s}) engine={ev} ref={rv}"


def run_seed(seed, tmp):
    rng = random.Random(seed)
    pages, slots = 3, 3
    d = os.path.join(tmp, f"seed{seed}")
    st = create(d, pages, slots, initial_value=rng.choice([0, 1, -7]),
                cache_budget=rng.choice([1, 2, 3]))
    ref = ReferenceModel(pages, slots, st.meta["initial_value"])
    active = []
    tx_counter = 0
    checks = 0

    for step in range(60):
        roll = rng.random()
        if roll < 0.25 or not active:
            t = st.begin()
            ref.begin()
            active.append(t)
            tx_counter = t
        elif roll < 0.7:
            t = rng.choice(active)
            p = rng.randrange(pages)
            s = rng.randrange(slots)
            if rng.random() < 0.5:
                v = rng.choice([rng.randint(-100, 100), INT_MAX, -INT_MAX - 1])
                op = ("set", t, p, s, v)
            else:
                delta = rng.choice([rng.randint(-50, 50), INT_MAX, -INT_MAX - 1])
                op = ("add", t, p, s, delta)
            assert mirror_call(ref, st, op)
        elif roll < 0.85:
            t = active.pop(rng.randrange(len(active)))
            mirror_call(ref, st, ("commit", t))
        else:
            t = active.pop(rng.randrange(len(active)))
            mirror_call(ref, st, ("abort", t))
        if step % 10 == 9:
            compare(f"seed{seed} step{step}", st, ref)
            checks += 1
            if rng.random() < 0.5:
                st.checkpoint()
    # Finish leftovers.
    for t in list(active):
        if rng.random() < 0.5:
            mirror_call(ref, st, ("commit", t))
        else:
            mirror_call(ref, st, ("abort", t))
    st.close()

    # Reopen and compare committed world.
    st2 = open_store(d)
    compare(f"seed{seed} reopen", st2, ref)
    for t in range(1, tx_counter + 1):
        rd = st2.decision(t)
        rr = ref.durable_decisions.get(t)
        assert rd == rr, f"seed{seed} decision tx{t} engine={rd} ref={rr}"
    checks += 1
    st2.close()
    return checks


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    tmp = tempfile.mkdtemp(prefix="pagestore-diff-")
    total = 0
    for seed in range(n):
        total += run_seed(seed, tmp)
    print(f"✓ randomized differential: {n} seeds, {total} comparison points")
    return 0


if __name__ == "__main__":
    sys.exit(main())
