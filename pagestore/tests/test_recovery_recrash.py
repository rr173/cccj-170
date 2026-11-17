"""Repeated crashes *during* recovery, at every stable recovery fault point.

A workload is created and crashed as a separate process, then recovery is
reopened repeatedly, arming a different recovery fault point each time.
Every intermediate state must converge to the same committed result, and
undo/redo/version handling must never be mistaken for new business work
(no double add, no resurrected abort).
"""

import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from pagestore import open_store
from pagestore.crash import FaultInjector
from tests.reference import ReferenceModel

PS = os.path.join(ROOT, "bin", "ps.py")
RECOVERY_POINTS = [
    "recovery_after_record_scan",
    "recovery_mid_cleanup",
    "recovery_mid_page_write",
]


def run(cmd, expect):
    p = subprocess.run([sys.executable, "-I"] + cmd,
                       capture_output=True, text=True)
    assert p.returncode in expect, (cmd, p.returncode, p.stderr)
    return p


def build_crashed_store(d):
    """Headline-style workload ending in a hard crash (no close)."""
    run([PS, "create", d, "--pages", "4", "--slots", "3",
         "--budget", "2"], (0,))
    run([PS, "phase", d, "main_demo", "--hard-exit", "--exit-code", "98"],
        (98,))


def snapshot(d):
    st = open_store(d)
    vals = {(p, s): st.read(p, s)
            for p in range(st.meta["num_pages"])
            for s in range(st.meta["slots_per_page"])}
    dec = dict(st._decisions)
    st.close()
    return vals, dec


def main():
    tmp = tempfile.mkdtemp(prefix="pagestore-recrash-")
    failures = 0
    for point in RECOVERY_POINTS:
        d = os.path.join(tmp, point.replace("_", "-"))
        build_crashed_store(d)
        faults = FaultInjector(os.path.join(d, "faults"))
        # Crash recovery at this point, then reopen to completion.
        faults.arm(point)
        run([PS, "read-all", d], (99,))
        # Arm the same point again: a second crash at the same phase must
        # still converge (idempotent re-entry).
        faults.arm(point)
        run([PS, "read-all", d], (99,))
        vals, dec = snapshot(d)
        ref = ReferenceModel(4, 3, 0)
        t1, t2, t3 = ref.begin(), ref.begin(), ref.begin()
        ref.add(t1, 0, 0, 10)
        ref.set(t2, 0, 1, 20)
        ref.commit(t2)
        ref.add(t3, 1, 0, 30)
        ref.commit(t3)
        expected = ref.crash({t2, t3}).all_slots()
        ok = vals == expected
        dec_ok = dec.get(t1) == "aborted" and dec.get(t2) == "committed" \
            and dec.get(t3) == "committed"
        print(f"{'✓' if ok and dec_ok else '✗'} recovery crash at {point}: "
              f"X={vals[(0,0)]} Y={vals[(0,1)]} Z={vals[(1,0)]} "
              f"decisions={dec}")
        failures += 0 if (ok and dec_ok) else 1
        # One more clean reopen to prove stability after convergence.
        vals2, _ = snapshot(d)
        if vals2 != vals:
            print("✗ non-deterministic reopen after convergence")
            failures += 1
    print(f"{'✓' if failures == 0 else '✗'} recovery re-crash matrix: "
          f"{len(RECOVERY_POINTS)} points, repeated twice each")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
