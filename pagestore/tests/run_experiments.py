"""Process-driven test harness with independent reference comparison.

Every crash test runs in its own OS process via ``bin/ps.py`` or the scenario
worker.  The harness:

1. builds the same operation stream in the independent :mod:`reference`
   model;
2. drives the real engine through real processes and real fault markers;
3. reopens (possibly repeatedly, with a crash *during* recovery);
4. compares every slot and every durable decision against the reference;
5. asserts on actual disk read/write counters and WAL scan ranges.

Run:  python3 tests/run_experiments.py [--quick]
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from pagestore import open_store  # noqa: E402
from pagestore.crash import FaultInjector  # noqa: E402
from tests.reference import ReferenceModel, RefConflict, RefOverflow  # noqa: E402

PS = os.path.join(ROOT, "bin", "ps.py")
WORKER = os.path.join(HERE, "worker.py")
PASS, FAIL = "PASS", "FAIL"
results = []


def run(cmd, expect_codes=(0,)):
    proc = subprocess.run([sys.executable, "-I"] + cmd,
                          capture_output=True, text=True)
    ok = proc.returncode in expect_codes
    return ok, proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def record(name, ok, detail=""):
    results.append((name, PASS if ok else FAIL, detail))
    flag = "✓" if ok else "✗"
    print(f"  {flag} {name}" + (f"  {detail}" if detail else ""))


def fresh_store(base, pages=5, slots=4, initial=0, budget=2):
    d = os.path.join(base, "db")
    ok, code, out, err = run([PS, "create", d, "--pages", str(pages),
                              "--slots", str(slots), "--initial",
                              str(initial), "--budget", str(budget)])
    assert ok, f"create failed: {err}"
    return d


def read_all(d):
    st = open_store(d)
    try:
        vals = {(p, s): st.read(p, s)
                for p in range(st.meta["num_pages"])
                for s in range(st.meta["slots_per_page"])}
        decisions = dict(st._decisions)
        stats = st.stats.as_dict()
    finally:
        st.close()
    return vals, decisions, stats


def compare_slots(name, d, ref):
    vals, decisions, stats = read_all(d)
    expected = ref.all_slots()
    ok = vals == expected
    record(name + " slot values", ok,
           "" if ok else f"engine={vals} ref={expected}")
    return ok, vals, decisions, stats


# --------------------------------------------------------------- tests
def test_headline(base):
    print("[headline: T1 active, T2 same-page commit, T3 no-flush, crash]")
    d = fresh_store(base, pages=4, slots=3, budget=2)
    ref = ReferenceModel(4, 3, 0)
    t1, t2, t3 = ref.begin(), ref.begin(), ref.begin()
    ref.add(t1, 0, 0, 10)
    ref.set(t2, 0, 1, 20)
    ref.commit(t2)
    ref.add(t3, 1, 0, 30)
    ref.commit(t3)
    # T1 still active at crash.

    ok, code, out, err = run([PS, "phase", d, "main_demo",
                              "--hard-exit", "--exit-code", "98"],
                             expect_codes=(98,))
    record("hard crash without close returns 98", ok, err)

    # Durable evidence BEFORE recovery.
    ok, code, p0, err = run([PS, "inspect-raw", d, "0"])
    slots0 = json.loads(p0)["slots"]
    record("durable page0 held T1 uncommitted pending X=10",
           ok and slots0[0]["state"] == 1 and slots0[0]["pending"] == 10
           and slots0[0]["pending_owner"] == t1)
    record("durable page0 already had T2 committed Y=20",
           slots0[1]["committed"] == 20)
    ok, code, p1, err = run([PS, "inspect-raw", d, "1", "--expect-missing"])
    record("Z page was never published before crash",
           json.loads(p1)["exists"] is False)

    # Crash DURING recovery after some page writes.
    FaultInjector(os.path.join(d, "faults")).arm("recovery_mid_page_write")
    ok, code, out, err = run([PS, "read-all", d], expect_codes=(99,))
    record("recovery itself can be hard-killed (99)", ok, err)

    # Reference prediction: only durable decisions are T2,T3 committed.
    ref_after = ref.crash({t2, t3})
    ok, vals, decisions, stats = compare_slots("after second recovery", d,
                                               ref_after)
    record("T1 aborted, T2/T3 committed",
           decisions.get(t1) == "aborted"
           and decisions.get(t2) == "committed"
           and decisions.get(t3) == "committed",
           str(decisions))
    record("recovery read only WAL suffix from checkpoint",
           stats["wal_bytes_read"] <= 260, str(stats["notes"]))
    # Reopen again: must be stable/idempotent.
    ok2, vals2, dec2, stats2 = compare_slots("third reopen stays identical",
                                             d, ref_after)
    record("add never applied twice across recoveries",
           vals[(1, 0)] == 30 and vals2[(1, 0)] == 30)


def test_history_prefix_skipped(base):
    print("[long finished history: new recovery must not read the prefix]")
    d = fresh_store(base, pages=5, slots=4, budget=3)
    # Build substantial committed history in one process.
    ok, code, out, err = run([PS, "phase", d, "more_history"])
    record("history phase completes", ok, err)
    seg1 = os.path.join(d, "wal", "seg-00000001")
    size_after_history = os.path.getsize(seg1)

    # A new transaction then a clean checkpoint, and reopen.  Because the
    # checkpoint sits at the end of the log, recovery's disk read of that
    # segment must be tiny (the seek skips the whole history prefix).
    st = open_store(d)
    t = st.begin()
    st.set(t, 3, 0, 1234)
    st.commit(t)
    st.checkpoint()
    st.close()
    st = open_store(d)
    notes = " ".join(st.stats.notes)
    wal_read = st.stats.wal_bytes_read
    st.close()
    record("recovery entry is a checkpoint, not log byte 0",
           "recovery_entry=" in notes
           and "wal_scan_start=seg1:0 " not in notes, notes)
    record("WAL prefix never read from disk (seek + tiny suffix)",
           wal_read < 64, f"read={wal_read} history={size_after_history}")


def test_partial_flush_hard_kill(base):
    print("[multi-page txn, partial flush, hard kill before commit]")
    d = fresh_store(base, pages=5, slots=4, budget=4)
    # Dedicated worker process performs the writes then hard-kills itself.
    ok, code, out, err = run([WORKER, d, "partial_flush_kill"],
                             expect_codes=(97,))
    record("partial-flush worker hard-killed (97)", ok, err)
    # Evidence: page 0 durable but pending; page 2 absent.
    ok, c, p0, e = run([PS, "inspect-raw", d, "0"])
    record("flushed page holds uncommitted pending",
           json.loads(p0)["slots"][0]["state"] == 1)
    ok, c, p2, e = run([PS, "inspect-raw", d, "2", "--expect-missing"])
    record("unflushed page absent before recovery",
           json.loads(p2)["exists"] is False)
    # Recovery must undo all three slots (this opens and resolves pages).
    ref = ReferenceModel(5, 4, 0)
    compare_slots("partial-flush recovery undoes everything", d, ref)
    ok, c, p0, e = run([PS, "inspect-raw", d, "0"])
    s0 = json.loads(p0)["slots"][0]
    record("after recovery flushed page is clean initial",
           s0["state"] == 0 and s0["committed"] == 0
           and s0["pending_owner"] == 0 and s0["pending"] == 0,
           str(s0))


def test_scenario_via_worker(base, scenario, fault_point=None,
                             expect_code=0, durable=None):
    print(f"[scenario {scenario} fault={fault_point}]")
    d = fresh_store(base)
    if fault_point:
        FaultInjector(os.path.join(d, "faults")).arm(fault_point)
    expect_codes = (expect_code,)
    ok, code, out, err = run([WORKER, d, scenario],
                             expect_codes=expect_codes)
    record(f"{scenario} worker exits as planned ({code})", ok, err)

    # Build reference expectation from the manifest the worker wrote.
    manifest_path = os.path.join(d, "manifest.json")
    manifest = json.load(open(manifest_path)) if os.path.exists(
        manifest_path) else None
    ref = ReferenceModel(5, 4, 0)
    durable_set = set()
    last_writes = {}
    if manifest:
        for op in manifest["ops"]:
            t = op["txn"]
            kind = op["kind"]
            if kind == "begin":
                ref.begin()
                last_writes.setdefault(t, {})
            elif kind == "set":
                ref.set(t, op["p"], op["s"], op["v"])
                last_writes.setdefault(t, {})[(op["p"], op["s"])] = op["v"]
            elif kind == "add":
                ref.add(t, op["p"], op["s"], op["v"])
                last_writes.setdefault(t, {})[(op["p"], op["s"])] = \
                    ref.read(op["p"], op["s"], t)
            elif kind == "commit":
                ref.commit(t, durable=op.get("durable", True))
                if op.get("durable", True):
                    durable_set.add(t)
            elif kind == "abort":
                ref.abort(t, durable=True)
                durable_set.add(t)
            elif kind in ("commit_attempt", "abort_attempt"):
                # A hard exit happened before the decision became durable:
                # this transaction must NOT take effect.
                pass
        durable_set = set(manifest.get("durable_at_crash", durable_set))
    ref_after = ref.crash(durable_set, final_working=last_writes)

    # Optionally crash once during recovery, then reopen to completion.
    if fault_point and "recovery" not in fault_point:
        FaultInjector(os.path.join(d, "faults")).arm("recovery_mid_cleanup")
        ok2, c2, o2, e2 = run([PS, "read-all", d], expect_codes=(99,))
        record(f"{scenario} mid-recovery crash tolerated", ok2, e2)
    compare_slots(f"{scenario} final", d, ref_after)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    base = tempfile.mkdtemp(prefix="pagestore-test-")
    try:
        test_headline(os.path.join(base, "headline"))
        test_history_prefix_skipped(os.path.join(base, "history"))
        test_scenario_via_worker(
            os.path.join(base, "long"), "long_txn",
            fault_point=None)
        test_scenario_via_worker(
            os.path.join(base, "partial"), "partial_flush",
            # No commit in this scenario: it hard-crashes by design instead.
            fault_point=None)
        # Separately verify the never-flushed page of a pending-only
        # transaction is undone after a hard process kill.
        test_partial_flush_hard_kill(os.path.join(base, "partial_kill"))
        test_scenario_via_worker(
            os.path.join(base, "abort"), "abort_interleave")
        test_scenario_via_worker(
            os.path.join(base, "reply"), "commit_reply_crash",
            fault_point="commit_after_decision_fsync", expect_code=99)
        test_scenario_via_worker(
            os.path.join(base, "predec"), "commit_predecision_crash",
            fault_point="commit_before_decision_fsync", expect_code=99)
        test_scenario_via_worker(
            os.path.join(base, "cpswitch"), "cp_switch_crash",
            fault_point="checkpoint_during_switch", expect_code=99)
    finally:
        if args.keep:
            print(f"artifacts kept at {base}")
        else:
            shutil.rmtree(base, ignore_errors=True)

    failed = [r for r in results if r[1] == FAIL]
    print("\n==== SUMMARY ====")
    for name, status, detail in results:
        print(f"{status:4} {name}")
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
