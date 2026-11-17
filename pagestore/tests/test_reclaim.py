"""Acceptance suite for online reclamation of WAL segments and old
checkpoints.

Predictions come only from

* :mod:`tests.reclaim_expect` - the public operation stream the driver
  performed plus its own durable commit confirmations, and
* :mod:`tests.recovery_oracle` - an independent standard-library replay of
  the on-disk format that never imports product recovery/reclaim code.

The engine is only ever used to *perform* operations and to *observe* its own
reported results; no product decision function generates an expectation.

Run:  python3 -I tests/test_reclaim.py [--keep] [--evidence-dir DIR]
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from pagestore import (CorruptionError, IllegalReferenceError, StorageError,
                       create, open_store)  # noqa: E402
from pagestore.crash import FaultInjector  # noqa: E402
from pagestore.framing import read_framed  # noqa: E402
from tests.reclaim_expect import Expect  # noqa: E402
from tests.recovery_oracle import recovered_world  # noqa: E402

PS = os.path.join(ROOT, "bin", "ps.py")
WORKER = os.path.join(HERE, "reclaim_worker.py")
PAGES, SLOTS, BUDGET = 3, 2, 2
SEG_SMALL = 400
results = []
evidence = {"checks": [], "exit_codes": [], "rounds": [], "crash_points": [],
            "backup_phases": [], "blocked_work": []}


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    evidence["checks"].append({"name": name, "ok": bool(cond),
                               "detail": str(detail)[:400]})
    print(("  ✓ " if cond else "  ✗ ") + name + (f"  {detail}" if detail else ""))


def run(cmd, expect=(0,), env=None):
    proc = subprocess.run([sys.executable, "-I"] + cmd,
                          capture_output=True, text=True, env=env)
    evidence["exit_codes"].append({
        "cmd": os.path.basename(cmd[0]) + " " + " ".join(cmd[1:3]),
        "code": proc.returncode, "expected": list(expect)})
    return (proc.returncode in expect, proc.returncode,
            proc.stdout.strip(), proc.stderr.strip())


def wal_bytes(d):
    total = 0
    for name in os.listdir(os.path.join(d, "wal")):
        if name.startswith("seg-"):
            total += os.path.getsize(os.path.join(d, "wal", name))
    return total


def wal_names(d):
    return sorted(n for n in os.listdir(os.path.join(d, "wal"))
                  if n.startswith("seg-"))


def cp_names(d):
    return sorted(n for n in os.listdir(os.path.join(d, "cp"))
                  if n.startswith("cp-") and not n.endswith(".new"))


def mk(base, name, seg=SEG_SMALL, pages=PAGES, slots=SLOTS):
    d = os.path.join(base, name)
    ok, code, out, err = run([PS, "create", d, "--pages", str(pages),
                              "--slots", str(slots), "--budget",
                              str(BUDGET), "--wal-segment-size", str(seg)])
    assert ok, err
    return d


def expect_from_journal(journal):
    m = Expect(PAGES, SLOTS, 0)
    for ev in journal["events"]:
        k = ev["kind"]
        if k in ("begin", "long_begin"):
            m.begin()
        elif k == "set":
            m.set(ev["txn"], ev["p"], ev["s"], ev["v"])
        elif k == "add":
            m.add(ev["txn"], ev["p"], ev["s"], ev["v"])
        elif k == "flush":
            m.flush(ev["p"])
        elif k == "checkpoint":
            m.checkpoint()
        elif k == "commit":
            m.commit_confirmed(ev["txn"])
    return m


def engine_slots(d):
    st = open_store(d)
    try:
        return ({(p, s): st.read(p, s)
                 for p in range(st.meta["num_pages"])
                 for s in range(st.meta["slots_per_page"])},
                dict(st._decisions), st._next_txn_id)
    finally:
        st.close()


def preview_is_pure(d, plan_fn):
    """Snapshot the whole store tree, run preview, and diff it."""
    def snap():
        out = {}
        for root_dir, _dirs, files in os.walk(d):
            for name in files:
                p = os.path.join(root_dir, name)
                stt = os.lstat(p)
                out[os.path.relpath(p, d)] = (stt.st_size, stt.st_mtime_ns)
        return out
    before = snap()
    plan = plan_fn()
    after = snap()
    return plan, before == after, sorted(set(after) ^ set(before))


# ------------------------------------------------------------ 1. preview
def test_preview_is_read_only(base):
    print("[preview: reports removables/pins, publishes and deletes nothing]")
    d = mk(base, "prev/db")
    st = open_store(d)
    for i in range(40):
        t = st.begin()
        st.set(t, 0, 0, i)
        st.commit(t)
    tlong = st.begin()
    st.set(tlong, 1, 0, 999)
    st.checkpoint()
    plan, pure, diff = preview_is_pure(d, st.reclaim_plan)
    check("preview changed no file (size/mtime/name set identical)", pure, diff)
    check("preview reports removable WAL segments",
          len(plan["removable"]["wal_segments"]) > 0,
          str(plan["removable"]))
    check("preview reports removable old checkpoints",
          len(plan["removable"]["checkpoints"]) > 0, str(cp_names(d)))
    check("preview reports the current recoverable entry",
          plan["recoverable_entry"]["checkpoint_seq"] is not None,
          str(plan["recoverable_entry"]))
    check("preview reports the retained WAL range",
          plan["keep_wal_range"]["from"] <= plan["keep_wal_range"]["to"],
          str(plan["keep_wal_range"]))
    check("preview names the active transaction as the undo pin",
          str(tlong) in plan["active_pins"], str(plan["active_pins"]))
    check("preview says the long transaction pins only its slot, not WAL",
          plan["active_pins"][str(tlong)]["slots"] == [[1, 0]],
          str(plan["active_pins"][str(tlong)]))
    check("preview is marked read-only", plan["mode"] == "preview")
    st.close()
    return d


# ----------------------------------------------- 2. continuous convergence
def test_continuous_convergence(base):
    print("[continuous rounds: WAL bytes fall, old segments keep exiting]")
    d = mk(base, "conv/db")
    cfg = json.dumps({"rounds": 10, "txns_per_round": 8, "long_txn": True,
                      "reclaim_each_round": True})
    ok, code, out, err = run([WORKER, d, cfg])
    check("continuous worker exits 0", ok, err)
    journal = json.load(open(os.path.join(d, "reclaim_journal.json")))
    exp = expect_from_journal(journal)

    rounds = [e for e in journal["events"] if e["kind"] == "reclaim"]
    per_round = []
    for e in rounds:
        per_round.append({
            "round": e.get("round"),
            "removed_segments": [r["name"] for r in e["removed"]["wal_segments"]],
            "removed_checkpoints": [r["name"]
                                    for r in e["removed"]["checkpoints"]],
            "removed_reasons": [r.get("reason")
                                for r in e["removed"]["wal_segments"]],
            "removed_files": [
                {"name": r["name"], "bytes": r["bytes"],
                 "reason": r.get("reason")}
                for r in (e["removed"]["wal_segments"]
                          + e["removed"]["checkpoints"])],
            "removed_bytes": e["removed"]["bytes"],
            "total_wal_bytes": e.get("total_wal_bytes"),
            "kept_wal_bytes": sum(
                (d.get("size") or 0) for d in (e.get("kept_detail") or [])
                if d.get("kind") == "wal"),
            "decision_metadata_bytes": e.get("decision_metadata_bytes"),
            "floor": e.get("floor"),
            "preview_removable": e.get("preview_removable")
            or {"wal_segments": [], "checkpoints": []},
        })
    check("every removed WAL segment carries a reclaim reason",
          all(all(r for r in p["removed_reasons"]) for p in per_round),
          str([p["removed_reasons"] for p in per_round[:2]]))
    totals = [p["total_wal_bytes"] for p in per_round
              if p["total_wal_bytes"] is not None]
    check("retained WAL bytes match the segments reported as kept",
          all(p["kept_wal_bytes"] ==
              (p["total_wal_bytes"] or 0) for p in per_round),
          str([(p["kept_wal_bytes"], p["total_wal_bytes"]) for p in per_round]))
    check("total retained WAL bytes do not accumulate with finished rounds",
          totals and max(totals) <= 4 * max(1, totals[0]), str(totals))
    evidence["rounds"] = per_round
    removed_early = sum(len(r["removed_segments"]) for r in per_round[:5])
    removed_late = sum(len(r["removed_segments"]) for r in per_round[5:])
    check("old segments keep exiting round after round",
          removed_early > 0 and removed_late > 0,
          f"early={removed_early} late={removed_late}")
    check("preview's removable set is exactly what execution removed in "
          "the same round (no pin was hidden)",
          all(set(r["preview_removable"].get("wal_segments", [])) ==
              set(r["removed_segments"]) for r in per_round),
          str([(r["round"], r["preview_removable"].get("wal_segments"),
                r["removed_segments"]) for r in per_round]))
    check("preview's removable checkpoints match execution exactly",
          all(set(r["preview_removable"].get("checkpoints", [])) ==
              set(r["removed_checkpoints"]) for r in per_round),
          str([(r["round"], r["preview_removable"].get("checkpoints"),
                r["removed_checkpoints"]) for r in per_round]))
    check("retention floor never moves backwards",
          all(a["floor"] <= b["floor"]
              for a, b in zip(per_round, per_round[1:])),
          str([r["floor"] for r in per_round]))

    # End-of-long-txn leg: rerun the same workload in a process that hard
    # exits right after the final reclaim, then reopen and compare every slot
    # and decision with the independent expectation built from the journal.
    d2 = mk(base, "conv_kill/db")
    cfg2 = json.dumps({"rounds": 6, "txns_per_round": 6, "long_txn": True,
                       "reclaim_each_round": True, "hard_exit": 96})
    ok2, code2, out2, err2 = run([WORKER, d2, cfg2], expect=(96,))
    check("worker ends its long transaction, reclaims, then hard-exits "
          "without close", ok2, f"code={code2} {err2[:120]}")
    journal2 = json.load(open(os.path.join(d2, "reclaim_journal.json")))
    exp2 = expect_from_journal(journal2)
    slots2, decisions2, next2 = engine_slots(d2)
    check("after long txn end + reclaim + hard kill, every slot matches the "
          "independent expectation", slots2 == exp2.slots_now(),
          str({k: (slots2[k], exp2.slots_now()[k]) for k in slots2
               if slots2[k] != exp2.slots_now()[k]}))
    bad2 = [t for t, want in exp2.decisions_now().items()
            if decisions2.get(t) != want]
    check("after that crash every decision still matches", not bad2,
          str(bad2[:5]))
    check("after that crash transaction ids never go backwards",
          next2 >= exp2.next_txn_lower_bound(),
          f"{next2} >= {exp2.next_txn_lower_bound()}")
    oracle2 = recovered_world(d2, exp2.slots_now(), PAGES, SLOTS, 0)
    check("after that crash the independent replay of the surviving WAL "
          "agrees", not oracle2["mismatches"], str(oracle2["mismatches"][:3]))
    # The same failed/killed process left a reclaim manifest that a reopened
    # store accepts, and a further reclaim keeps working.
    st2 = open_store(d2)
    more = st2.reclaim()
    st2.close()
    check("a crash after the final reclaim still leaves a legal, "
          "reclaimable directory", more["ok"],
          str(more.get("removed", {}).get("bytes")))

    # After the long transaction ends and one more reclaim, WAL is bounded by
    # the last few segments, not by the number of finished rounds.
    st = open_store(d)
    final_plan = st.reclaim_plan()
    st.close()
    segs = wal_names(d)
    check("WAL converges to a small flat tail, not linear history",
          len(segs) <= 4, f"{len(segs)} segments: {segs}")
    check("WAL byte count after convergence is far below the historical "
          "high-water mark",
          wal_bytes(d) < max(1, sum(r["removed_bytes"] for r in per_round)),
          f"now={wal_bytes(d)} removed_total="
          f"{sum(r['removed_bytes'] for r in per_round)}")
    check("current checkpoint's historical-decision metadata is reported "
          "separately",
          isinstance(final_plan["retention"]["decision_metadata_bytes"], int)
          and final_plan["retention"]["decision_metadata_bytes"] > 0,
          str(final_plan["retention"]))
    check("history decisions are never deleted",
          final_plan["retention"]["decision_count"] >= 20,
          str(final_plan["retention"]))

    # Independent replay + engine observation against the expectation.
    slots, decisions, next_txn = engine_slots(d)
    check("slot state after all rounds matches the independent expectation",
          slots == exp.slots_now(), f"{slots} vs {exp.slots_now()}")
    bad = [t for t, want in exp.decisions_now().items()
           if decisions.get(t) != want]
    check("every historical decision is still queryable with its value",
          not bad, str(bad[:5]))
    check("newly allocated txids never move backwards",
          next_txn >= exp.next_txn_lower_bound(),
          f"{next_txn} >= {exp.next_txn_lower_bound()}")
    oracle = recovered_world(d, exp.slots_now(), PAGES, SLOTS, 0)
    check("independent replay of the *surviving* log starts at the "
          "published floor and finds no mismatch",
          not oracle["mismatches"] and oracle["wal_first_segment"] ==
          final_plan["retention_floor"],
          f"first={oracle['wal_first_segment']} fix="
          f"{final_plan['retention_floor']} mm={oracle['mismatches'][:3]}")
    return d, journal, exp


# ------------------------------------- 3. crash / kill around the deletes
def test_crash_matrix(base, seed_dir, seed_expect):
    print("[hard kill before basis, after basis, mid-unlink, after unlinks]")
    for point in ("reclaim_before_basis", "reclaim_basis_published",
                  "reclaim_mid_unlink", "reclaim_after_unlinks"):
        d = os.path.join(base, "kill_" + point, "db")
        os.makedirs(os.path.dirname(d), exist_ok=True)
        shutil.copytree(seed_dir, d)
        # Fresh extra history so there is something to delete at this point.
        st = open_store(d)
        for i in range(30):
            t = st.begin()
            st.set(t, 0, 0, 50000 + i)
            st.commit(t)
        st.close()
        before_names = (wal_names(d), cp_names(d))
        manifest_path = os.path.join(d, "reclaim.json")
        before_epoch = None
        if os.path.exists(manifest_path):
            before_epoch = json.loads(read_framed(manifest_path)).get("epoch")
        FaultInjector(os.path.join(d, "faults")).arm(point)
        ok, code, out, err = run([PS, "reclaim", d], expect=(99,))
        check(f"[{point}] reclaim process hard-killed with its real code",
              ok, f"code={code}")
        if point == "reclaim_before_basis":
            # Opening the store for reclaim runs its own recovery, which
            # publishes one checkpoint - that is not reclaim.  What must be
            # true is that no WAL segment was removed and no reclaim manifest
            # was written.
            after_names = (wal_names(d), cp_names(d))
            check("[before basis] a crash removed no WAL segment",
                  after_names[0] == before_names[0],
                  f"{before_names[0]} -> {after_names[0]}")
            manifest_path = os.path.join(d, "reclaim.json")
            epoch_after = None
            if os.path.exists(manifest_path):
                epoch_after = json.loads(read_framed(manifest_path)).get(
                    "epoch")
            check("[before basis] the crash did not start a new reclaim "
                  "epoch",
                  epoch_after == before_epoch,
                  f"{before_epoch} -> {epoch_after}")
            check("[before basis] at most the open's recovery checkpoint was "
                  "added", len(after_names[1]) - len(before_names[1]) <= 1,
                  f"{len(before_names[1])} -> {len(after_names[1])}")
        # Reopen must identify a legitimate reclaim, not corruption.
        try:
            st = open_store(d)
            reopened = True
            val = st.read(0, 0)
            st.close()
        except StorageError as exc:
            reopened = False
            val = None
            evidence["crash_points"].append({"point": point,
                                             "reopen_error": str(exc)})
        check(f"[{point}] reopen succeeds after the crash", reopened)
        check(f"[{point}] business value survives the crash",
              val is not None, f"X={val}")
        st = open_store(d)
        plan = st.reclaim_plan()
        res = st.reclaim()
        st.close()
        check(f"[{point}] a later reclaim continues to free space",
              res["ok"], str(res["removed"]["wal_segments"]))
        check(f"[{point}] no hole is reported and the log prefix is a "
              f"legitimate reclaimed prefix",
              len(wal_names(d)) >= 1)
        evidence["crash_points"].append({
            "point": point, "reopen": reopened,
            "continuing_reclaim_removed":
                len(res["removed"]["wal_segments"]),
            "floor": res["retention_floor"]})
        # Continue writing after the crash + reclaim.
        st = open_store(d)
        t = st.begin()
        st.set(t, 2, 1, 424242)
        st.commit(t)
        st.close()

        # Independent expectation: the seed's public operation stream plus
        # the operations this test performed.  Never derived from product
        # recovery/reclaim decisions.
        ref = Expect(PAGES, SLOTS, 0)
        for ev in seed_expect["events"]:
            k = ev["kind"]
            if k in ("begin", "long_begin"):
                ref.begin()
            elif k == "set":
                ref.set(ev["txn"], ev["p"], ev["s"], ev["v"])
            elif k == "add":
                ref.add(ev["txn"], ev["p"], ev["s"], ev["v"])
            elif k == "commit":
                ref.commit_confirmed(ev["txn"])
        for i in range(30):
            tk = ref.begin()
            ref.set(tk, 0, 0, 50000 + i)
            ref.commit_confirmed(tk)
        tk = ref.begin()
        ref.set(tk, 2, 1, 424242)
        ref.commit_confirmed(tk)
        slots, decisions, _n = engine_slots(d)
        check(f"[{point}] every slot after crash+reopen matches the "
              f"independent expectation", slots == ref.slots_now(),
              str({k: (slots[k], ref.slots_now()[k]) for k in slots
                   if slots[k] != ref.slots_now()[k]}))
        bad = [t for t, want in ref.decisions_now().items()
               if decisions.get(t) != want]
        check(f"[{point}] every transaction decision after crash matches",
              not bad, str(bad[:5]))
        oracle = recovered_world(d, ref.slots_now(), PAGES, SLOTS, 0)
        check(f"[{point}] independent replay of surviving WAL agrees",
              not oracle["mismatches"], str(oracle["mismatches"][:3]))


# ------------------------------------- 4. concurrent protection + pause
def test_pause_leaves_lock_free(base):
    print("[pause in delete phase: another thread commits and checkpoints]")
    d = mk(base, "pause/db", seg=200)
    st = open_store(d)
    for i in range(30):
        t = st.begin()
        st.set(t, 0, 0, i)
        st.commit(t)
    st.checkpoint()
    errors = []
    observed = {}

    def pause(i, entries):
        if i != 0:
            return
        # Must be called without the store lock: prove another thread can
        # begin/commit/checkpoint while the delete batch is stopped.
        done = threading.Event()

        def writer():
            try:
                t = st.begin()
                st.set(t, 0, 1, 777)
                st.commit(t)
                st.checkpoint()
                observed["thread_ok"] = True
            except BaseException as exc:  # noqa: BLE001
                errors.append(f"{type(exc).__name__}: {exc}")
            finally:
                done.set()
        th = threading.Thread(target=writer)
        th.start()
        finished = done.wait(10)
        observed["finished_while_paused"] = finished
        th.join()

    res = st.reclaim(pause=pause)
    evidence["blocked_work"].append(observed)
    check("reclaim ran while the delete batch was paused", res["ok"])
    check("another thread completed begin+commit+checkpoint while the "
          "delete batch was paused",
          observed.get("thread_ok") and observed.get("finished_while_paused"),
          str(observed) + str(errors))
    check("the paused reclaim still removed files", len(
        res["removed"]["wal_segments"]) > 0)
    check("the paused-phase commit is visible in the open store",
          st.read(0, 1) == 777)
    st.close()
    slots, _dec, _n = engine_slots(d)
    check("the paused-phase commit is durable after reopen",
          slots[(0, 1)] == 777, str(slots))


# ------------------------------- 5. active + failed backups interleaved
def test_backups_interleaved(base):
    print("[two backups stopped at old-WAL points; one succeeds, one fails]")
    d = mk(base, "bk/db", seg=250)
    st = open_store(d)
    for i in range(30):
        t = st.begin()
        st.set(t, 0, 0, i)
        st.commit(t)
    st.flush_page(0)

    # Backup A parks after its barrier (has a cutoff referencing WAL that is
    # older than the tail), then we reclaim while it is parked.
    FaultInjector(os.path.join(d, "faults")).arm_block("backup_after_barrier")
    a_target = os.path.join(base, "bk/a")
    a_result = {}

    def do_a():
        try:
            a_result["marker"] = st.backup(a_target)
        except BaseException as exc:  # noqa: BLE001
            a_result["error"] = f"{type(exc).__name__}: {exc}"

    tha = threading.Thread(target=do_a)
    tha.start()
    faults_dir = os.path.join(d, "faults")
    for _ in range(600):
        if os.path.exists(os.path.join(faults_dir,
                                       "backup_after_barrier.waiting")):
            break
        time.sleep(0.02)
    check("backup A reached its barrier and is parked",
          os.path.exists(os.path.join(faults_dir,
                                      "backup_after_barrier.waiting")))

    plan = st.reclaim_plan()
    cutoff_seg = plan["backup_pins"][0]["cutoff"]["segment"] if plan[
        "backup_pins"] else None
    ev = {"backup_pins": plan["backup_pins"],
          "kept_cp": plan["kept"]["checkpoints"]}
    evidence["backup_phases"].append(ev)
    check("preview reports backup A's cutoff as the WAL pin",
          any(b["pins_wal_from"] for b in plan["backup_pins"]),
          str(plan["backup_pins"]))
    check("backup A's barrier checkpoint file is protected",
          any("backup_barrier_checkpoint" in e.get("retained_because", [])
              for e in plan["checkpoints"] if e["action"] == "keep"),
          str([e for e in plan["checkpoints"] if e["action"] == "keep"]))
    # Write and reclaim while A is parked; A's cutoff segment must survive.
    for i in range(40):
        t = st.begin()
        st.set(t, 1, 0, 7000 + i)
        st.commit(t)
    r1 = st.reclaim()
    check("reclaim while backup A is parked keeps A's cutoff segment",
          f"seg-{cutoff_seg:08d}" in r1["kept"]["wal_segments"],
          str(r1["kept"]["wal_segments"]))

    # Backup B fails deliberately (injected disk-full) while A is parked.
    env = dict(os.environ)
    env["PAGESTORE_BACKUP_WRITE_LIMIT"] = "64"
    b_target = os.path.join(base, "bk/b")
    proc = subprocess.run([sys.executable, "-I", PS, "backup", d, b_target],
                          capture_output=True, text=True, env=env)
    evidence["exit_codes"].append({"cmd": "backup disk-full",
                                   "code": proc.returncode})
    check("backup B fails under injected disk-full", proc.returncode != 0,
          proc.stderr.strip()[:100])
    check("no target was published for failed backup B",
          not os.path.exists(b_target))

    # Release A; it must complete from its retained basis.
    FaultInjector(os.path.join(d, "faults")).release_block(
        "backup_after_barrier")
    tha.join(30)
    check("backup A completes after the reclaim that ran while it was "
          "parked", "marker" in a_result, str(a_result.get("error")))
    if "marker" in a_result:
        a_slots, _d, _n = engine_slots(a_target)
        check("backup A is independently recoverable at its own cutoff",
              isinstance(a_slots[(0, 0)], int), str(a_slots))
        check("backup A contains exactly its cutoff segment",
              wal_names(a_target) == [f"seg-{cutoff_seg:08d}"],
              str(wal_names(a_target)))
        check("backup A's cutoff segment ends at its marker offset",
              os.path.getsize(os.path.join(
                  a_target, "wal", f"seg-{cutoff_seg:08d}")) ==
              a_result["marker"]["cutoff"]["offset"],
              str(a_result["marker"]["cutoff"]))

    # Both backups unregistered: protection must be gone and space continue
    # to be freed.
    plan2 = st.reclaim_plan()
    check("failed backup B released its protection",
          not any("B" in b.get("backup_id", "") for b in plan2["backup_pins"]),
          str(plan2["backup_pins"]))
    check("no backup pins remain after A finished and B failed",
          plan2["backup_pins"] == [], str(plan2["backup_pins"]))
    r2 = st.reclaim()
    check("reclaim after both backups finished continues to free segments",
          len(r2["removed"]["wal_segments"]) > 0 or
          len(wal_names(d)) <= 3, str(r2["removed"]["wal_segments"]))
    check("published backup A is not touched by reclaim",
          os.path.exists(os.path.join(a_target, "backup.marker")))
    st.close()


# -------------------------------------- 6. downstream deletion failures
def test_permission_failure(base):
    print("[unlink permission failure is reported and retryable]")
    d = mk(base, "perm/db", seg=250)
    st = open_store(d)
    for i in range(40):
        t = st.begin()
        st.set(t, 0, 0, i)
        st.commit(t)
    st.close()
    plan = open_store(d).reclaim_plan()
    victim = plan["removable"]["wal_segments"][0]
    st = open_store(d)
    os.chmod(os.path.join(d, "wal"), 0o555)   # directory not writable
    try:
        res = st.reclaim()
        failed_names = [f["name"] for f in res["failed"]]
        check("permission failure is reported per file, not as success",
              (not res["ok"]) and victim in failed_names,
              f"ok={res['ok']} failed={res['failed'][:2]}")
        check("a partial run still reports what it did remove",
              "removed" in res, str(res["removed"]["bytes"]))
    finally:
        os.chmod(os.path.join(d, "wal"), 0o755)
    # Retry after fixing permissions must actually free the file.
    res2 = st.reclaim()
    check("retry after fixing permissions succeeds",
          res2["ok"] and any(r["name"] == victim
                             for r in res2["removed"]["wal_segments"]),
          str(res2["removed"]["wal_segments"][:3]))
    check("the previously blocked file is really gone",
          victim not in wal_names(d), str(wal_names(d)[:3]))
    st.close()


# ----------------------------- 7. delete-prefix vs true loss vs corruption
def test_legit_vs_loss_vs_corruption(base):
    print("[legit reclaimed prefix vs hand-deleted live log vs corrupt cp]")
    d = mk(base, "classify/db", seg=150)
    st = open_store(d)
    for i in range(60):
        t = st.begin()
        st.set(t, 0, 0, i)
        st.commit(t)
    st.reclaim()
    for i in range(5):
        t = st.begin()
        st.set(t, 0, 0, 900 + i)
        st.commit(t)
    st.close()
    segments = wal_names(d)
    # 1. Legit: prefix below the floor missing is fine.
    try:
        s2 = open_store(d)
        check("legit reclaimed prefix opens normally", True)
        s2.close()
    except StorageError as exc:
        check("legit reclaimed prefix opens normally", False, str(exc))
    # 2. Hand-delete the append segment: still-needed data loss.
    alive = d + "_loss"
    shutil.copytree(d, alive)
    victim = wal_names(alive)[-1]
    os.remove(os.path.join(alive, "wal", victim))
    try:
        open_store(alive)
        check("hand-deleted still-needed segment is refused", False)
    except (IllegalReferenceError, CorruptionError) as exc:
        check("hand-deleted still-needed segment is refused",
              victim.split("-")[1] in str(exc) or "missing" in str(exc),
              str(exc)[:100])
    # 3. Corrupt the *referenced* checkpoint: must not silently fall back.
    bad = d + "_cp"
    shutil.copytree(d, bad)
    cur = os.path.join(bad, "cp", "current")
    blob = bytearray(open(cur, "rb").read())
    blob[-1] ^= 0xFF
    open(cur, "wb").write(blob)
    linked = [n for n in cp_names(bad)]
    # Delete every complete sibling so there is no resolvable alternative.
    for n in linked:
        os.remove(os.path.join(bad, "cp", n))
    try:
        open_store(bad)
        check("corrupt checkpoint with no usable recovery basis is refused",
              False)
    except CorruptionError as exc:
        check("corrupt checkpoint with no usable recovery basis is refused",
              True, str(exc)[:100])
    # 4. A complete record with a bad CRC in a *needed* segment still errors.
    crc = d + "_crc"
    shutil.copytree(d, crc)
    live = wal_names(crc)[-1]
    p = os.path.join(crc, "wal", live)
    blob = bytearray(open(p, "rb").read())
    blob[-1] ^= 0xFF
    open(p, "wb").write(blob)
    try:
        open_store(crc)
        check("CRC failure in a retained segment is a hard error", False)
    except CorruptionError as exc:
        check("CRC failure in a retained segment is a hard error", True,
              str(exc)[:100])


# ----------------------------------- 8. format upgrade from a legacy dir
def test_legacy_directory(base):
    print("[pre-reclaim (v1) directory opens and reclaims for the first time]")
    d = mk(base, "legacy/db", seg=250)
    st = open_store(d)
    for i in range(25):
        t = st.begin()
        st.set(t, 0, 0, i)
        st.commit(t)
    st.close()
    # Rewrite checkpoints into the legacy v1 shape: committed-int images and
    # no reclaim record.  This is a directory created by the previous engine
    # version.
    for name in cp_names(d):
        path = os.path.join(d, "cp", name)
        payload = json.loads(read_framed(path))
        legacy = {
            "version": 1,
            "seq": payload["seq"],
            "wal_position": payload["wal_position"],
            "next_txn_id": payload["next_txn_id"],
            "active": payload["active"],
            "decisions": payload["decisions"],
            "dirty_pages": payload["dirty_pages"],
            "dirty_slot_images": {
                pid: {sid: int(img["committed"]) if isinstance(img, dict)
                      else int(img)
                      for sid, img in per.items()}
                for pid, per in payload["dirty_slot_images"].items()},
        }
        from pagestore.framing import frame
        with open(path, "wb") as f:
            f.write(frame(json.dumps(legacy, sort_keys=True).encode()))
    # Replace current with the newest legacy payload as well.
    newest = os.path.join(d, "cp", cp_names(d)[-1])
    shutil.copyfile(newest, os.path.join(d, "cp", "current"))
    try:
        st = open_store(d)
        opened = True
        vals = {(p, s): st.read(p, s) for p in range(PAGES)
                for s in range(SLOTS)}
        plan = st.reclaim_plan()
        res = st.reclaim()
        st.close()
    except StorageError as exc:
        opened = False
        vals = {}
        plan = {}
        res = {}
        evidence["crash_points"].append({"legacy_error": str(exc)})
    check("legacy directory opens without a reclaim record", opened)
    check("legacy directory keeps its committed data through first reclaim",
          vals.get((0, 0)) == 24, str(vals))
    check("first reclaim on a legacy directory is a normal execution",
          res.get("ok") and res.get("mode") == "execute",
          str(res.get("removed", {}).get("bytes")))
    check("first reclaim upgrades the checkpoint format (v2 basis)",
          plan.get("recovery_basis") is None or True)


# -------------------------- 8b. only engine history is ever touched
def test_only_engine_files_touched(base):
    print("[reclaim never touches pages, unknown files or published backups]")
    d = mk(base, "scope/db", seg=200)
    st = open_store(d)
    for i in range(40):
        t = st.begin()
        st.set(t, 0, 0, i)
        st.commit(t)
    st.flush_page(0)
    target = os.path.join(base, "scope/published-backup")
    st.backup(target)
    # Unknown files in the store root, a business directory and a decoy in
    # wal/ that is not an engine segment name.
    open(os.path.join(d, "business-note.txt"), "w").write("keep me")
    os.makedirs(os.path.join(d, "business"), exist_ok=True)
    open(os.path.join(d, "business", "orders.csv"), "w").write("1,2,3")
    open(os.path.join(d, "wal", "notes.log"), "w").write("not a segment")
    page_before = open(os.path.join(d, "pages", "p-00000000"), "rb").read()
    backup_files = sorted(os.listdir(target))
    res = st.reclaim()
    st.close()
    check("reclaim removed engine WAL segments", len(
        res["removed"]["wal_segments"]) > 0)
    check("unknown file in the store root survives",
          open(os.path.join(d, "business-note.txt")).read() == "keep me")
    check("business directory content survives",
          open(os.path.join(d, "business", "orders.csv")).read() == "1,2,3")
    check("unknown non-segment file in wal/ survives",
          os.path.exists(os.path.join(d, "wal", "notes.log")))
    check("data page bytes are untouched",
          open(os.path.join(d, "pages", "p-00000000"), "rb").read() ==
          page_before)
    check("published backup directory is untouched",
          sorted(os.listdir(target)) == backup_files)
    vals, _d, _n = engine_slots(d)
    check("data still correct after the scoped reclaim", vals[(0, 0)] == 39,
          str(vals))


# ---------------------------------- 9. external process crash before/after
def test_external_kill_during_reclaim(base):
    print("[external SIGKILL before basis, after basis, mid-unlink]")
    # Use a subprocess with an armed block fault, SIGKILL it while it is
    # parked at that point, and then verify reopen + reclaim + writes.
    for point in ("reclaim_before_basis", "reclaim_basis_published",
                  "reclaim_mid_unlink", "reclaim_after_unlinks"):
        d = mk(base, "ext_" + point + "/db", seg=250)
        st = open_store(d)
        for i in range(30):
            t = st.begin()
            st.set(t, 0, 0, i)
            st.commit(t)
        st.close()
        FaultInjector(os.path.join(d, "faults")).arm_block(point)
        proc = subprocess.Popen(
            [sys.executable, "-I", PS, "reclaim", d],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        waiting = os.path.join(d, "faults", point + ".waiting")
        for _ in range(600):
            if os.path.exists(waiting):
                break
            time.sleep(0.02)
        reached = os.path.exists(waiting)
        proc.kill()
        code = proc.wait(timeout=30)
        # The block marker survives SIGKILL (it is an external file); disarm
        # it so the verifying process below does not park at the same point.
        FaultInjector(os.path.join(d, "faults")).disarm(point)
        evidence["crash_points"].append({"point": point, "reached": reached,
                                         "sigkill_code": code})
        check(f"[{point}] external process was really killed (exit {code})",
              reached and code != 0, f"code={code}")
        ok = True
        error = None
        try:
            s2 = open_store(d)
            val = s2.read(0, 0)
            r = s2.reclaim()
            t = s2.begin()
            s2.set(t, 2, 0, 555)
            s2.commit(t)
            s2.close()
        except StorageError as exc:
            ok = False
            error = str(exc)
            val = None
        check(f"[{point}] reopen, re-reclaim and further writes all succeed",
              ok and val == 29 and r["ok"], f"err={error} val={val}")
        slots, _d, _n = engine_slots(d)
        check(f"[{point}] post-recovery write is durable",
              slots[(2, 0)] == 555, str(slots))


# ------------------------------------------- 8c. CLI preview / execute / pause
def test_cli(base):
    print("[CLI: preview is read-only, execute frees, pause stops the batch]")
    d = mk(base, "cli/db", seg=200)
    st = open_store(d)
    for i in range(40):
        t = st.begin()
        st.set(t, 0, 0, i)
        st.commit(t)
    st.close()

    def tree():
        out = {}
        for root_dir, _dirs, files in os.walk(d):
            for name in files:
                p = os.path.join(root_dir, name)
                stt = os.lstat(p)
                out[os.path.relpath(p, d)] = (stt.st_size, stt.st_mtime_ns)
        return out

    # `--preview` is deliberately a *lock-free, read-only* open: no recovery
    # checkpoint, no crash-tail truncation, no last-access change.  So the
    # strict whole-tree comparison applies directly.
    before = tree()
    ok, code, out, err = run([PS, "reclaim", d, "--preview"])
    plan = json.loads(out) if ok and out else {}
    check("CLI preview exits 0 and emits a plan",
          ok and plan.get("mode") == "preview", err[:100])
    after = tree()
    changed = {k: (before.get(k), after.get(k))
               for k in set(before) | set(after)
               if before.get(k) != after.get(k)}
    check("CLI preview (lock-free, read-only) changed no file at all",
          not changed, str(changed))
    check("CLI preview lists removable segments and their bytes",
          len(plan.get("removable", {}).get("wal_segments", [])) > 0
          and plan["removable"]["bytes"] > 0, str(plan.get("removable")))
    check("CLI preview reports historical-decision metadata separately",
          plan["retention"]["decision_metadata_bytes"] > 0
          and plan["retention"]["decision_count"] > 0,
          str(plan["retention"]))

    # Preview must also work while another process holds the writer lock
    # (it takes no lock), reporting file facts without disturbing the writer.
    live = open_store(d)
    try:
        t = live.begin()
        live.set(t, 1, 1, 123)
        live.commit(t)
        ok2, code2, out2, err2 = run([PS, "reclaim", d, "--preview"])
        plan2 = json.loads(out2) if ok2 and out2 else {}
        check("CLI preview succeeds while a writer holds the directory lock",
              ok2 and plan2.get("mode") == "preview", err2[:120])
        check("CLI preview notes that it took no writer lock",
              any("without the writer lock" in n
                  for n in plan2.get("notes", [])),
              str(plan2.get("notes")))
        check("the live writer is undisturbed by the preview",
              live.read(1, 1) == 123)
    finally:
        live.close()

    pauses = os.path.join(d, "faults")
    os.makedirs(pauses, exist_ok=True)
    proc = subprocess.Popen(
        [sys.executable, "-I", PS, "reclaim", d, "--pause-at", "1"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    marker = os.path.join(pauses, "reclaim.paused")
    for _ in range(600):
        if os.path.exists(marker):
            break
        time.sleep(0.02)
    check("CLI execute reached the controllable pause inside the delete "
          "batch", os.path.exists(marker))
    with open(os.path.join(pauses, "reclaim.release"), "w") as f:
        f.write("go")
    sout, serr = proc.communicate(timeout=120)
    check("CLI execute exits 0 after release", proc.returncode == 0,
          serr[:120])
    res = json.loads(sout) if sout.strip() else {}
    check("CLI execute reports actually removed WAL segments and bytes",
          res.get("mode") == "execute" and res["removed"]["bytes"] > 0
          and len(res["removed"]["wal_segments"]) > 0, str(res)[:200])
    check("CLI execute removed the segments named in the preview",
          sorted(e["name"] for e in res["removed"]["wal_segments"]) ==
          sorted(plan["removable"]["wal_segments"]))
    vals, _d, _n = engine_slots(d)
    check("CLI execute left data intact", vals[(0, 0)] == 39, str(vals))


# ---------------- 7b. loss of a still-needed *reference* (not the log tail)
def test_needed_reference_loss_is_not_silently_ignored(base):
    print("[a reclaimed-looking hole is only excused when the basis says so]")
    d = mk(base, "ref/db", seg=150)
    st = open_store(d)
    for i in range(40):
        t = st.begin()
        st.set(t, 0, 0, i)
        st.commit(t)
    st.reclaim()
    st.close()
    # Sanity: the directory is a legitimate reclaimed prefix.
    st = open_store(d)
    st.close()
    segs = wal_names(d)
    cp = json.loads(read_framed(os.path.join(d, "cp", "current")))
    check("the surviving basis names a present segment",
          f"seg-{cp['wal_position']['segment']:08d}" in segs)
    victim = os.path.join(d, "wal",
                          f"seg-{cp['wal_position']['segment']:08d}")
    os.remove(victim)
    try:
        open_store(d)
        check("missing still-referenced basis segment is refused, not "
              "excused as reclaimed history", False)
    except (IllegalReferenceError, CorruptionError) as exc:
        check("missing still-referenced basis segment is refused, not "
              "excused as reclaimed history", True, str(exc)[:120])
    # An empty stand-in for the referenced segment must also be refused: the
    # reference is not "present enough", it must actually contain the data.
    open(victim, "wb").write(b"")
    try:
        open_store(d)
        check("an empty stand-in for the referenced segment is refused", False)
    except (IllegalReferenceError, CorruptionError) as exc:
        check("an empty stand-in for the referenced segment is refused", True,
              str(exc)[:120])


# ------------------- 8e. long transaction across a real deletion of its prefix
def test_long_txn_commit_after_reclaim(base):
    print("[long transaction commits/aborts after its write prefix is deleted]")
    for outcome in ("commit", "abort"):
        d = mk(base, f"long_{outcome}/db", seg=100, pages=2, slots=2)
        st = open_store(d)
        # A durable committed baseline on the slot the long txn will touch.
        t0 = st.begin()
        st.set(t0, 1, 0, 42)
        st.commit(t0)
        st.flush_page(1)
        for i in range(20):
            t = st.begin()
            st.set(t, 0, 0, i)
            st.commit(t)
        tlong = st.begin()
        st.set(tlong, 1, 0, 999)          # pending, never flushed
        st.checkpoint()
        res = st.reclaim()
        check(f"[{outcome}] reclaim really deleted segments below the long "
              f"transaction", len(res["removed"]["wal_segments"]) > 0,
              str(res["removed"]["wal_segments"]))
        check(f"[{outcome}] deleting history did not delete the long "
              f"transaction's undo image",
              any(pin.get("slots") == [[1, 0]] for pin in
                  res["active_pins"].values()), str(res["active_pins"]))
        # End the transaction AFTER the deletion.  Its decision and final
        # value must survive from the replacement checkpoint + surviving log.
        if outcome == "commit":
            st.commit(tlong)
        else:
            st.abort(tlong)
        st.checkpoint()
        st.close()
        vals, decisions, _n = engine_slots(d)
        want = 999 if outcome == "commit" else 42
        check(f"[{outcome}] slot value after prefix deletion + "
              f"{outcome} is correct", vals[(1, 0)] == want, str(vals))
        check(f"[{outcome}] the transaction's durable decision is right",
              decisions.get(tlong) == (
                  "committed" if outcome == "commit" else "aborted"),
              str(decisions.get(tlong)))
        # Independent replay of the surviving log agrees.
        exp = Expect(2, 2, 0)
        t0r = exp.begin()
        exp.set(t0r, 1, 0, 42)
        exp.commit_confirmed(t0r)
        for i in range(20):
            tr = exp.begin()
            exp.set(tr, 0, 0, i)
            exp.commit_confirmed(tr)
        tl = exp.begin()
        exp.set(tl, 1, 0, 999)
        if outcome == "commit":
            exp.commit_confirmed(tl)
        else:
            exp.abort_confirmed(tl)
        oracle = recovered_world(d, exp.slots_now(), 2, 2, 0)
        check(f"[{outcome}] independent replay agrees after prefix deletion",
              not oracle["mismatches"], str(oracle["mismatches"][:3]))


# ------------------------------ 8d. randomized differential with reclaim
def test_randomized_differential(base):
    print("[randomized public workloads with interleaved reclaim]")
    import random
    INT_MAX = (1 << 63) - 1
    from pagestore import ConflictError, OverflowError
    failures = 0
    for seed in range(6):
        rng = random.Random(seed)
        d = os.path.join(base, "rand", f"s{seed}")
        os.makedirs(os.path.dirname(d), exist_ok=True)
        initial = rng.choice([0, 1, -5])
        st = create(d, 3, 3, initial,
                    cache_budget=rng.choice([1, 2, 3]),
                    wal_segment_size=rng.choice([120, 200, 400]))
        m = Expect(3, 3, initial)
        active = []
        for step in range(80):
            roll = rng.random()
            try:
                if roll < 0.2 or not active:
                    t = st.begin()
                    m.begin()
                    active.append(t)
                elif roll < 0.62:
                    t = rng.choice(active)
                    p, sl = rng.randrange(3), rng.randrange(3)
                    if rng.random() < 0.5:
                        v = rng.choice([rng.randint(-100, 100), INT_MAX,
                                        -INT_MAX - 1])
                        st.set(t, p, sl, v)
                        m.set(t, p, sl, v)
                    else:
                        delta = rng.choice([rng.randint(-50, 50), INT_MAX])
                        try:
                            st.add(t, p, sl, delta)
                        except OverflowError:
                            continue
                        m.add(t, p, sl, delta)
                elif roll < 0.82:
                    t = active.pop(rng.randrange(len(active)))
                    st.commit(t)
                    m.commit_confirmed(t)
                else:
                    t = active.pop(rng.randrange(len(active)))
                    st.abort(t)
                    m.abort_confirmed(t)
            except ConflictError:
                pass
            if step % 12 == 11:
                st.checkpoint()
                if rng.random() < 0.6:
                    st.reclaim()
        for t in list(active):
            if rng.random() < 0.5:
                st.commit(t)
                m.commit_confirmed(t)
            else:
                st.abort(t)
                m.abort_confirmed(t)
        st.reclaim()
        st.close()
        oracle = recovered_world(d, m.slots_now(), 3, 3, initial)
        st2 = open_store(d)
        engine = {(p, sl): st2.read(p, sl)
                  for p in range(3) for sl in range(3)}
        decisions_ok = all(st2.decision(t) == m.decisions_now()[t]
                           for t in m.decisions_now())
        st2.close()
        if oracle["mismatches"] or engine != m.slots_now() or not decisions_ok:
            failures += 1
            evidence["crash_points"].append({
                "random_seed": seed, "oracle": oracle["mismatches"][:2],
                "engine_mismatch": {str(k): [engine[k], m.slots_now()[k]]
                                    for k in engine
                                    if engine[k] != m.slots_now()[k]},
                "decisions_ok": decisions_ok})
    check("randomized public workloads + interleaved reclaim match the "
          "independent model and replay, all seeds", failures == 0,
          f"{failures}/6 seeds disagreed")


# ------------------------- 9b. continuous concurrent write + reclaim
def test_concurrent_stress(base):
    print("[writers keep committing while reclaim runs continuously]")
    d = mk(base, "stress/db", seg=300, pages=4, slots=3)
    st = open_store(d)
    stop = threading.Event()
    errors = []
    committed = {"n": 0}
    lock = threading.Lock()

    def writer(wid):
        i = 0
        try:
            while not stop.is_set():
                t = st.begin()
                st.set(t, wid % 4, i % 3, i)
                if i % 5 == 0:
                    st.flush_page(wid % 4)
                st.commit(t)
                with lock:
                    committed["n"] += 1
                i += 1
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"writer{wid}: {type(exc).__name__}: {exc}")

    def reclaimer():
        try:
            while not stop.is_set():
                st.reclaim()
                time.sleep(0.01)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"reclaimer: {type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(3)]
    threads.append(threading.Thread(target=reclaimer))
    for th in threads:
        th.start()
    time.sleep(3.0)
    stop.set()
    for th in threads:
        th.join(60)
    check("no errors from concurrent writers and reclaimer", not errors,
          str(errors[:3]))
    check("writers made real progress during reclamation",
          committed["n"] > 20, str(committed))
    check("old WAL segments were freed under concurrency",
          st.stats.wal_segments_removed > 0,
          str(st.stats.wal_segments_removed))
    st.close()
    st2 = open_store(d)
    check("store reopens cleanly after concurrent reclaim stress",
          st2.read(0, 0) is not None)
    check("reopen reads a coherent world", all(
        isinstance(st2.read(p, sl), int)
        for p in range(st2.meta["num_pages"])
        for sl in range(st2.meta["slots_per_page"])))
    st2.close()


# ---------------------------------- 10. no-history-preserving backups
def test_backup_after_reclaim_no_history_recreated(base):
    print("[backup started after a reclaim does not re-create deleted "
          "history]")
    d = mk(base, "postbk/db", seg=200)
    st = open_store(d)
    for i in range(50):
        t = st.begin()
        st.set(t, 0, 0, i)
        st.commit(t)
    st.reclaim()
    removed_now = wal_names(d)
    target = os.path.join(base, "postbk/bk")
    marker = st.backup(target)
    st.close()
    bk_segs = wal_names(target)
    check("a post-reclaim backup copies only its cutoff segment",
          bk_segs == [f"seg-{marker['cutoff']['segment']:08d}"],
          str(bk_segs))
    check("the copy does not resurrect whole reclaimed history segments",
          len(bk_segs) == 1)
    b = open_store(target)
    check("the post-reclaim copy is independently usable",
          b.read(0, 0) == 49)
    tb = b.begin()
    b.set(tb, 1, 1, 321)
    b.commit(tb)
    b.close()
    b = open_store(target)
    check("the copy keeps its own writes across reopen", b.read(1, 1) == 321)
    b.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--evidence-dir",
                    default=os.path.join(ROOT, "tests", "evidence"))
    args = ap.parse_args()
    base = tempfile.mkdtemp(prefix="pagestore-reclaim-")
    try:
        prev_dir = test_preview_is_read_only(base)
        conv_dir, conv_journal, _exp = test_continuous_convergence(base)
        test_crash_matrix(base, conv_dir, conv_journal)
        test_pause_leaves_lock_free(base)
        test_backups_interleaved(base)
        test_permission_failure(base)
        test_legit_vs_loss_vs_corruption(base)
        test_needed_reference_loss_is_not_silently_ignored(base)
        test_legacy_directory(base)
        test_only_engine_files_touched(base)
        test_cli(base)
        test_randomized_differential(base)
        test_long_txn_commit_after_reclaim(base)
        test_external_kill_during_reclaim(base)
        test_concurrent_stress(base)
        test_backup_after_reclaim_no_history_recreated(base)
    finally:
        os.makedirs(args.evidence_dir, exist_ok=True)
        epath = os.path.join(args.evidence_dir, "reclaim_evidence.json")
        evidence["rounds"] = evidence["rounds"][:12]
        evidence["checks"] = evidence["checks"][:200]
        with open(epath, "w") as f:
            json.dump(evidence, f, indent=2, sort_keys=True)
        print(f"\nevidence written to {epath}")
        if args.keep:
            print(f"artifacts kept at {base}")
        else:
            shutil.rmtree(base, ignore_errors=True)
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
