"""Command-line driver for process-level crash/restart experiments.

Each *phase* is a separate process so a fault-induced ``os._exit`` is a real
process crash with no clean close.  Phases exit 0 on success; an armed fault
exits with its configured code (default 99).  Stats are written to a JSON
file next to the store so external tests can verify read ranges and
page/WAL counters rather than trust printed claims.
"""

import argparse
import json
import os
import sys

from . import create, open_store
from .crash import FaultInjector
from .errors import BackupError, CorruptionError, StorageError
from . import backup as backup_mod
from . import reclaim as reclaim_mod


def _arm(store_dir, point, code, mode="exit"):
    FaultInjector(os.path.join(store_dir, "faults")).arm(
        point, int(code), mode=mode)
    print(json.dumps({"armed": point, "code": int(code), "mode": mode}))


def _disarm(store_dir, point):
    FaultInjector(os.path.join(store_dir, "faults")).disarm(point)
    print(json.dumps({"disarmed": point}))


def _release(store_dir, point):
    FaultInjector(os.path.join(store_dir, "faults")).release_block(point)
    print(json.dumps({"released": point}))


def _dump_stats(store, path):
    with open(path, "w") as f:
        json.dump(store.stats.as_dict(), f, indent=2, sort_keys=True)


def _print_slots(store, page_ids, slots_per_page):
    out = {}
    for pid in page_ids:
        out[str(pid)] = [store.read(pid, sid) for sid in range(slots_per_page)]
    print(json.dumps(out, sort_keys=True))


def main(argv=None):
    p = argparse.ArgumentParser(prog="pagestore")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("create")
    sp.add_argument("dir")
    sp.add_argument("--pages", type=int, required=True)
    sp.add_argument("--slots", type=int, required=True)
    sp.add_argument("--initial", type=int, default=0)
    sp.add_argument("--budget", type=int, default=4)
    sp.add_argument("--wal-segment-size", type=int, default=1 << 20)

    arm = sub.add_parser("arm-fault")
    arm.add_argument("dir")
    arm.add_argument("point")
    arm.add_argument("--code", type=int, default=99)
    arm.add_argument("--mode", choices=("exit", "block", "event"),
                     default="exit")

    dis = sub.add_parser("disarm-fault")
    dis.add_argument("dir")
    dis.add_argument("point")

    rel = sub.add_parser("release-fault")
    rel.add_argument("dir")
    rel.add_argument("point")

    ph = sub.add_parser("phase")
    ph.add_argument("dir")
    ph.add_argument("name")
    ph.add_argument("--stats-out")
    ph.add_argument("--hard-exit", action="store_true",
                    help="os._exit at the end without close (crash model)")
    ph.add_argument("--exit-code", type=int, default=98)

    rd = sub.add_parser("read-all")
    rd.add_argument("dir")
    rd.add_argument("--stats-out")

    insp = sub.add_parser("inspect-page")
    insp.add_argument("dir")
    insp.add_argument("page_id", type=int)

    raw = sub.add_parser("inspect-raw")
    raw.add_argument("dir")
    raw.add_argument("page_id", type=int)
    raw.add_argument("--expect-missing", action="store_true")

    dec = sub.add_parser("decisions")
    dec.add_argument("dir")
    dec.add_argument("txids", type=int, nargs="+")
    dec.add_argument("--stats-out")

    bk = sub.add_parser(
        "backup",
        help="export a live, independently openable consistent backup")
    bk.add_argument("dir")
    bk.add_argument("target")
    bk.add_argument("--stats-out")

    mk = sub.add_parser(
        "backup-marker",
        help="print the cutoff marker of a published backup")
    mk.add_argument("target")

    rc = sub.add_parser(
        "reclaim",
        help="free engine-managed history (WAL segments / old checkpoints)")
    rc.add_argument("dir")
    rc.add_argument("--preview", action="store_true",
                    help="read-only: report what could be freed and why the "
                         "rest is pinned; publishes/writes/deletes nothing")
    rc.add_argument("--keep-checkpoints", type=int,
                    default=reclaim_mod.DEFAULT_CP_KEEP,
                    help="number of newest complete checkpoints to keep in "
                         "addition to the basis/current (default: %(default)s)")
    rc.add_argument("--stats-out")
    rc.add_argument("--pause-at", type=int, default=None,
                    help="test/ops seam: stop after this many unlinks and "
                         "wait for SIGCONT/the release file")

    args = p.parse_args(argv)

    if args.cmd == "create":
        st = create(args.dir, args.pages, args.slots, args.initial,
                    args.budget, args.wal_segment_size)
        st.close()
        print(json.dumps({"created": args.dir, "pages": args.pages,
                          "slots": args.slots, "initial": args.initial}))
        return 0

    if args.cmd == "arm-fault":
        _arm(args.dir, args.point, args.code, args.mode)
        return 0
    if args.cmd == "disarm-fault":
        _disarm(args.dir, args.point)
        return 0
    if args.cmd == "release-fault":
        _release(args.dir, args.point)
        return 0

    if args.cmd == "phase":
        return run_phase(args.dir, args.name, args.stats_out,
                         hard_exit=args.hard_exit, exit_code=args.exit_code)
    if args.cmd == "read-all":
        st = open_store(args.dir)
        try:
            _print_slots(st, range(st.meta["num_pages"]),
                         st.meta["slots_per_page"])
            if args.stats_out:
                _dump_stats(st, args.stats_out)
        finally:
            st.close()
        return 0
    if args.cmd == "inspect-page":
        st = open_store(args.dir)
        try:
            print(json.dumps(st.inspect_durable_page(args.page_id),
                             sort_keys=True))
        finally:
            st.close()
        return 0
    if args.cmd == "inspect-raw":
        # No open_store(): parse the raw page file straight from disk so the
        # evidence reflects the crash state, not a recovered one.
        from . import pageio
        from .stats import Stats
        from .framing import CorruptionError
        pages_dir = os.path.join(args.dir, "pages")
        path = os.path.join(pages_dir, pageio.page_filename(args.page_id))
        if not os.path.exists(path):
            if args.expect_missing:
                print(json.dumps({"page": args.page_id, "exists": False}))
                return 0
            print(json.dumps({"page": args.page_id, "exists": False}),
                  file=sys.stderr)
            return 3
        try:
            slots = pageio.inspect_page(pages_dir, args.page_id, Stats())
        except CorruptionError as exc:
            print(json.dumps({"page": args.page_id, "corrupt": str(exc)}),
                  file=sys.stderr)
            return 4
        print(json.dumps({"page": args.page_id, "exists": True,
                          "slots": slots}, sort_keys=True))
        return 0
    if args.cmd == "decisions":
        st = open_store(args.dir)
        try:
            print(json.dumps({str(t): st.decision(t)
                              for t in args.txids}, sort_keys=True))
            if args.stats_out:
                _dump_stats(st, args.stats_out)
        finally:
            st.close()
        return 0
    if args.cmd == "backup":
        st = open_store(args.dir)
        try:
            marker = st.backup(args.target)
            if args.stats_out:
                _dump_stats(st, args.stats_out)
        except BackupError as exc:
            print(json.dumps({"error": "BackupError", "detail": str(exc)}),
                  file=sys.stderr)
            return 3
        finally:
            if not st._closed:
                st.close()
        print(json.dumps({"backup": args.target, "cutoff": marker["cutoff"],
                          "committed_txids": marker["committed_txids"],
                          "next_txn_id": marker["next_txn_id"]},
                         sort_keys=True))
        return 0
    if args.cmd == "reclaim":
        if args.preview and args.pause_at is None:
            return _cli_preview(args)
        try:
            st = open_store(args.dir)
        except CorruptionError as exc:
            print(json.dumps({"error": "CorruptionError",
                              "detail": str(exc)}), file=sys.stderr)
            return 4
        except StorageError as exc:
            print(json.dumps({"error": type(exc).__name__,
                              "detail": str(exc)}), file=sys.stderr)
            return 2
        try:
            if args.preview:
                result = st.reclaim_plan()
                code = 0
            else:
                pause = None
                if args.pause_at is not None:
                    pause = _make_pause(os.path.join(args.dir, "faults"),
                                        args.pause_at)
                result = st.reclaim(keep_checkpoints=args.keep_checkpoints,
                                    pause=pause)
                code = 0 if result.get("ok") else 5
            if args.stats_out:
                _dump_stats(st, args.stats_out)
        finally:
            if not st._closed:
                st.close()
        print(json.dumps(result, sort_keys=True))
        return code
    if args.cmd == "backup-marker":
        try:
            marker = backup_mod.read_marker(args.target)
        except OSError as exc:
            print(json.dumps({"error": type(exc).__name__,
                              "detail": str(exc)}), file=sys.stderr)
            return 3
        print(json.dumps(marker, sort_keys=True))
        return 0
    return 2


def _cli_preview(args):
    """Read-only CLI preview that never opens the store for writing.

    It takes no lock, publishes no checkpoint, writes no file and deletes
    nothing - not even the crash-tail truncation a writer open performs.  The
    plan itself is produced by the same planner the storage API uses.
    """
    from . import reclaim as reclaim_mod, wal as wal_mod
    from .framing import CorruptionError, read_framed
    from .stats import Stats
    store_dir = args.dir
    try:
        meta = json.loads(read_framed(
            os.path.join(store_dir, "meta")).decode("utf-8"))
    except (OSError, CorruptionError, ValueError) as exc:
        print(json.dumps({"error": type(exc).__name__, "detail": str(exc)}),
              file=sys.stderr)
        return 4
    wal = wal_mod.WAL(os.path.join(store_dir, "wal"),
                      meta["wal_segment_size"], Stats())
    wal.open_read_only()
    try:
        result = reclaim_mod.preview_directory(store_dir, wal, Stats())
    finally:
        wal.close()
    print(json.dumps(result, sort_keys=True))
    return 0


def _make_pause(faults_dir, at):
    """Return a pause callback used by ``reclaim --pause-at``.

    After ``at`` unlinks the process writes ``reclaim.paused`` and waits for
    ``reclaim.release`` (or up to ~10 minutes).  Other threads/processes may
    keep committing while the batch is stopped.
    """
    import time
    state = {"n": 0}

    def pause(i, entries):
        state["n"] += 1
        if state["n"] != at + 1:
            return
        os.makedirs(faults_dir, exist_ok=True)
        marker = os.path.join(faults_dir, "reclaim.paused")
        with open(marker, "w") as f:
            f.write(f"{os.getpid()} {i} {len(entries)}\n")
            f.flush()
            os.fsync(f.fileno())
        rel = os.path.join(faults_dir, "reclaim.release")
        for _ in range(6000):
            if os.path.exists(rel):
                break
            time.sleep(0.1)
        else:
            raise TimeoutError("reclaim pause never released")

    return pause


# ----------------------------------------------------------------- phases
def run_phase(store_dir, name, stats_out=None, hard_exit=False,
              exit_code=98):
    """Named workload phases; each opens the store and closes it normally,
    unless an armed fault (or --hard-exit) terminates the process first."""
    st = open_store(store_dir)
    result = PHASES[name](st)
    if stats_out:
        _dump_stats(st, stats_out)
    if result is not None:
        print(json.dumps(result, sort_keys=True), flush=True)
    if hard_exit:
        # Emulate a process crash: no close, no finally flush.
        os._exit(int(exit_code))
    st.close()
    return 0


def phase_main_demo(st):
    """Headline scenario: same-page X/Y, another page Z.

    T1: X += 10 (left active); T2: Y = 20 committed; the shared page is
    flushed carrying BOTH changes; T3: Z += 30 committed without flushing Z;
    checkpoint spans active T1.  Process is killed externally afterwards.
    """
    t1 = st.begin()
    st.add(t1, 0, 0, 10)                 # X
    t2 = st.begin()
    st.set(t2, 0, 1, 20)                 # Y, same page different slot
    st.commit(t2)
    st.flush_page(0)                     # durable page holds T1 pending + T2
    t3 = st.begin()
    st.add(t3, 1, 0, 30)                 # Z on another page
    st.commit(t3)                        # no flush of page 1
    st.checkpoint()                      # active T1 crosses the checkpoint
    return {"t1": t1, "t2": t2, "t3": t3}


def phase_more_history(st):
    """Generate a long prefix of finished, durable transactions."""
    n = 0
    for i in range(200):
        t = st.begin()
        st.set(t, 2, 0, i + 1)
        st.commit(t)
        n += 1
    st.flush_page(2)
    st.checkpoint()
    return {"history_txns": n}


PHASES = {
    "main_demo": phase_main_demo,
    "more_history": phase_more_history,
}


if __name__ == "__main__":
    try:
        sys.exit(main())
    except StorageError as exc:
        print(json.dumps({"error": type(exc).__name__,
                          "detail": str(exc)}), file=sys.stderr)
        sys.exit(2)
