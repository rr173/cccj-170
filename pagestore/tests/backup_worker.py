"""Standalone worker process for the headline online-backup scenario.

Runs in its own OS process (launched by test_backup.py) and records a
durable JSON journal of every public operation and every fsync-confirmed
commit.  Block-mode fault markers give the external driver deterministic
seams:

  * the backup thread parks at ``backup_after_barrier`` and
    ``backup_mid_page_copy`` while the main thread runs live transactions,
    page flushes and a checkpoint against the still-open source;
  * the driver releases the blocks to resume the copy.

The journal is the only bridge to the independent model in
:mod:`tests.backup_reference`; this worker never inspects engine internals.
"""

import json
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from pagestore import open_store  # noqa: E402
from pagestore.crash import FaultInjector  # noqa: E402

JOURNAL = "backup_journal.json"


def _journal_path(store_dir):
    return os.path.join(store_dir, JOURNAL)


def write_journal(store_dir, data):
    path = _journal_path(store_dir)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def run(store_dir, target, fault_mode):
    """fault_mode: 'block' (deterministic seams), 'event' (observe only) or
    'none'."""
    st = open_store(store_dir)
    faults = FaultInjector(os.path.join(store_dir, "faults"))
    journal = {"events": [], "durable_commits": [], "durable_aborts": [],
               "backup": None, "stats": None}
    lock = threading.Lock()
    seq = [0]

    def log(kind, **kw):
        with lock:
            seq[0] += 1
            entry = {"seq": seq[0], "kind": kind, **kw}
            journal["events"].append(entry)
            write_journal(store_dir, journal)
        return entry["seq"]

    def arm(point, mode):
        if mode == "block":
            faults.arm_block(point)
        elif mode == "event":
            faults.arm_event(point)

    def wait_reached(point):
        waiting = os.path.join(store_dir, "faults", point + ".waiting")
        event = os.path.join(store_dir, "faults", point + ".event")
        for _ in range(6000):
            if os.path.exists(waiting) or os.path.exists(event):
                return
            time.sleep(0.02)
        raise TimeoutError(f"never reached {point}")

    # ---- Pre-backup durable history on two distinct pages ---------------
    t_pre1 = st.begin()
    log("begin", txn=t_pre1)
    st.set(t_pre1, 0, 0, 100)          # page 0, slot X
    log("set", txn=t_pre1, p=0, s=0, v=100)
    st.commit(t_pre1)                  # durable before backup starts
    journal["durable_commits"].append(t_pre1)
    log("commit", txn=t_pre1, durable=True)

    t_pre2 = st.begin()
    log("begin", txn=t_pre2)
    st.set(t_pre2, 1, 0, 200)          # page 1, slot Z
    log("set", txn=t_pre2, p=1, s=0, v=200)
    st.commit(t_pre2)
    journal["durable_commits"].append(t_pre2)
    log("commit", txn=t_pre2, durable=True)

    # Publish both pages cleanly before the backup so the barrier pins real
    # durable page versions; later replacements must go via retention.
    st.flush_page(0)
    st.flush_page(1)
    log("flush", p=0, note="pre-backup clean publish")
    log("flush", p=1, note="pre-backup clean publish")

    # t_cross starts before the barrier and is still ACTIVE at the cutoff.
    t_cross = st.begin()
    log("begin", txn=t_cross)
    st.set(t_cross, 0, 1, 11)          # page 0, slot Y (uncommitted)
    log("set", txn=t_cross, p=0, s=1, v=11)

    arm("backup_after_barrier", fault_mode)
    arm("backup_mid_page_copy", fault_mode)

    def do_backup():
        start_seq = log("backup_start", target=target)
        marker = st.backup(target)     # parks at the two armed seams
        end_seq = log("backup_end", target=target)
        with lock:
            journal["backup"] = {"marker": marker,
                                 "start_event_seq": start_seq,
                                 "end_event_seq": end_seq}
            write_journal(store_dir, journal)

    bth = threading.Thread(target=do_backup, name="backup")
    bth.start()

    # The barrier is now fixed; everything below happens while page files
    # are being (or about to be) copied.
    wait_reached("backup_after_barrier")
    log("barrier_observed")

    # Flush a page carrying an ACTIVE uncommitted modification during the
    # copy window.  This replaces the pinned barrier version, so the backup
    # must reconstruct page 0 from its retained barrier copy.
    st.flush_page(0)
    log("flush", p=0, note="active pending flushed during copy")

    # t_post begins AND commits after the cutoff: wholly excluded.
    t_post = st.begin()
    log("begin", txn=t_post)
    st.set(t_post, 1, 1, 22)           # page 1, slot W
    log("set", txn=t_post, p=1, s=1, v=22)

    # t_cross's decision lands after the barrier although it began before.
    st.commit(t_cross)
    journal["durable_commits"].append(t_cross)
    log("commit", txn=t_cross, durable=True, note="after cutoff")
    if fault_mode == "block":
        faults.release_block("backup_after_barrier")

    # At the mid-copy seam, drive a checkpoint on the live source while the
    # backup is still copying page files.
    wait_reached("backup_mid_page_copy")
    log("mid_copy_observed")
    st.checkpoint()
    log("checkpoint", note="during page file copy")

    st.commit(t_post)
    journal["durable_commits"].append(t_post)
    log("commit", txn=t_post, durable=True, note="after cutoff")
    if fault_mode == "block":
        faults.release_block("backup_mid_page_copy")

    bth.join()
    log("backup_joined")

    # A post-backup transaction proves the source kept serving writes and
    # its later decisions never leaked into the copy.
    t_after = st.begin()
    log("begin", txn=t_after)
    st.add(t_after, 0, 0, 7)           # 100 -> 107 on the source only
    log("add", txn=t_after, p=0, s=0, delta=7)
    st.commit(t_after)
    journal["durable_commits"].append(t_after)
    log("commit", txn=t_after, durable=True, note="after backup")

    journal["stats"] = st.stats.as_dict()
    write_journal(store_dir, journal)
    st.close()
    print(json.dumps({"worker": "ok",
                      "durable_commits": journal["durable_commits"]},
                     sort_keys=True))


if __name__ == "__main__":
    mode = sys.argv[3] if len(sys.argv) > 3 else "block"
    run(sys.argv[1], sys.argv[2], mode)
