"""Process worker for reclaim acceptance scenarios.

Every public operation is journaled *before* the next one, so the driver can
build an independent expectation (:mod:`tests.reclaim_expect`) from the
operation stream and the driver's own durable-commit confirmations.
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from pagestore import open_store  # noqa: E402

JOURNAL = "reclaim_journal.json"


def write_journal(store_dir, data):
    path = os.path.join(store_dir, JOURNAL)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def run(store_dir, rounds, txns_per_round, long_txn, reclaim_each_round,
        hard_exit_code=None):
    st = open_store(store_dir)
    journal = {"events": [], "next_txn": 1, "reclaim": []}

    def log(kind, **kw):
        journal["events"].append({"kind": kind, **kw})
        write_journal(store_dir, journal)

    long_id = None
    if long_txn:
        long_id = st.begin()
        log("long_begin", txn=long_id)
        st.set(long_id, 0, 1, 1000)
        log("set", txn=long_id, p=0, s=1, v=1000)
        journal["long_txn"] = long_id

    for r in range(rounds):
        # A page whose committed value is intentionally never flushed before
        # the checkpoint ("committed but not yet written back"), and a page
        # carrying an uncommitted value that *is* written back.
        t = st.begin()
        log("begin", txn=t)
        st.set(t, 1, 0, 10 + r)
        log("set", txn=t, p=1, s=0, v=10 + r)
        st.commit(t)                   # committed, page 1 never flushed
        log("commit", txn=t, durable=True, note="unflushed commit")
        t2 = st.begin()
        log("begin", txn=t2)
        v = st.add(t2, 2, 0, 1)
        log("add", txn=t2, p=2, s=0, v=1, after=v)
        st.flush_page(2)              # pending value really on disk
        log("flush", p=2)
        st.commit(t2)
        log("commit", txn=t2, durable=True)
        for i in range(txns_per_round):
            tk = st.begin()
            log("begin", txn=tk)
            pid = i % 3
            sid = i % 2
            if long_id is not None and pid == 0 and sid == 1:
                sid = 0   # long transaction holds page0/slot1
            st.set(tk, pid, sid, r * 1000 + i)
            log("set", txn=tk, p=pid, s=sid, v=r * 1000 + i)
            st.commit(tk)
            log("commit", txn=tk, durable=True)
        st.checkpoint()
        log("checkpoint", round=r)
        if reclaim_each_round:
            before = st.reclaim_plan()
            res = st.reclaim()
            log("reclaim", round=r, removed=res["removed"],
                floor=res["retention_floor"], epoch=res["epoch"],
                preview_removable=before["removable"],
                total_wal_bytes=res["total_wal_bytes"],
                kept_wal=res["kept"]["wal_segments"],
                kept_detail=res["kept_detail"],
                decision_metadata_bytes=res["retention"][
                    "decision_metadata_bytes"])

    if long_id is not None:
        st.commit(long_id)
        log("commit", txn=long_id, durable=True, note="long txn ended")
        st.checkpoint()
        log("checkpoint", note="after long txn")
        before = st.reclaim_plan()
        res = st.reclaim()
        log("reclaim", note="after long txn", removed=res["removed"],
            preview_removable=before["removable"],
            floor=res["retention_floor"],
            total_wal_bytes=res["total_wal_bytes"],
            kept_wal=res["kept"]["wal_segments"],
            kept_detail=res["kept_detail"],
            decision_metadata_bytes=res["retention"][
                "decision_metadata_bytes"])

    journal["next_txn"] = st._next_txn_id
    journal["stats"] = st.stats.as_dict()
    write_journal(store_dir, journal)
    if hard_exit_code is not None:
        os._exit(int(hard_exit_code))
    st.close()
    print(json.dumps({"worker": "ok", "long_txn": long_id,
                      "next_txn": journal["next_txn"]}, sort_keys=True))


if __name__ == "__main__":
    cfg = json.loads(sys.argv[2])
    run(sys.argv[1], cfg.get("rounds", 3), cfg.get("txns_per_round", 5),
        cfg.get("long_txn", False), cfg.get("reclaim_each_round", True),
        cfg.get("hard_exit"))
