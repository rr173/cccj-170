"""Checkpoint publication and selection.

A checkpoint is a framed JSON blob.  It does **not** require active
transactions to finish or dirty pages to be flushed: it persists

* the WAL position everything after which must still be scanned;
* every active transaction's txid and the slots it has locked (the
  information needed to undo it);
* every durable commit/abort decision known so far;
* the set of pages that currently carry uncommitted state (dirty-pages
  recovery start list);
* the next txid to allocate.

Publication protocol, crash-safe in every phase:

1. write ``cp/cp-<seq>.new`` and fsync it;
2. rename to ``cp/cp-<seq>`` and fsync the directory (complete checkpoint);
3. atomically replace ``cp/current`` and fsync the directory.

A crash in phase 1/2 leaves only a ``.new``/missing file that is never
selected.  A crash in phase 3 leaves the previous ``current`` intact, so the
reopened store always chooses one complete, valid starting point.
"""

import json
import os

from .framing import CorruptionError, read_framed, write_framed

CURRENT = "current"
SUFFIX_NEW = ".new"


def cp_filename(seq: int) -> str:
    return f"cp-{seq:010d}"


def encode(payload_dict: dict) -> bytes:
    return json.dumps(payload_dict, sort_keys=True).encode("utf-8")


def decode(body: bytes) -> dict:
    try:
        d = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CorruptionError(f"checkpoint JSON invalid: {exc}")
    required = {"version", "wal_position", "next_txn_id", "active",
                "decisions", "dirty_pages", "seq", "dirty_slot_images"}
    missing = required - set(d)
    if missing:
        raise CorruptionError(f"checkpoint missing fields {sorted(missing)}")
    pos = d["wal_position"]
    if not isinstance(pos.get("segment"), int) or not isinstance(
            pos.get("offset"), int) or pos["segment"] < 1 or pos["offset"] < 0:
        raise CorruptionError("checkpoint carries illegal WAL position")
    if not isinstance(d["active"], dict) or not isinstance(
            d["decisions"], dict) or not isinstance(d["dirty_pages"], list):
        raise CorruptionError("checkpoint field type mismatch")
    return d


def publish(cp_dir: str, seq: int, payload_dict: dict, stats,
            faults=None) -> str:
    os.makedirs(cp_dir, exist_ok=True)
    final_path = os.path.join(cp_dir, cp_filename(seq))
    tmp_path = final_path + SUFFIX_NEW
    body = encode(payload_dict)
    # Phase 1: staged temp file, fully fsynced.
    from .framing import frame  # local import keeps framing API explicit
    with open(tmp_path, "wb") as f:
        f.write(frame(body))
        f.flush()
        os.fsync(f.fileno())
    # Phase 2: promote to its final name; only now is it a candidate.
    os.replace(tmp_path, final_path)
    _fsync_dir(cp_dir)
    # Crash here leaves the new complete checkpoint unpublished and the
    # previous ``current`` fully valid.
    if faults is not None:
        faults.reach("checkpoint_during_switch")
    # Phase 3: repoint current atomically.
    write_framed(os.path.join(cp_dir, CURRENT), body)
    stats.checkpoints_published += 1
    return final_path


def select_latest(cp_dir: str, stats):
    """Return the newest *complete and valid* checkpoint, or None.

    ``current`` is preferred; if it is missing or corrupt we fall back to
    the highest-numbered complete ``cp-NNN`` file.  A present-but-corrupt
    ``current`` pointing at a valid target never silently loses data: the
    fallback re-selects the same complete checkpoint.  An unreadable current
    with no valid sibling is a hard error (the reference may be illegal).
    """
    if not os.path.isdir(cp_dir):
        return None
    current_path = os.path.join(cp_dir, CURRENT)
    if os.path.exists(current_path):
        try:
            return decode(read_framed(current_path)), current_path
        except CorruptionError:
            stats.notes.append("current checkpoint unreadable; scanning cp dir")
    candidates = []
    for name in os.listdir(cp_dir):
        if name == CURRENT or name.endswith(SUFFIX_NEW):
            continue
        if name.startswith("cp-"):
            try:
                seq = int(name[3:])
            except ValueError:
                continue
            candidates.append((seq, name))
    for seq, name in sorted(candidates, reverse=True):
        path = os.path.join(cp_dir, name)
        try:
            d = decode(read_framed(path))
            return d, path
        except CorruptionError:
            continue
    if os.path.exists(current_path):
        raise CorruptionError(
            "checkpoint current is corrupt and no valid sibling exists",
            current_path)
    return None


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
