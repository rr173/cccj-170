"""Checkpoint publication, selection and reclamation.

A checkpoint is a framed JSON blob.  It does **not** require active
transactions to finish or dirty pages to be flushed: it persists

* the WAL position everything after which must still be scanned;
* every active transaction's txid and the slots it has locked (the
  information needed to undo it) **plus the undo (``prev``) value of every
  slot it locked**, so a reclaimed prefix can never make an old active
  transaction unrecoverable;
* every durable commit/abort decision known so far;
* the set of pages that currently carry uncommitted state (dirty-pages
  recovery start list) and, self-contained, the committed/undo image of
  every slot on them;
* the next txid to allocate;
* a ``reclaim`` record: a monotonically increasing epoch and the retention
  floor (lowest WAL segment that may still be needed).  Its presence is what
  lets a later open prove a missing prefix was removed by this engine rather
  than lost.

Publication protocol, crash-safe in every phase:

1. write ``cp/cp-<seq>.new`` and fsync it;
2. rename to ``cp/cp-<seq>`` and fsync the directory (complete checkpoint);
   optionally touch an external fsync witness **here** - after the complete
   checkpoint exists but before ``current`` names it;
3. atomically replace ``cp/current`` and fsync the directory.

A crash in phase 1/2 leaves only a ``.new``/missing file that is never
selected.  A crash in phase 3 leaves the previous ``current`` intact, so the
reopened store always chooses one complete, valid starting point.

Selection respects reclamation:

* ``current`` is preferred when it is complete and its WAL reference is
  satisfiable;
* otherwise the newest complete ``cp-N`` is chosen, but a checkpoint that
  depends on WAL segments already deleted (or on a segment range with a real
  hole) is *not* silently used;
* if every usable checkpoint is gone, selection returns ``None``; if there
  is no checkpoint at all the caller reports corruption.  A present-but-
  corrupt ``current`` with no valid sibling is a hard error.
"""

import json
import os

from .framing import CorruptionError, read_framed, write_framed

CURRENT = "current"
SUFFIX_NEW = ".new"
CP_PREFIX = "cp-"


def cp_filename(seq: int) -> str:
    return f"{CP_PREFIX}{seq:010d}"


def cp_seq_of(name: str):
    if not name.startswith(CP_PREFIX) or name.endswith(SUFFIX_NEW):
        return None
    try:
        return int(name[len(CP_PREFIX):])
    except ValueError:
        return None


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
            faults=None, witness=None) -> str:
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
    if witness is not None:
        # External durability witness for crash tests: by the time this
        # returns, a *complete* checkpoint with this WAL position exists
        # under its final name, while ``current`` may still point at an
        # older one.  Written only when a test/operator supplies a path.
        write_framed(witness, json.dumps(
            {"seq": int(seq), "wal_position": payload_dict["wal_position"]}
        ).encode("utf-8"))
    # Crash here leaves the new complete checkpoint unpublished and the
    # previous ``current`` fully valid.
    if faults is not None:
        faults.reach("checkpoint_during_switch")
    # Phase 3: repoint current atomically.
    write_framed(os.path.join(cp_dir, CURRENT), body)
    stats.checkpoints_published += 1
    return final_path


def list_complete(cp_dir: str):
    """``[(seq, path)]`` for every complete (non-``.new``) checkpoint file."""
    out = []
    try:
        names = os.listdir(cp_dir)
    except FileNotFoundError:
        return out
    for name in names:
        seq = cp_seq_of(name)
        if seq is not None:
            out.append((seq, os.path.join(cp_dir, name)))
    out.sort()
    return out


def wal_reference_ok(wal, position) -> bool:
    """True when every segment the position depends on still exists whole."""
    try:
        seg = int(position["segment"])
        off = int(position["offset"])
    except (KeyError, TypeError, ValueError):
        return False
    on_disk = wal.on_disk()
    if seg not in on_disk:
        return False
    wanted = [s for s in on_disk if s >= seg]
    for prev, cur in zip(wanted, wanted[1:]):
        if cur != prev + 1:
            return False
    try:
        size = os.path.getsize(os.path.join(wal.dir, _seg_name(seg)))
    except OSError:
        return False
    return 0 <= off <= size


def _seg_name(index):
    return f"seg-{index:08d}"


def select_latest(cp_dir: str, stats, wal=None):
    """Return ``(payload, path)`` for the newest usable checkpoint or None.

    The **newest complete checkpoint is authoritative**.  With ``wal`` given,
    it must have a satisfiable WAL reference: a newest checkpoint naming a
    segment that is gone is data loss, never a fallback opportunity, so it
    raises (:class:`~pagestore.errors.IllegalReferenceError`).  Older
    complete checkpoints may legitimately reference segments removed by
    online reclamation; those are skipped, and selection never "goes back"
    past the newest one.

    Without ``wal`` only completeness/CRC is checked (legacy callers).
    """
    if not os.path.isdir(cp_dir):
        return None
    current_path = os.path.join(cp_dir, CURRENT)
    current = None
    if os.path.exists(current_path):
        try:
            current = decode(read_framed(current_path))
        except CorruptionError:
            stats.notes.append("current checkpoint unreadable; scanning cp dir")
    complete = list_complete(cp_dir)
    if not complete:
        if os.path.exists(current_path):
            raise CorruptionError(
                "checkpoint current is corrupt and no complete checkpoint "
                "exists", current_path)
        return None
    # ``current`` is the published pointer (phase 3 of the protocol) and is
    # preferred when complete and its log reference is satisfiable.  A crash
    # between promoting cp-N and switching ``current`` can legitimately
    # leave ``current`` one generation behind; falling back to a still-valid
    # sibling is then safe because nothing was reclaimed below it.  What is
    # never safe is falling *past* an unusable newest checkpoint into older
    # history whose log prefix may already be gone.
    if current is not None:
        if wal is None or wal_reference_ok(wal, current["wal_position"]):
            return current, current_path
        _require_no_fallback_past(current, cp_dir, current_path)
    newest_seq, newest_path = complete[-1]
    try:
        newest = decode(read_framed(newest_path))
    except CorruptionError as exc:
        raise CorruptionError(
            f"newest checkpoint is corrupt: {exc}", newest_path)
    if wal is None or wal_reference_ok(wal, newest["wal_position"]):
        return newest, newest_path
    # Newest checkpoint's own log reference cannot be followed.  Reclamation
    # only ever deletes segments strictly below the published floor of a
    # checkpoint that is itself newer, so this is loss, not history.
    _require_no_fallback_past(newest, cp_dir, newest_path)


def _require_no_fallback_past(entry, cp_dir, path):
    """Refuse rather than fall back into older, possibly reclaimed history."""
    from .errors import IllegalReferenceError
    pos = entry["wal_position"]
    raise IllegalReferenceError(
        "checkpoint references WAL that is missing or too short "
        f"(seg-{int(pos['segment']):08d}:{int(pos['offset'])}); a retained "
        "recovery basis is unavailable and the store will not silently fall "
        "back into older, reclaimed history",
        os.path.join(cp_dir, os.path.basename(path)))


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
