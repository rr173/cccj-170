"""WAL record protocol (version 1).

Every record body starts with a one-byte type.  Integer fields are big
endian; values are signed 64-bit.  The WAL layer in :mod:`pagestore.wal`
wraps each body in a length+CRC frame, so a torn tail and a corrupt but
fully present record are distinguishable.

Record types
------------
TXN_BEGIN (1)   txid
SLOT_WRITE (2)  txid, page_id, slot_id, new_value
                ``new_value`` is the *full* post-image of the slot for this
                transaction, never a delta.  Repeated set/add therefore
                collapses into one final value and replay can never add twice.
COMMIT    (3)   txid        - durable commit decision
ABORT     (4)   txid        - explicit abort decision
REC_ABORT (5)   txid        - abort decision written *by recovery* for a
                transaction that never got a durable decision.  Keeping the
                same type as a user abort means a second recovery treats the
                cleanup as an already-published decision, never as new work.

A page write-back itself needs no WAL of old images: each slot on a page
keeps its own last-committed value alongside the pending value, which is the
undo information (see :mod:`pagestore.pageio`).
"""

TXN_BEGIN = 1
SLOT_WRITE = 2
COMMIT = 3
ABORT = 4
REC_ABORT = 5

ALL_TYPES = frozenset({TXN_BEGIN, SLOT_WRITE, COMMIT, ABORT, REC_ABORT})


def _i64(v: int) -> bytes:
    return v.to_bytes(8, "big", signed=True)


def _u64(v: int) -> bytes:
    return v.to_bytes(8, "big", signed=False)


def _u32(v: int) -> bytes:
    return v.to_bytes(4, "big", signed=False)


def encode(rec_type: int, txid: int, page_id: int = 0, slot_id: int = 0,
           value: int = 0) -> bytes:
    if rec_type == SLOT_WRITE:
        return bytes([rec_type]) + _u64(txid) + _u32(page_id) + _u32(slot_id) + _i64(value)
    if rec_type in (TXN_BEGIN, COMMIT, ABORT, REC_ABORT):
        return bytes([rec_type]) + _u64(txid)
    raise ValueError(f"unknown record type {rec_type}")


def decode(body: bytes):
    """Return a dict describing the record; raise on malformed bodies."""
    if not body:
        raise ValueError("empty record body")
    rec_type = body[0]
    if rec_type not in ALL_TYPES:
        raise ValueError(f"unknown record type {rec_type}")
    if rec_type == SLOT_WRITE:
        if len(body) != 25:
            raise ValueError("bad SLOT_WRITE length")
        return {
            "type": rec_type,
            "txid": int.from_bytes(body[1:9], "big"),
            "page_id": int.from_bytes(body[9:13], "big"),
            "slot_id": int.from_bytes(body[13:17], "big"),
            "value": int.from_bytes(body[17:25], "big", signed=True),
        }
    if len(body) != 9:
        raise ValueError("bad record length")
    return {"type": rec_type, "txid": int.from_bytes(body[1:9], "big")}


def type_name(t: int) -> str:
    return {
        TXN_BEGIN: "BEGIN", SLOT_WRITE: "WRITE", COMMIT: "COMMIT",
        ABORT: "ABORT", REC_ABORT: "REC_ABORT",
    }.get(t, f"TYPE{t}")
