"""On-disk page format and per-slot undo/redo state.

One page file ``pages/p-NNNNNNNN`` is a single framed blob (see
:mod:`pagestore.framing`) containing:

    magic "PSPG" | version u16 | page_id u32 | slot_count u32 | slots...

Every slot occupies 42 bytes::

    state           u8     0 = clean, 1 = pending uncommitted value
    reserved        u8
    pending_owner   i64    txid that owns the pending value (0 = none)
    last_committer  i64    txid that produced ``committed`` (0 = initial)
    committed       i64    value visible to all readers (last committed)
    pending         i64    the owning transaction's working value
    prev            i64    value ``committed`` had when the lock was taken

The three-value layout is what makes the requirements possible:

* A write-back of a page containing an *uncommitted* transaction stores its
  real pending value (the file on disk genuinely contains it), while
  ``committed`` keeps the value every other transaction must read.
* Abort (or recovery cleanup) restores that one slot from ``prev`` without
  touching any other slot, so a page carrying T1's pending slot X and T2's
  committed slot Y can never lose T2 by restoring an old whole-page image.
* Commit makes the slot clean by moving ``pending`` to ``committed``;
  ``last_committer`` makes replay idempotent.

A page publication is temp-file write -> fsync -> atomic replace ->
directory fsync.  Within the stated hardware model (fsync is honored and a
completed replacement never tears) each published page is all-or-nothing.
"""

import os

from .framing import CorruptionError, read_framed, write_framed

PAGE_MAGIC = b"PSPG"
PAGE_VERSION = 1
SLOT_BYTES = 42
STATE_CLEAN = 0
STATE_PENDING = 1


def page_filename(page_id: int) -> str:
    return f"p-{page_id:08d}"


def initial_payload(page_id: int, slot_count: int, initial_value: int) -> bytes:
    out = bytearray()
    out += PAGE_MAGIC
    out += PAGE_VERSION.to_bytes(2, "big")
    out += page_id.to_bytes(4, "big")
    out += slot_count.to_bytes(4, "big")
    for _ in range(slot_count):
        out += _pack_slot(STATE_CLEAN, 0, 0, initial_value, initial_value,
                          initial_value)
    return bytes(out)


def _pack_slot(state, owner, committer, committed, pending, prev) -> bytes:
    return (
        bytes([state, 0])
        + owner.to_bytes(8, "big", signed=True)
        + committer.to_bytes(8, "big", signed=True)
        + committed.to_bytes(8, "big", signed=True)
        + pending.to_bytes(8, "big", signed=True)
        + prev.to_bytes(8, "big", signed=True)
    )


def _unpack_slot(b: bytes):
    return {
        "state": b[0],
        "pending_owner": int.from_bytes(b[2:10], "big", signed=True),
        "last_committer": int.from_bytes(b[10:18], "big", signed=True),
        "committed": int.from_bytes(b[18:26], "big", signed=True),
        "pending": int.from_bytes(b[26:34], "big", signed=True),
        "prev": int.from_bytes(b[34:42], "big", signed=True),
    }


def write_page(pages_dir: str, page_id: int, slot_count: int, slots,
               stats) -> None:
    """Publish a page from an in-memory slot list (dicts as in SlotState)."""
    out = bytearray()
    out += PAGE_MAGIC
    out += PAGE_VERSION.to_bytes(2, "big")
    out += page_id.to_bytes(4, "big")
    out += slot_count.to_bytes(4, "big")
    for s in slots:
        out += _pack_slot(
            s["state"], s["pending_owner"], s["last_committer"],
            s["committed"], s["pending"], s["prev"])
    payload = bytes(out)
    path = os.path.join(pages_dir, page_filename(page_id))
    write_framed(path, payload)
    size = 12 + len(payload)
    stats.page_writes += 1
    stats.page_write_bytes += size
    stats.dir_fsyncs += 1


def read_page(pages_dir: str, page_id: int, stats):
    """Return (slot_count, slots) validating every field of the page."""
    path = os.path.join(pages_dir, page_filename(page_id))
    payload = read_framed(path)  # raises CorruptionError on bad CRC
    size = 12 + len(payload)
    stats.page_reads += 1
    stats.page_read_bytes += size
    if len(payload) < 14 or payload[:4] != PAGE_MAGIC:
        raise CorruptionError("bad page magic", path)
    version = int.from_bytes(payload[4:6], "big")
    if version != PAGE_VERSION:
        raise CorruptionError(f"unsupported page version {version}", path)
    pid = int.from_bytes(payload[6:10], "big")
    if pid != page_id:
        raise CorruptionError("page id mismatch", path)
    slot_count = int.from_bytes(payload[10:14], "big")
    body = payload[14:]
    if len(body) != slot_count * SLOT_BYTES:
        raise CorruptionError("page slot section wrong size", path)
    slots = []
    for i in range(slot_count):
        raw = body[i * SLOT_BYTES:(i + 1) * SLOT_BYTES]
        s = _unpack_slot(raw)
        if s["state"] not in (STATE_CLEAN, STATE_PENDING):
            raise CorruptionError(f"illegal slot state {s['state']}", path)
        if s["state"] == STATE_CLEAN and s["pending_owner"] != 0:
            raise CorruptionError("clean slot carries an owner", path)
        slots.append(s)
    return slot_count, slots


def inspect_page(pages_dir: str, page_id: int, stats):
    """Debug view of the *durable* page content (bypasses the cache)."""
    _, slots = read_page(pages_dir, page_id, stats)
    return slots
