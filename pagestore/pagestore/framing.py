"""Binary framing with length + CRC32C-style (zlib crc32) protection.

All durable files (pages, WAL, meta, checkpoints) use framed blobs or
explicitly documented fixed layouts whose header carries a CRC that covers
the whole payload.  A truncated trailing frame is detected as an incomplete
tail; a frame whose CRC does not match is corruption, not an ignorable tail.
"""

import os
import zlib

MAGIC = 0x50534D47  # "PSMG"


class CorruptionError(Exception):
    """Raised when durable content fails validation.

    ``location`` identifies the file (and offset when available) so callers
    can report exactly which reference is bad instead of silently rebuilding.
    """

    def __init__(self, message, location=None):
        if location is not None:
            message = f"{message} (at {location})"
        super().__init__(message)
        self.location = location


class IncompleteTail(Exception):
    """Raised by frame readers when the final frame is not fully on disk."""

    def __init__(self, location=None, valid_bytes=0):
        super().__init__(f"incomplete trailing frame at {location}")
        self.location = location
        self.valid_bytes = valid_bytes


def crc32(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


def frame(payload: bytes) -> bytes:
    """Wrap ``payload`` as MAGIC(4) LEN(4) CRC(4) PAYLOAD."""
    head = MAGIC.to_bytes(4, "big") + len(payload).to_bytes(4, "big")
    crc = crc32(head + payload)
    return head + crc.to_bytes(4, "big") + payload


def write_framed(path: str, payload: bytes, tmp_suffix: str = ".tmp") -> None:
    """Durable one-file publication: temp file, fsync, rename, fsync dir."""
    tmp = path + tmp_suffix
    with open(tmp, "wb") as f:
        f.write(frame(payload))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(os.path.dirname(path) or ".")


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def read_framed(path: str) -> bytes:
    """Read a single framed file, validating magic/length/CRC."""
    with open(path, "rb") as f:
        blob = f.read()
    off, payload = parse_frame(blob, 0, path)
    if off != len(blob):
        raise CorruptionError("trailing bytes after framed record", path)
    return payload


def parse_frame(buf: bytes, offset: int, location: str):
    """Parse one frame at ``offset``; return (new_offset, payload).

    Raises IncompleteTail if fewer than a full frame is present and
    CorruptionError if a full frame fails its checksum.
    """
    if len(buf) - offset < 12:
        raise IncompleteTail(location, len(buf) - offset)
    magic = int.from_bytes(buf[offset:offset + 4], "big")
    length = int.from_bytes(buf[offset + 4:offset + 8], "big")
    crc = int.from_bytes(buf[offset + 8:offset + 12], "big")
    end = offset + 12 + length
    if len(buf) < end:
        raise IncompleteTail(location, len(buf) - offset)
    if magic != MAGIC:
        raise CorruptionError("bad frame magic", f"{location}:{offset}")
    payload = buf[offset + 12:end]
    if crc32(buf[offset:offset + 8] + payload) != crc:
        raise CorruptionError("frame checksum mismatch", f"{location}:{offset}")
    if length > (1 << 30):
        raise CorruptionError("implausible frame length", f"{location}:{offset}")
    return end, payload
