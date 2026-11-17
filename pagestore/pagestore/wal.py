"""Write-ahead log: framed records in rolling segment files.

Segments are named ``wal/seg-00000001`` and contain only length+CRC framed
record bodies.  Scans always start at an explicit (segment, offset)
position published by a checkpoint - recovery never has to read the whole
log prefix.  A torn final frame ends the scan at the last valid prefix; a
complete frame with a bad CRC is hard corruption.
"""

import os
import re

from .framing import CorruptionError, IncompleteTail, frame, parse_frame
from .errors import IllegalReferenceError

SEG_PREFIX = "seg-"
_SEG_RE = re.compile(r"^seg-(\d+)$")


def seg_name(index: int) -> str:
    return f"{SEG_PREFIX}{index:08d}"


def seg_index(name: str) -> int:
    m = _SEG_RE.match(name)
    return int(m.group(1)) if m else -1


class WAL:
    def __init__(self, wal_dir: str, segment_size: int, stats):
        self.dir = wal_dir
        self.segment_size = segment_size
        self.stats = stats
        self._fh = None
        self.segment = 0
        self.offset = 0

    # ------------------------------------------------------------------ open
    def open_for_append(self):
        os.makedirs(self.dir, exist_ok=True)
        existing = sorted(seg_index(n) for n in os.listdir(self.dir)
                          if _SEG_RE.match(n))
        if existing:
            self.segment = existing[-1]
            path = os.path.join(self.dir, seg_name(self.segment))
            # Find the valid prefix length: a crash may leave a torn tail.
            self.offset = self._valid_prefix_len(path)
            if self.offset < os.path.getsize(path):
                # Drop the torn bytes so later appends keep framing intact.
                os.truncate(path, self.offset)
            self._fh = open(path, "ab")
        else:
            self.segment = 1
            self.offset = 0
            self._fh = self._open_segment(self.segment)

    @staticmethod
    def _fsync_dir(path: str) -> None:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _valid_prefix_len(path: str) -> int:
        with open(path, "rb") as f:
            blob = f.read()
        pos = 0
        while pos < len(blob):
            try:
                pos, _ = parse_frame(blob, pos, path)
            except IncompleteTail:
                return pos
        return pos

    def _open_segment(self, index: int):
        path = os.path.join(self.dir, seg_name(index))
        fh = open(path, "ab")
        # New directory entry must be durable before records land in it.
        self._fsync_dir(self.dir)
        return fh

    # ---------------------------------------------------------------- write
    def append(self, body: bytes):
        data = frame(body)
        if self.offset > 0 and self.offset + len(data) > self.segment_size:
            self._rotate()
        assert self._fh is not None
        self._fh.write(data)
        self.offset += len(data)
        self.stats.wal_bytes_written += len(data)

    def _rotate(self):
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self.segment += 1
        self.offset = 0
        path = os.path.join(self.dir, seg_name(self.segment))
        self._fh = open(path, "ab")
        self._fsync_dir(self.dir)

    def fsync(self):
        assert self._fh is not None
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self.stats.wal_fsyncs += 1

    def position(self):
        return {"segment": self.segment, "offset": self.offset}

    def close(self):
        if self._fh is not None:
            try:
                self._fh.close()
            finally:
                self._fh = None

    # ----------------------------------------------------------------- read
    def scan(self, start_segment: int, start_offset: int):
        """Yield decoded frame payloads from an explicit log position.

        Returns a tuple ``(records, end_position)`` where records are
        ``(payload_bytes, segment, frame_end_offset)``.  The end position is
        what the next checkpoint can point at.
        """
        if start_segment < 1:
            raise IllegalReferenceError(
                f"checkpoint references illegal WAL segment {start_segment}")
        on_disk = sorted(seg_index(n) for n in os.listdir(self.dir)
                         if _SEG_RE.match(n))
        wanted = [s for s in on_disk if s >= start_segment]
        if not wanted or wanted[0] != start_segment:
            raise IllegalReferenceError(
                "WAL segment referenced by checkpoint is missing",
                os.path.join(self.dir, seg_name(start_segment)))
        # Contiguity: a hole in the middle means a referenced record is gone.
        for prev, cur in zip(wanted, wanted[1:]):
            if cur != prev + 1:
                raise IllegalReferenceError(
                    "WAL segment sequence has a hole",
                    os.path.join(self.dir, seg_name(prev + 1)))
        records = []
        end_seg, end_off = start_segment, start_offset
        for si in wanted:
            path = os.path.join(self.dir, seg_name(si))
            try:
                with open(path, "rb") as f:
                    if si == start_segment:
                        # Seek to the checkpoint offset so the unrelated
                        # prefix is genuinely never read from disk here.
                        f.seek(start_offset)
                        blob = f.read()
                        base = start_offset
                    else:
                        blob = f.read()
                        base = 0
            except FileNotFoundError:
                raise IllegalReferenceError("WAL segment vanished", path)
            self.stats.wal_bytes_read += len(blob)
            rel = 0
            end_abs = base + len(blob)
            if start_offset > end_abs and si == start_segment:
                raise IllegalReferenceError(
                    "checkpoint offset beyond segment end", path)
            while rel < len(blob):
                try:
                    new_rel, payload = parse_frame(blob, rel, path)
                except IncompleteTail:
                    # Valid prefix ends here; everything later is untrusted.
                    break
                except CorruptionError:
                    raise
                self.stats.wal_records_read += 1
                records.append((payload, si, base + new_rel))
                rel = new_rel
            end_seg, end_off = si, base + rel
        return records, {"segment": end_seg, "offset": end_off}
