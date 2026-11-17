"""Write-ahead log: framed records in rolling segment files.

Segments are named ``wal/seg-00000001`` and contain only length+CRC framed
record bodies.  Scans always start at an explicit (segment, offset)
position published by a checkpoint - recovery never has to read the whole
log prefix.

**Retention.**  Log segments below a *retention floor* may be physically
removed by the online reclaimer (:mod:`pagestore.reclaim`).  The floor is
never below the append position, so the active segment is always present,
and every remaining segment is re-scanned from its own start when the
append position sits at offset 0 of a later segment.  Filenames are the
only identity a segment has, so a segment that survives always keeps its
original index; indexes never shift.  A missing segment is therefore never
mistaken for a hole in a retained range: the scanner only accepts a range
starting exactly at a checkpoint/reference position and reports anything
else as an illegal reference.

A torn final frame ends the scan at the last valid prefix; a complete frame
with a bad CRC is hard corruption.
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


def on_disk_segments(directory: str):
    """Sorted indexes of segment files currently present."""
    try:
        names = os.listdir(directory)
    except FileNotFoundError:
        return []
    return sorted(i for i in (seg_index(n) for n in names) if i > 0)


class WAL:
    def __init__(self, wal_dir: str, segment_size: int, stats):
        self.dir = wal_dir
        self.segment_size = segment_size
        self.stats = stats
        self._fh = None
        self.segment = 0
        self.offset = 0
        # Lowest segment index that may still be appended to or read as a
        # live log.  The reclaimer advances it; appends never go below it.
        self.retention_floor = 1

    # ------------------------------------------------------------------ open
    def open_for_append(self):
        os.makedirs(self.dir, exist_ok=True)
        existing = on_disk_segments(self.dir)
        if existing:
            self.segment = existing[-1]
            path = os.path.join(self.dir, seg_name(self.segment))
            # Find the valid prefix length: a crash may leave a torn tail.
            self.offset = self._valid_prefix_len(path)
            if self.offset < os.path.getsize(path):
                # Drop the torn bytes so later appends keep framing intact.
                os.truncate(path, self.offset)
                # The truncation itself must be durable before new records
                # land; otherwise a crash could resurrect the torn bytes in
                # front of fresh frames.
                fd = os.open(path, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            self._fh = open(path, "ab")
        else:
            self.segment = 1
            self.offset = 0
            self._fh = self._open_segment(self.segment)

    def open_read_only(self):
        """Locate the append position without writing or truncating.

        Used by the read-only preview path: a torn tail is *reported* via the
        position but never repaired, so previewing a crash-dirty directory
        changes nothing on disk.
        """
        os.makedirs(self.dir, exist_ok=True)
        existing = on_disk_segments(self.dir)
        if existing:
            self.segment = existing[-1]
            path = os.path.join(self.dir, seg_name(self.segment))
            self.offset = self._valid_prefix_len(path)
        else:
            self.segment = 1
            self.offset = 0
        self._fh = None

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

    def on_disk(self):
        """Sorted segment indexes currently present (fresh directory scan).

        Publishing a checkpoint is intentionally cheap and does not call
        this; only retention-sensitive paths (reclaim planning, recovery
        entry validation) rescan the directory.
        """
        return on_disk_segments(self.dir)

    def close(self):
        if self._fh is not None:
            try:
                self._fh.close()
            finally:
                self._fh = None

    # ------------------------------------------------------------- retention
    def set_retention_floor(self, segment: int):
        """Remember the lowest segment a future append may legitimately need.

        Never set above the current append segment: the active segment must
        always stay appendable.
        """
        self.retention_floor = max(1, min(int(segment), self.segment))

    def trim_below(self, segment: int):
        """Physically unlink every segment file with index < ``segment``.

        The caller has already made the *replacement recovery basis* durable
        (a published checkpoint whose reference starts at >= ``segment`` and
        whose required metadata is self-contained).  Returns the removed
        (index, name, size) triples.  A segment that vanishes concurrently is
        not an error - the postcondition is "no such file remains".
        """
        segment = int(segment)
        removed = []
        for idx in on_disk_segments(self.dir):
            if idx >= segment:
                continue
            path = os.path.join(self.dir, seg_name(idx))
            try:
                size = os.path.getsize(path)
            except FileNotFoundError:
                continue
            try:
                os.unlink(path)
            except FileNotFoundError:
                continue
            removed.append((idx, seg_name(idx), size))
            self.stats.wal_segments_removed += 1
            self.stats.wal_segments_removed_bytes += size
        if removed:
            self._fsync_dir(self.dir)
            self.stats.reclaim_unlinks += len(removed)
        return removed

    # ----------------------------------------------------------------- read
    def scan(self, start_segment: int, start_offset: int):
        """Yield decoded frame payloads from an explicit log position.

        Returns ``(records, end_position)`` where records are
        ``(payload_bytes, segment, frame_end_offset)``.

        Contiguity is enforced **above** the start position: a hole after
        the segment the reference names means a referenced record is gone.
        Segments below ``start_segment`` may legitimately have been reclaimed
        and are simply not consulted.
        """
        if start_segment < 1:
            raise IllegalReferenceError(
                f"checkpoint references illegal WAL segment {start_segment}")
        on_disk = on_disk_segments(self.dir)
        if start_segment not in on_disk:
            raise IllegalReferenceError(
                "WAL segment referenced by checkpoint is missing",
                os.path.join(self.dir, seg_name(start_segment)))
        wanted = [s for s in on_disk if s >= start_segment]
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
