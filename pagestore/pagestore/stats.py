"""Bounded disk-I/O statistics used to prove what recovery actually read.

Counters distinguish pages from WAL bytes and applied from skipped records.
Checkpoints publish the WAL offsets they start from, and tests assert the
read-range counters instead of trusting printed claims.
"""

from dataclasses import dataclass, field


@dataclass
class Stats:
    page_reads: int = 0
    page_read_bytes: int = 0
    page_writes: int = 0
    page_write_bytes: int = 0
    wal_bytes_read: int = 0
    wal_records_read: int = 0
    wal_records_applied: int = 0
    wal_records_skipped: int = 0
    wal_bytes_written: int = 0
    wal_fsyncs: int = 0
    dir_fsyncs: int = 0
    checkpoints_published: int = 0
    # Online backup counters: bytes/pages copied into a backup staging area
    # and the number of completed, published backups.
    backup_page_copies: int = 0
    backup_wal_bytes_copied: int = 0
    backup_bytes_verified: int = 0
    backup_retention_copies: int = 0
    backups_published: int = 0
    # Free-form annotations (e.g. chosen checkpoint path and WAL range).
    notes: list = field(default_factory=list)

    def reset(self):
        for name in list(self.__dataclass_fields__):
            if name == "notes":
                setattr(self, name, [])
            else:
                setattr(self, name, 0)

    def as_dict(self):
        d = dict((k, getattr(self, k)) for k in self.__dataclass_fields__)
        return d
