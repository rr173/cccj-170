"""A page-based transactional storage engine built only on the stdlib."""

from .errors import (ConflictError, CorruptionError, IllegalReferenceError,
                     IncompleteTail, LockError, OverflowError, StorageClosedError,
                     StorageError)
from .stats import Stats
from .store import Storage, create, open_store

__all__ = [
    "Storage", "create", "open_store", "Stats",
    "StorageError", "ConflictError", "LockError", "StorageClosedError",
    "OverflowError", "CorruptionError", "IncompleteTail",
    "IllegalReferenceError",
]
