"""Public exception hierarchy for the storage engine."""


class StorageError(Exception):
    """Base class for all storage-engine errors."""


class ConflictError(StorageError):
    """A slot is locked by another active transaction.

    Waiting is deliberately not implemented: callers get an explicit refusal
    and may abort/retry themselves.
    """


class LockError(StorageError):
    """Another process already holds the directory's writer lock."""


class StorageClosedError(StorageError):
    """Operation attempted after close / before open completed."""


class OverflowError(StorageError):
    """An add would leave the signed 64-bit range.

    The operation is rejected before anything is written, so no partial
    mutation remains.
    """


class IllegalReferenceError(StorageError):
    """A durable pointer (checkpoint/WAL segment) cannot be followed."""


# Re-exported so callers only need ``pagestore.errors``.
from .framing import CorruptionError, IncompleteTail  # noqa: E402,F401
