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


class BackupError(StorageError):
    """An online backup could not be completed and published.

    The source store is never modified by a failed backup: the partially
    built target stays in an unpublished staging directory that is removed
    before this error is raised, so no valid-looking backup ever exists at
    the requested target path.
    """


# Re-exported so callers only need ``pagestore.errors``.
from .framing import CorruptionError, IncompleteTail  # noqa: E402,F401
