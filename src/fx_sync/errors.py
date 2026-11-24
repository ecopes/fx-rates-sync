"""Error taxonomy. Callers branch on type, never on message text."""

from __future__ import annotations


class FxSyncError(Exception):
    """Base class for every error raised deliberately by this package."""


class InvalidRangeError(FxSyncError, ValueError):
    """A date range or chunk size is not usable."""


class ApiError(FxSyncError):
    """The upstream API call failed."""


class TransientApiError(ApiError):
    """Retryable: timeouts, connection errors, HTTP 429 and 5xx."""

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class PermanentApiError(ApiError):
    """Not retryable: the request itself is wrong (HTTP 4xx other than 429)."""

    def __init__(self, message: str, *, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class MalformedPayloadError(ApiError):
    """The response body is not parseable JSON."""

    def __init__(self, message: str, *, raw: str) -> None:
        super().__init__(message)
        self.raw = raw


class CircuitOpenError(ApiError):
    """Too many consecutive failures; calls are short-circuited."""


class NotificationError(FxSyncError):
    """Alert delivery failed. The message never contains the webhook URL (it is a secret)."""


class StorageError(FxSyncError):
    """The database failed. Fatal for the current run."""


class MigrationError(StorageError):
    """A migration could not be applied or was modified after being applied."""


class LockHeldError(FxSyncError):
    """Another sync currently holds the lock."""
