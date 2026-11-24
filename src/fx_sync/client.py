"""Async Frankfurter client with retries, Retry-After support and a circuit breaker."""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any, Protocol

import httpx

from fx_sync.errors import (
    ApiError,
    CircuitOpenError,
    MalformedPayloadError,
    PermanentApiError,
    TransientApiError,
)
from fx_sync.models import DateRange
from fx_sync.resilience import CircuitBreaker, RetryPolicy

DEFAULT_BASE_URL = "https://api.frankfurter.dev/v1"


class RateSource(Protocol):
    """Anything that can return the raw range payload; lets the service be tested without HTTP."""

    async def fetch_range(self, base: str, symbols: Sequence[str], window: DateRange) -> object: ...


def parse_retry_after(value: str | None, now: datetime) -> float | None:
    """Parse a Retry-After header (delta-seconds or HTTP-date); ``None`` if absent or invalid."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max((when - now).total_seconds(), 0.0)


def decode_body(text: str) -> object:
    """Decode JSON keeping numbers as exact ``Decimal`` so no float ever touches a rate."""
    try:
        return json.loads(text, parse_float=Decimal)
    except ValueError as exc:
        raise MalformedPayloadError("response is not valid JSON", raw=text[:2000]) from exc


class FrankfurterClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        base_url: str = DEFAULT_BASE_URL,
        retry: RetryPolicy | None = None,
        breaker: CircuitBreaker | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: random.Random | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")
        self._retry = retry or RetryPolicy()
        self._breaker = breaker or CircuitBreaker(threshold=5)
        self._sleep = sleep
        self._rng = rng or random.Random()  # noqa: S311 - jitter, not security
        self._now = now

    async def fetch_range(self, base: str, symbols: Sequence[str], window: DateRange) -> object:
        self._breaker.before_call()
        url = f"{self._base_url}/{window.start.isoformat()}..{window.end.isoformat()}"
        params = {"base": base}
        if symbols:
            params["symbols"] = ",".join(symbols)
        try:
            payload = await self._get_with_retries(url, params)
        except CircuitOpenError:
            raise
        except ApiError:
            self._breaker.record_failure()
            raise
        self._breaker.record_success()
        return payload

    async def _get_with_retries(self, url: str, params: dict[str, str]) -> object:
        attempt = 0
        while True:
            error = await self._attempt(url, params)
            if not isinstance(error, TransientApiError):
                return error
            if attempt >= self._retry.max_retries:
                raise error
            await self._sleep(self._retry.delay(attempt, self._rng, error.retry_after))
            attempt += 1

    async def _attempt(self, url: str, params: dict[str, str]) -> Any:
        """Return the decoded payload, or a ``TransientApiError`` instance if worth retrying."""
        try:
            response = await self._http.get(url, params=params)
        except httpx.TimeoutException as exc:
            return TransientApiError(f"timeout calling {url}: {exc!r}")
        except httpx.TransportError as exc:
            return TransientApiError(f"transport error calling {url}: {exc!r}")
        status = response.status_code
        if status == 429 or status >= 500:
            retry_after = parse_retry_after(response.headers.get("Retry-After"), self._now())
            return TransientApiError(f"HTTP {status} from {url}", retry_after=retry_after)
        if status >= 400:
            raise PermanentApiError(
                f"HTTP {status} from {url}: {response.text[:200]}", status_code=status
            )
        return decode_body(response.text)
