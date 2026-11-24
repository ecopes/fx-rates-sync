from __future__ import annotations

import random
from datetime import UTC, date, datetime

import httpx
import pytest

from fx_sync.client import FrankfurterClient, decode_body, parse_retry_after
from fx_sync.errors import (
    CircuitOpenError,
    MalformedPayloadError,
    PermanentApiError,
    TransientApiError,
)
from fx_sync.models import DateRange
from fx_sync.resilience import CircuitBreaker, RetryPolicy
from tests.conftest import json_response

W = DateRange(date(2025, 3, 10), date(2025, 3, 14))
NOW = datetime(2025, 3, 12, 9, 0, tzinfo=UTC)


def test_retry_policy_backoff_and_retry_after() -> None:
    policy = RetryPolicy(backoff_base=1.0, backoff_max=8.0, retry_after_max=60)
    rng = random.Random(1)
    for attempt, ceiling in [(0, 1.0), (1, 2.0), (2, 4.0), (5, 8.0), (30, 8.0)]:
        assert 0 <= policy.delay(attempt, rng) <= ceiling
    assert policy.delay(0, rng, retry_after=7) == 7
    assert policy.delay(0, rng, retry_after=9999) == 60
    assert policy.delay(0, rng, retry_after=-5) == 0


def test_circuit_breaker() -> None:
    breaker = CircuitBreaker(2)
    breaker.before_call()
    breaker.record_failure()
    assert not breaker.is_open
    breaker.record_failure()
    assert breaker.is_open
    with pytest.raises(CircuitOpenError):
        breaker.before_call()
    breaker.record_success()
    breaker.before_call()
    with pytest.raises(ValueError, match="threshold"):
        CircuitBreaker(0)


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, None), ("", None), ("5", 5.0), (" 12 ", 12.0), ("garbage", None),
     ("Wed, 12 Mar 2025 09:00:30 GMT", 30.0), ("Wed, 12 Mar 2025 08:00:00 GMT", 0.0)],
)  # fmt: skip
def test_parse_retry_after(value: str | None, expected: float | None) -> None:
    assert parse_retry_after(value, NOW) == expected


def test_decode_body_keeps_exact_decimals_and_flags_garbage() -> None:
    body = decode_body('{"rates": {"x": {"EUR": 0.1234567890123}}}')
    assert str(body["rates"]["x"]["EUR"]) == "0.1234567890123"  # type: ignore[index]
    with pytest.raises(MalformedPayloadError) as info:
        decode_body("<html>oops</html>")
    assert info.value.raw.startswith("<html>")


def make_client(
    handler: httpx.MockTransport, sleeps: list[float], **kwargs: object
) -> FrankfurterClient:
    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    http = httpx.AsyncClient(transport=handler)
    return FrankfurterClient(
        http, base_url="https://api.test/v1/", sleep=fake_sleep, rng=random.Random(0),
        now=lambda: NOW, **kwargs,  # type: ignore[arg-type]
    )  # fmt: skip


async def test_success_builds_url_and_params() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return json_response({"base": "USD", "rates": {}})

    client = make_client(httpx.MockTransport(handler), [])
    assert await client.fetch_range("USD", ("EUR", "GBP"), W) == {"base": "USD", "rates": {}}
    assert (
        str(seen[0].url) == "https://api.test/v1/2025-03-10..2025-03-14?base=USD&symbols=EUR%2CGBP"
    )
    await client.fetch_range("USD", (), W)
    assert "symbols" not in seen[1].url.params


async def test_retries_429_honouring_retry_after_then_succeeds() -> None:
    answers = iter([httpx.Response(429, headers={"Retry-After": "3"}), httpx.Response(503),
                    json_response({"base": "USD", "rates": {}})])  # fmt: skip
    sleeps: list[float] = []
    client = make_client(httpx.MockTransport(lambda r: next(answers)), sleeps)
    assert await client.fetch_range("USD", (), W)
    assert sleeps[0] == 3 and len(sleeps) == 2


async def test_gives_up_after_max_retries_and_opens_breaker() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    sleeps: list[float] = []
    client = make_client(httpx.MockTransport(handler), sleeps, retry=RetryPolicy(max_retries=2),
                         breaker=CircuitBreaker(1))  # fmt: skip
    with pytest.raises(TransientApiError):
        await client.fetch_range("USD", (), W)
    assert calls == 3 and len(sleeps) == 2
    with pytest.raises(CircuitOpenError):
        await client.fetch_range("USD", (), W)
    assert calls == 3  # short-circuited, no new request


async def test_4xx_is_permanent_and_not_retried() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404, text="not found")

    client = make_client(httpx.MockTransport(handler), [])
    with pytest.raises(PermanentApiError) as info:
        await client.fetch_range("USD", (), W)
    assert info.value.status_code == 404 and calls == 1


@pytest.mark.parametrize("exc", [httpx.ReadTimeout("slow"), httpx.ConnectError("down")])
async def test_network_errors_are_transient(exc: Exception) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc

    client = make_client(httpx.MockTransport(handler), [], retry=RetryPolicy(max_retries=1))
    with pytest.raises(TransientApiError):
        await client.fetch_range("USD", (), W)


async def test_malformed_json_is_reported_not_retried() -> None:
    client = make_client(httpx.MockTransport(lambda r: httpx.Response(200, text="<html>")), [])
    with pytest.raises(MalformedPayloadError):
        await client.fetch_range("USD", (), W)
