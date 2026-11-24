from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from fx_sync.config import Settings
from fx_sync.errors import NotificationError
from fx_sync.logging_setup import configure_logging
from fx_sync.models import RunCounters, RunStatus
from fx_sync.notifier import Alert, CompositeNotifier, LogNotifier, Severity, WebhookNotifier
from fx_sync.server import make_server
from fx_sync.sqlite_repo import SqliteRepository
from fx_sync.status import evaluate_health, status_payload
from tests.conftest import NOW, RecordingNotifier

ALERT = Alert(Severity.ERROR, "FX sync failed", "api down")


async def test_webhook_posts_slack_compatible_body() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await WebhookNotifier("https://hooks.test/x", http).notify(ALERT)
        await WebhookNotifier("https://hooks.test/x", http).notify(
            Alert(Severity.WARNING, "t", "m")
        )
    first, second = (json.loads(r.content)["text"] for r in seen)
    assert first == ":rotating_light: *FX sync failed*\napi down"
    assert second.startswith(":warning:")


async def test_webhook_http_error_raises_and_composite_isolates_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(500))
    ) as http:
        hook = WebhookNotifier("https://hooks.test/x", http)
        with pytest.raises(NotificationError, match="HTTP 500") as info:
            await hook.notify(ALERT)
        assert "hooks.test" not in str(info.value)
        recorder = RecordingNotifier()
        with caplog.at_level(logging.ERROR, logger="fx_sync.alerts"):
            await CompositeNotifier(hook, recorder).notify(ALERT)
    assert recorder.alerts == [ALERT]  # later notifiers still run
    assert "WebhookNotifier failed" in caplog.text
    assert "hooks.test" not in caplog.text  # the URL is a secret


async def test_log_notifier_levels(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="fx_sync.alerts"):
        await LogNotifier().notify(ALERT)
        await LogNotifier().notify(Alert(Severity.WARNING, "w", "m"))
    assert [r.levelno for r in caplog.records] == [logging.ERROR, logging.WARNING]


def test_settings_parsing_and_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FX_SYMBOLS", "eur, gbp ,jpy")
    monkeypatch.setenv("FX_CHUNK_DAYS", "30")
    settings = Settings()
    assert settings.symbols == ("EUR", "GBP", "JPY") and settings.chunk_days == 30
    assert Settings(symbols=["chf"]).symbols == ("CHF",)
    for bad in ({"symbols": "EU"}, {"symbols": "E1R"}, {"base": "usd"}, {"chunk_days": 0},
                {"concurrency": 99}, {"log_format": "xml"}):  # fmt: skip
        with pytest.raises(ValidationError):
            Settings(**bad)


@pytest.mark.parametrize("fmt", ["text", "json"])
def test_logging_setup_is_idempotent(fmt: str, capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", fmt)
    configure_logging("INFO", fmt)
    logger = logging.getLogger("fx_sync.test")
    logger.info("hello")
    try:
        raise ValueError("x")
    except ValueError:
        logger.exception("with trace")
    lines = [ln for ln in capsys.readouterr().err.splitlines() if ln.startswith(("{", "20"))]
    assert sum("hello" in ln for ln in lines) == 1  # not duplicated by the second call
    if fmt == "json":
        records = [json.loads(ln) for ln in lines]
        assert records[0]["message"] == "hello" and "ValueError" in records[1]["exc"]


def seeded(repo: SqliteRepository, status: RunStatus, finished_ago: timedelta) -> None:
    run = repo.start_run("sync", "USD:EUR", NOW - finished_ago)
    repo.finish_run(run, status, RunCounters(1, 0, 1, 0, 0, 0), NOW - finished_ago, None)


def test_health_rules(repo: SqliteRepository) -> None:
    stale = timedelta(hours=48)
    assert evaluate_health(repo.snapshot(), NOW, stale).reasons == ["no sync has ever run"]
    seeded(repo, RunStatus.SUCCESS, timedelta(hours=1))
    assert evaluate_health(repo.snapshot(), NOW, stale).healthy
    seeded(repo, RunStatus.FAILED, timedelta(minutes=5))
    assert "ended failed" in evaluate_health(repo.snapshot(), NOW, stale).reasons[0]
    assert not evaluate_health(repo.snapshot(), NOW + timedelta(days=5), stale).healthy
    payload = status_payload(repo.snapshot(), evaluate_health(repo.snapshot(), NOW, stale))
    assert payload["last_run"]["status"] == "failed"
    assert payload["last_success"]["finished_at"] is not None


def test_health_only_failures(repo: SqliteRepository) -> None:
    seeded(repo, RunStatus.FAILED, timedelta(minutes=5))
    reasons = evaluate_health(repo.snapshot(), NOW, timedelta(hours=48)).reasons
    assert "no successful run on record" in reasons


@pytest.fixture
def http_server(db_path: Path) -> Iterator[tuple[Path, str]]:
    server = make_server("127.0.0.1", 0, lambda: SqliteRepository(db_path), timedelta(hours=48),
                         clock=lambda: NOW)  # fmt: skip
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield db_path, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


def get(url: str) -> tuple[int, dict[str, object]]:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read())


def test_http_endpoints(http_server: tuple[Path, str]) -> None:
    db_path, base = http_server
    code, body = get(f"{base}/health")
    assert code == 503 and body["healthy"] is False
    repo = SqliteRepository(db_path)
    seeded(repo, RunStatus.SUCCESS, timedelta(hours=1))
    repo.close()
    assert get(f"{base}/health")[0] == 200
    code, body = get(f"{base}/status")
    assert code == 200 and body["runs"] == 1
    assert get(f"{base}/nope")[0] == 404


async def test_transport_failure_message_hides_url() -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("cannot reach https://hooks.test/secret")

    async with httpx.AsyncClient(transport=httpx.MockTransport(boom)) as http:
        with pytest.raises(NotificationError) as info:
            await WebhookNotifier("https://hooks.test/secret", http).notify(ALERT)
    assert str(info.value) == "webhook delivery failed: ConnectError"
    assert info.value.__cause__ is None and info.value.__suppress_context__


def test_httpx_request_logging_is_silenced(caplog: pytest.LogCaptureFixture) -> None:
    configure_logging("DEBUG", "text")
    assert logging.getLogger("httpx").level == logging.WARNING
