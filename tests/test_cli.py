from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from fx_sync.cli import EXIT_FAILED, EXIT_LOCKED, EXIT_OK, EXIT_USAGE, main
from fx_sync.models import DateRange
from fx_sync.sqlite_repo import SqliteRepository
from tests.conftest import NOW, make_payload


def api(calls: list[httpx.Request] | None = None, fail: bool = False) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if calls is not None:
            calls.append(request)
        if fail:
            return httpx.Response(400, text="bad request")
        start, end = request.url.path.rsplit("/", 1)[1].split("..")
        window = DateRange(date.fromisoformat(start), date.fromisoformat(end))
        symbols = request.url.params.get("symbols", "EUR").split(",")
        payload = make_payload("USD", window, dict.fromkeys(symbols, "0.9"))
        return httpx.Response(200, json=payload)

    return httpx.MockTransport(handler)


def run(argv: list[str], db: Path, transport: httpx.MockTransport | None = None) -> int:
    return main([*argv, "--db", str(db)] if argv[0] != "--version" else argv,
                transport=transport, clock=lambda: NOW)  # fmt: skip


def test_sync_then_query_then_status(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(["sync", "--symbols", "EUR,GBP", "--chunk-days", "10"], db_path, api()) == EXIT_OK
    out = capsys.readouterr().out
    assert "success: run #1" in out and "inserted=44" in out and "watermark=2025-03-12" in out

    assert run(["query", "rate", "usd", "eur", "--date", "2025-03-15"], db_path) == EXIT_OK
    assert "USD/EUR" in capsys.readouterr().out  # weekend falls back to Friday
    assert (
        run(["query", "rate", "USD", "EUR", "--date", "2025-03-15", "--exact"], db_path)
        == EXIT_FAILED
    )
    assert "no rate found" in capsys.readouterr().err

    assert run(["query", "series", "USD", "GBP", "--from", "2025-03-10", "--to", "today",
                "--limit", "2", "--json"], db_path) == EXIT_OK  # fmt: skip
    series = json.loads(capsys.readouterr().out)
    assert [r["date"] for r in series] == ["2025-03-11", "2025-03-12"]
    assert run(["query", "latest", "--symbols", "EUR", "--json"], db_path) == EXIT_OK
    assert [r["quote"] for r in json.loads(capsys.readouterr().out)] == ["EUR"]

    assert run(["status", "--check"], db_path) == EXIT_OK
    assert "health:      OK" in capsys.readouterr().out
    assert run(["status", "--json"], db_path) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["rates"] == 44


def test_second_sync_is_a_noop_on_data(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    run(["sync", "--symbols", "EUR"], db_path, api())
    capsys.readouterr()
    run(["sync", "--symbols", "EUR"], db_path, api())
    assert "inserted=0 updated=0 unchanged=3" in capsys.readouterr().out


def test_backfill(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = run(
        ["backfill", "--from", "2025-03-03", "--to", "2025-03-07", "--symbols", "EUR"],
        db_path,
        api(),
    )
    assert code == EXIT_OK and "inserted=5" in capsys.readouterr().out


def test_failed_api_gives_nonzero_exit_and_unhealthy_status(
    db_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(["sync", "--symbols", "EUR"], db_path, api(fail=True)) == EXIT_FAILED
    captured = capsys.readouterr()
    assert "failed: run #1" in captured.out and "HTTP 400" in captured.err
    assert run(["status", "--check"], db_path) == EXIT_FAILED


def test_lock_held_exit_code(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo = SqliteRepository(db_path)
    repo.acquire_lock("fx-sync", "other", 900, NOW)
    repo.close()
    assert run(["sync", "--symbols", "EUR"], db_path, api()) == EXIT_LOCKED
    assert "already running" in capsys.readouterr().err


def test_webhook_is_called_on_failure(db_path: Path) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(500) if "api.frankfurter" in str(request.url) else httpx.Response(200)

    transport = httpx.MockTransport(handler)
    argv = ["sync", "--symbols", "EUR", "--webhook-url", "https://hooks.test/in"]
    assert (
        main([*argv, "--db", str(db_path)], transport=transport, clock=lambda: NOW) == EXIT_FAILED
    )
    posts = [c for c in calls if c.method == "POST"]
    assert len(posts) == 1 and "FX sync failed" in json.loads(posts[0].content)["text"]


@pytest.mark.parametrize(
    "argv",
    [
        ["sync", "--symbols", "EU"],
        ["sync", "--base", "usd"],
        ["backfill", "--from", "2025-03-09", "--to", "2025-03-01"],
        ["sync", "--since", "not-a-date"],
    ],
)
def test_usage_errors(argv: list[str], db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(argv, db_path, api()) == EXIT_USAGE
    assert capsys.readouterr().err.startswith(("error:", "configuration error:"))


def test_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        main(["--version"])
    assert info.value.code == 0 and capsys.readouterr().out.startswith("fx-sync ")
