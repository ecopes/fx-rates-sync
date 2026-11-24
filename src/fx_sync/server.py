"""Optional read-only HTTP status endpoint (stdlib only)."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fx_sync.repository import Repository
from fx_sync.status import evaluate_health, status_payload


def make_server(
    host: str,
    port: int,
    repo_factory: Callable[[], Repository],
    stale_after: timedelta,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ThreadingHTTPServer:
    """Serve ``GET /health`` (200/503) and ``GET /status``. A repo is opened per request."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path not in ("/health", "/status"):
                self._send(404, {"error": "not found"})
                return
            repo = repo_factory()
            try:
                snapshot = repo.snapshot()
            finally:
                repo.close()
            health = evaluate_health(snapshot, clock(), stale_after)
            if self.path == "/health":
                self._send(
                    200 if health.healthy else 503,
                    {"healthy": health.healthy, "reasons": health.reasons},
                )
            else:
                self._send(200, status_payload(snapshot, health))

        def _send(self, code: int, body: dict[str, object]) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: object) -> None:
            return

    return ThreadingHTTPServer((host, port), Handler)
