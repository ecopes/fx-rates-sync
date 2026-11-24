"""Logging setup: human-readable text or one-JSON-object-per-line for log shippers."""

from __future__ import annotations

import json
import logging
import sys

_MARK = "_fx_sync_handler"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging(level: str = "INFO", fmt: str = "text") -> None:
    logger = logging.getLogger("fx_sync")
    for handler in [h for h in logger.handlers if getattr(h, _MARK, False)]:
        logger.removeHandler(handler)
    handler = logging.StreamHandler(sys.stderr)
    setattr(handler, _MARK, True)
    handler.setFormatter(
        JsonFormatter()
        if fmt == "json"
        else logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(level.upper())
    # httpx logs every request URL at INFO; a webhook URL is a secret, so keep it out of the logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
