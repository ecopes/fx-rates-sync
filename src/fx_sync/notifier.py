"""Pluggable alert delivery. Notifier failures must never break a sync."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

import httpx

from fx_sync.errors import NotificationError

logger = logging.getLogger("fx_sync.alerts")


class Severity(StrEnum):
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class Alert:
    severity: Severity
    title: str
    message: str


class Notifier(Protocol):
    async def notify(self, alert: Alert) -> None: ...


class LogNotifier:
    """Default notifier: emits the alert as a log record."""

    async def notify(self, alert: Alert) -> None:
        level = logging.ERROR if alert.severity is Severity.ERROR else logging.WARNING
        logger.log(level, "ALERT %s: %s", alert.title, alert.message)


class WebhookNotifier:
    """Posts a Slack-compatible ``{"text": ...}`` body (Mattermost accepts it too)."""

    def __init__(self, url: str, http: httpx.AsyncClient) -> None:
        self._url = url
        self._http = http

    async def notify(self, alert: Alert) -> None:
        icon = ":rotating_light:" if alert.severity is Severity.ERROR else ":warning:"
        body = {"text": f"{icon} *{alert.title}*\n{alert.message}"}
        try:
            response = await self._http.post(self._url, json=body)
        except httpx.HTTPError as exc:
            raise NotificationError(f"webhook delivery failed: {type(exc).__name__}") from None
        if response.status_code >= 400:
            raise NotificationError(f"webhook returned HTTP {response.status_code}")


class CompositeNotifier:
    """Fans out to several notifiers, isolating failures of each."""

    def __init__(self, *notifiers: Notifier) -> None:
        self._notifiers = notifiers

    async def notify(self, alert: Alert) -> None:
        for notifier in self._notifiers:
            try:
                await notifier.notify(alert)
            except Exception:
                logger.exception("notifier %s failed", type(notifier).__name__)
