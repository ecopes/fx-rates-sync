"""Operational health derived from the run log and watermark."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from fx_sync.models import RunRecord, RunStatus, StatusSnapshot


@dataclass(frozen=True)
class Health:
    healthy: bool
    reasons: list[str]


def evaluate_health(snapshot: StatusSnapshot, now: datetime, stale_after: timedelta) -> Health:
    reasons: list[str] = []
    last, ok = snapshot.last_run, snapshot.last_success
    if last is None:
        reasons.append("no sync has ever run")
    else:
        if last.status in (RunStatus.FAILED, RunStatus.PARTIAL):
            reasons.append(f"last run #{last.id} ended {last.status}")
        if ok is None:
            reasons.append("no successful run on record")
    if ok is not None and ok.finished_at is not None and now - ok.finished_at > stale_after:
        age_h = (now - ok.finished_at).total_seconds() / 3600
        reasons.append(f"last success is stale ({age_h:.1f}h old, limit {stale_after})")
    return Health(not reasons, reasons)


def _run_dict(run: RunRecord | None) -> dict[str, Any] | None:
    if run is None:
        return None
    return {
        "id": run.id,
        "mode": run.mode,
        "series": run.series,
        "status": run.status,
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "chunks_total": run.counters.chunks_total,
        "chunks_failed": run.counters.chunks_failed,
        "inserted": run.counters.inserted,
        "updated": run.counters.updated,
        "unchanged": run.counters.unchanged,
        "rejected": run.counters.rejected,
        "error": run.error,
    }


def status_payload(snapshot: StatusSnapshot, health: Health) -> dict[str, Any]:
    return {
        "healthy": health.healthy,
        "reasons": health.reasons,
        "rates": snapshot.rate_count,
        "rejected_payloads": snapshot.rejected_count,
        "runs": snapshot.run_count,
        "first_date": snapshot.first_date.isoformat() if snapshot.first_date else None,
        "last_date": snapshot.last_date.isoformat() if snapshot.last_date else None,
        "watermarks": {w.series: w.high_water_date.isoformat() for w in snapshot.watermarks},
        "last_run": _run_dict(snapshot.last_run),
        "last_success": _run_dict(snapshot.last_success),
    }
