"""Turn a raw Frankfurter range payload into validated rows plus quarantined rejections."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from pydantic import BaseModel, TypeAdapter, ValidationError

from fx_sync.models import DateRange, RateRow, Rejection

_DATE = TypeAdapter(date)
_SNIPPET_LIMIT = 2000


class RangeEnvelope(BaseModel):
    base: str
    rates: dict[str, Any]


@dataclass
class ParsedChunk:
    rows: list[RateRow] = field(default_factory=list)
    rejections: list[Rejection] = field(default_factory=list)


def _snippet(value: object) -> str:
    return json.dumps(value, default=str)[:_SNIPPET_LIMIT]


def _short_error(exc: ValidationError) -> str:
    first = exc.errors()[0]
    loc = ".".join(str(part) for part in first["loc"])
    return f"{loc}: {first['msg']}" if loc else str(first["msg"])


def parse_range_payload(payload: object, *, base: str, window: DateRange) -> ParsedChunk:
    """Validate ``payload`` for ``window``.

    Never raises on bad data. Dates outside ``window`` are dropped silently because the API
    returns the previous business day when the window starts on a weekend. Days that are
    absent (weekends, holidays) are simply not in the payload and are not an error.
    """
    parsed = ParsedChunk()
    try:
        envelope = RangeEnvelope.model_validate(payload)
    except ValidationError as exc:
        parsed.rejections.append(
            Rejection(window, f"envelope: {_short_error(exc)}", _snippet(payload))
        )
        return parsed
    if envelope.base != base:
        reason = f"base mismatch: expected {base}, got {envelope.base}"
        parsed.rejections.append(Rejection(window, reason, _snippet(payload)))
        return parsed

    for raw_date, day in sorted(envelope.rates.items()):
        try:
            rate_date = _DATE.validate_python(raw_date, strict=False)
        except ValidationError:
            parsed.rejections.append(
                Rejection(window, f"invalid date key {raw_date!r}", _snippet(day))
            )
            continue
        if rate_date not in window:
            continue
        if not isinstance(day, dict):
            parsed.rejections.append(
                Rejection(window, "day entry is not an object", _snippet(day), rate_date)
            )
            continue
        for quote, value in sorted(day.items()):
            try:
                parsed.rows.append(RateRow(base=base, quote=quote, rate_date=rate_date, rate=value))
            except ValidationError as exc:
                parsed.rejections.append(
                    Rejection(
                        window,
                        _short_error(exc),
                        _snippet({"date": raw_date, "quote": quote, "value": value}),
                        rate_date,
                    )
                )
    return parsed
