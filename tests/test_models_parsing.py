from __future__ import annotations

import itertools
from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from fx_sync.errors import InvalidRangeError
from fx_sync.models import DateRange, RateRow, plan_chunks, series_key
from fx_sync.parsing import parse_range_payload

W = DateRange(date(2025, 3, 10), date(2025, 3, 14))


def test_date_range_basics() -> None:
    assert W.days == 5
    assert date(2025, 3, 10) in W and date(2025, 3, 15) not in W and "x" not in W
    assert str(W) == "2025-03-10..2025-03-14"
    with pytest.raises(InvalidRangeError):
        DateRange(date(2025, 3, 2), date(2025, 3, 1))


@pytest.mark.parametrize(("days", "expected"), [(1, 5), (2, 3), (5, 1), (90, 1)])
def test_plan_chunks_cover_exactly_once(days: int, expected: int) -> None:
    chunks = plan_chunks(W, days)
    assert len(chunks) == expected
    assert chunks[0].start == W.start and chunks[-1].end == W.end
    assert all(a.end < b.start for a, b in itertools.pairwise(chunks))
    assert sum(c.days for c in chunks) == W.days


def test_plan_chunks_rejects_bad_size() -> None:
    with pytest.raises(InvalidRangeError):
        plan_chunks(W, 0)


def test_series_key_is_order_independent() -> None:
    assert series_key("USD", ("GBP", "EUR")) == series_key("USD", ("EUR", "GBP")) == "USD:EUR,GBP"
    assert series_key("USD", ()) == "USD:*"


def test_rate_row_hash_is_canonical() -> None:
    a = RateRow(base="USD", quote="EUR", rate_date=date(2025, 3, 10), rate=Decimal("0.90"))
    b = RateRow(base="USD", quote="EUR", rate_date=date(2025, 3, 10), rate=Decimal("0.9"))
    c = RateRow(base="USD", quote="EUR", rate_date=date(2025, 3, 10), rate=Decimal("0.91"))
    assert a.rate_text == "0.9" and a.source_hash == b.source_hash != c.source_hash


@pytest.mark.parametrize("bad", ["0.9", True, None, 0, -1, float("nan"), float("inf")])
def test_rate_row_rejects_bad_rates(bad: object) -> None:
    with pytest.raises(ValidationError):
        RateRow(base="USD", quote="EUR", rate_date=date(2025, 3, 10), rate=bad)


def payload(rates: object, base: str = "USD") -> dict[str, object]:
    return {"base": base, "rates": rates}


def test_parse_valid_and_out_of_window_dates_dropped() -> None:
    rates = {
        "2025-03-07": {"EUR": 0.9},  # before window (API returns previous business day)
        "2025-03-10": {"EUR": 0.91, "GBP": 0.8},
        "2025-03-14": {"EUR": 0.92},
    }
    parsed = parse_range_payload(payload(rates), base="USD", window=W)
    assert [(r.rate_date.isoformat(), r.quote) for r in parsed.rows] == [
        ("2025-03-10", "EUR"), ("2025-03-10", "GBP"), ("2025-03-14", "EUR"),
    ]  # fmt: skip
    assert parsed.rejections == []


def test_parse_quarantines_each_kind_of_bad_data() -> None:
    rates = {
        "not-a-date": {"EUR": 1.0},
        "2025-03-10": {"EUR": "0.9", "GBP": -1, "JPY": 150.5, "eur": 1.0},
        "2025-03-11": "oops",
    }
    parsed = parse_range_payload(payload(rates), base="USD", window=W)
    assert [r.quote for r in parsed.rows] == ["JPY"]
    reasons = sorted(r.reason for r in parsed.rejections)
    assert len(reasons) == 5
    assert any("invalid date key" in r for r in reasons)
    assert any("not an object" in r for r in reasons)


@pytest.mark.parametrize(
    "bad", [None, [], "x", {"base": "USD"}, {"rates": {}}, {"base": 1, "rates": {}}]
)
def test_parse_bad_envelope_is_one_rejection(bad: object) -> None:
    parsed = parse_range_payload(bad, base="USD", window=W)
    assert parsed.rows == [] and len(parsed.rejections) == 1
    assert parsed.rejections[0].reason.startswith("envelope")


def test_parse_base_mismatch() -> None:
    parsed = parse_range_payload(payload({}, base="EUR"), base="USD", window=W)
    assert "base mismatch" in parsed.rejections[0].reason


def test_parse_empty_rates_is_not_an_error() -> None:
    parsed = parse_range_payload(payload({}), base="USD", window=W)
    assert parsed.rows == [] and parsed.rejections == []
