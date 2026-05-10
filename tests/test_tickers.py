"""Tests for lib.tickers using an injectable fake fetcher (no network)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Callable

import polars as pl
import pytest

from lib import storage, tickers


@pytest.fixture(autouse=True)
def fresh_data_dir(tmp_path):
    storage.set_data_dir(tmp_path / "data")
    # Reset fetcher to default after each test.
    yield
    tickers.set_fetcher(None)


def _make_fake_fetcher(
    ticker_data: dict[str, list[tuple[str, float, float, float, float]]],
) -> Callable[[str, date, date], pl.DataFrame]:
    """
    Build a fake fetcher that returns data for known tickers, filtered to
    the requested [start, end] range.

    `ticker_data` maps ticker -> list of (date_iso, open, high, low, close).
    """

    def fetch(symbol: str, start: date, end: date) -> pl.DataFrame:
        rows = ticker_data.get(symbol.upper(), [])
        filtered = [
            r for r in rows
            if start.isoformat() <= r[0] <= end.isoformat()
        ]
        if not filtered:
            return pl.DataFrame(schema=storage.TICKER_PRICES_SCHEMA)
        return pl.DataFrame(
            {
                "date": [r[0] for r in filtered],
                "open": [r[1] for r in filtered],
                "high": [r[2] for r in filtered],
                "low": [r[3] for r in filtered],
                "close": [r[4] for r in filtered],
            },
            schema=storage.TICKER_PRICES_SCHEMA,
        )

    return fetch


# ---------------------------------------------------------------------------
# detect_close_only
# ---------------------------------------------------------------------------

def test_detect_close_only_true_when_all_equal():
    df = pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "open":  [100.0, 101.0],
            "high":  [100.0, 101.0],
            "low":   [100.0, 101.0],
            "close": [100.0, 101.0],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    assert tickers.detect_close_only(df) is True


def test_detect_close_only_false_when_any_differ():
    df = pl.DataFrame(
        {
            "date":  ["2024-01-02", "2024-01-03"],
            "open":  [100.0, 100.5],
            "high":  [100.5, 101.5],
            "low":   [99.5,  100.0],
            "close": [100.0, 101.0],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    assert tickers.detect_close_only(df) is False


def test_detect_close_only_empty_returns_false():
    df = pl.DataFrame(schema=storage.TICKER_PRICES_SCHEMA)
    assert tickers.detect_close_only(df) is False


# ---------------------------------------------------------------------------
# add_ticker
# ---------------------------------------------------------------------------

def test_add_ticker_persists_prices_and_metadata():
    fake = _make_fake_fetcher({
        "VOO": [
            ("2024-01-02", 450.0, 452.0, 449.5, 451.0),
            ("2024-01-03", 451.0, 453.0, 450.0, 452.5),
        ],
    })
    tickers.set_fetcher(fake)

    meta = tickers.add_ticker("VOO", start=date(2024, 1, 1), end=date(2024, 1, 5))
    assert meta["ticker"] == "VOO"
    assert meta["earliest_date"] == "2024-01-02"
    assert meta["latest_date"] == "2024-01-03"
    assert meta["close_only"] is False
    assert meta["price_type"] == "close"

    df = storage.load_ticker_prices("VOO")
    assert df.height == 2


def test_add_ticker_detects_mutual_fund():
    """Mutual fund: O=H=L=C. Should be flagged close_only and locked to close."""
    fake = _make_fake_fetcher({
        "FXAIX": [
            ("2024-01-02", 100.0, 100.0, 100.0, 100.0),
            ("2024-01-03", 101.0, 101.0, 101.0, 101.0),
            ("2024-01-04", 102.0, 102.0, 102.0, 102.0),
        ],
    })
    tickers.set_fetcher(fake)

    meta = tickers.add_ticker("FXAIX", start=date(2024, 1, 1), end=date(2024, 1, 5))
    assert meta["close_only"] is True
    assert meta["price_type"] == "close"


def test_add_ticker_rejects_empty_response():
    fake = _make_fake_fetcher({})  # nothing for any ticker
    tickers.set_fetcher(fake)
    with pytest.raises(ValueError, match="No price data"):
        tickers.add_ticker("BOGUS", start=date(2024, 1, 1), end=date(2024, 1, 5))


def test_add_ticker_rejects_duplicate():
    fake = _make_fake_fetcher({
        "VOO": [("2024-01-02", 450.0, 452.0, 449.5, 451.0)],
    })
    tickers.set_fetcher(fake)
    tickers.add_ticker("VOO", start=date(2024, 1, 1), end=date(2024, 1, 5))
    with pytest.raises(ValueError, match="already in the UI"):
        tickers.add_ticker("VOO", start=date(2024, 1, 1), end=date(2024, 1, 5))


# ---------------------------------------------------------------------------
# refresh_ticker
# ---------------------------------------------------------------------------

def test_refresh_ticker_fills_gap():
    """Initially seed two rows; refresh fetches a third newer row."""
    fake = _make_fake_fetcher({
        "VOO": [
            ("2024-01-02", 450.0, 452.0, 449.5, 451.0),
            ("2024-01-03", 451.0, 453.0, 450.0, 452.5),
            ("2024-01-04", 452.5, 454.0, 451.0, 453.0),
            ("2024-01-05", 453.0, 455.0, 452.0, 454.5),
        ],
    })
    tickers.set_fetcher(fake)

    # Initial seed: only fetch through Jan 3.
    tickers.add_ticker("VOO", start=date(2024, 1, 2), end=date(2024, 1, 3))
    assert storage.load_ticker_prices("VOO").height == 2

    # Now widen the fetcher's effective end: simulate "today is Jan 5".
    # The refresh logic uses date.today() internally, but our fake fetcher
    # filters by the requested range. Patch by using force_full_refresh which
    # rescans from earliest_date through date.today().
    # Easier: just check that refresh_ticker on its own sees today's data
    # by seeding the fake with today's row.
    today_iso = date.today().isoformat()
    extended_fake = _make_fake_fetcher({
        "VOO": [
            ("2024-01-02", 450.0, 452.0, 449.5, 451.0),
            ("2024-01-03", 451.0, 453.0, 450.0, 452.5),
            (today_iso,    500.0, 501.0, 499.0, 500.5),
        ],
    })
    tickers.set_fetcher(extended_fake)

    meta = tickers.refresh_ticker("VOO")
    assert meta["latest_date"] == today_iso
    assert storage.load_ticker_prices("VOO").height == 3


def test_refresh_ticker_already_up_to_date():
    """If latest_date is today, refresh is a no-op on the data."""
    today_iso = date.today().isoformat()
    fake = _make_fake_fetcher({
        "VOO": [(today_iso, 450.0, 452.0, 449.5, 451.0)],
    })
    tickers.set_fetcher(fake)

    tickers.add_ticker("VOO", start=date.today() - timedelta(days=5), end=date.today())
    # Refresh with the same data available - should be no-op.
    meta = tickers.refresh_ticker("VOO")
    assert meta["latest_date"] == today_iso
    assert storage.load_ticker_prices("VOO").height == 1


def test_refresh_ticker_unknown_raises():
    tickers.set_fetcher(_make_fake_fetcher({}))
    with pytest.raises(ValueError, match="not cached"):
        tickers.refresh_ticker("NOPE")


def test_refresh_ticker_force_full_refresh():
    """force_full_refresh=True rescans from earliest_date."""
    fake = _make_fake_fetcher({
        "VOO": [
            ("2024-01-02", 450.0, 452.0, 449.5, 451.0),
            ("2024-01-03", 451.0, 453.0, 450.0, 452.5),
        ],
    })
    tickers.set_fetcher(fake)
    tickers.add_ticker("VOO", start=date(2024, 1, 2), end=date(2024, 1, 3))

    # Force full refresh - should still complete, no new data
    meta = tickers.refresh_ticker("VOO", force_full_refresh=True)
    assert meta["latest_date"] == "2024-01-03"
