"""Tests for lib.simulation."""

from __future__ import annotations

from datetime import date
import math

import polars as pl
import pytest

from lib import simulation, storage


@pytest.fixture(autouse=True)
def fresh_data_dir(tmp_path):
    storage.set_data_dir(tmp_path / "data")
    yield


def _seed_ticker(ticker: str, rows: list[tuple[str, float]], price_type: str = "close",
                 close_only: bool = False) -> None:
    """Seed a ticker's prices (where O=H=L=C=row[1]) and metadata."""
    df = pl.DataFrame(
        {
            "date": [r[0] for r in rows],
            "open": [r[1] for r in rows],
            "high": [r[1] for r in rows],
            "low": [r[1] for r in rows],
            "close": [r[1] for r in rows],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    storage.upsert_ticker_prices(ticker, df)
    storage.upsert_ticker_metadata(ticker, price_type=price_type, close_only=close_only)


# ---------------------------------------------------------------------------
# simulate_ticker_position
# ---------------------------------------------------------------------------

def test_simulate_simple_buy_and_hold():
    """Deposit 1000 when price=100 -> 10 shares."""
    prices = pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-02-01"],
            "open": [100.0, 110.0],
            "high": [100.0, 110.0],
            "low":  [100.0, 110.0],
            "close": [100.0, 110.0],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    cash_flows = [(date(2024, 1, 2), 1000.0)]
    shares, warnings = simulation.simulate_ticker_position(cash_flows, prices, "close")
    assert math.isclose(shares, 10.0)
    assert warnings == []


def test_simulate_uses_next_trading_day_for_weekend():
    """Cash flow on a Saturday with no Saturday price; next Monday's price is used."""
    prices = pl.DataFrame(
        {
            "date": ["2024-01-08"],   # Monday
            "open": [100.0],
            "high": [100.0],
            "low":  [100.0],
            "close": [100.0],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    # Saturday Jan 6
    cash_flows = [(date(2024, 1, 6), 1000.0)]
    shares, warnings = simulation.simulate_ticker_position(cash_flows, prices, "close")
    assert math.isclose(shares, 10.0)
    assert warnings == []


def test_simulate_deposit_then_withdrawal():
    """Buy 10 shares at 100, then sell 5 shares worth at 200."""
    prices = pl.DataFrame(
        {
            "date":  ["2024-01-02", "2024-06-01"],
            "open":  [100.0, 200.0],
            "high":  [100.0, 200.0],
            "low":   [100.0, 200.0],
            "close": [100.0, 200.0],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    cash_flows = [
        (date(2024, 1, 2), 1000.0),     # buy 10 shares at 100
        (date(2024, 6, 1), -1000.0),    # sell $1000 worth at 200 = 5 shares
    ]
    shares, warnings = simulation.simulate_ticker_position(cash_flows, prices, "close")
    assert math.isclose(shares, 5.0)
    assert warnings == []


def test_simulate_negative_shares_warns():
    """Withdrawal larger than simulated holdings -> negative shares + warning."""
    prices = pl.DataFrame(
        {
            "date":  ["2024-01-02", "2024-06-01"],
            "open":  [100.0, 100.0],
            "high":  [100.0, 100.0],
            "low":   [100.0, 100.0],
            "close": [100.0, 100.0],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    cash_flows = [
        (date(2024, 1, 2), 500.0),       # buy 5 shares
        (date(2024, 6, 1), -2000.0),     # withdraw value of 20 shares -> -15 shares
    ]
    shares, warnings = simulation.simulate_ticker_position(cash_flows, prices, "close")
    assert shares < 0
    assert any("negative" in w for w in warnings)


def test_simulate_skips_when_no_price_available():
    """Cash flow before the earliest cached price -> warning, flow skipped."""
    prices = pl.DataFrame(
        {
            "date": ["2024-06-01"],
            "open": [100.0],
            "high": [100.0],
            "low":  [100.0],
            "close": [100.0],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    cash_flows = [(date(2024, 1, 2), 1000.0)]   # no price on or after... wait
    # Actually 2024-06-01 IS on or after 2024-01-02, so the next-trading-day rule
    # picks it up. Let's use a date AFTER the cache to trigger the warning.
    cash_flows = [(date(2024, 12, 1), 1000.0)]
    shares, warnings = simulation.simulate_ticker_position(cash_flows, prices, "close")
    assert shares == 0.0
    assert any("No price available" in w for w in warnings)


def test_simulate_uses_chosen_column():
    """Verify that price_type='open' uses open prices, not close."""
    prices = pl.DataFrame(
        {
            "date":  ["2024-01-02"],
            "open":  [100.0],
            "high":  [110.0],
            "low":   [99.0],
            "close": [105.0],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    cash_flows = [(date(2024, 1, 2), 1000.0)]

    shares_open, _ = simulation.simulate_ticker_position(cash_flows, prices, "open")
    shares_close, _ = simulation.simulate_ticker_position(cash_flows, prices, "close")

    assert math.isclose(shares_open, 10.0)        # 1000 / 100
    assert math.isclose(shares_close, 1000/105)   # 1000 / 105


def test_simulate_invalid_column_raises():
    prices = pl.DataFrame(schema=storage.TICKER_PRICES_SCHEMA)
    with pytest.raises(ValueError):
        simulation.simulate_ticker_position([], prices, "midprice")


# ---------------------------------------------------------------------------
# compute_ticker_comparison_mwrr
# ---------------------------------------------------------------------------

def test_compute_comparison_simple_doubling():
    """Price doubles in a year -> simulated MWRR ~100%."""
    _seed_ticker("VOO", [
        ("2023-01-02", 100.0),
        ("2024-01-02", 200.0),
    ])
    cash_flows = [(date(2023, 1, 2), 1000.0)]
    rate, warnings = simulation.compute_ticker_comparison_mwrr(
        cash_flows, "VOO", date(2024, 1, 2)
    )
    assert rate is not None
    assert math.isclose(rate, 1.0, rel_tol=1e-3)
    # No warnings for a clean case.
    assert warnings == []


def test_compute_comparison_uses_next_trading_day():
    """User deposits on Saturday; simulation uses next trading day's price."""
    _seed_ticker("VOO", [
        ("2024-01-08", 100.0),    # Monday
        ("2025-01-08", 200.0),
    ])
    # Saturday Jan 6
    cash_flows = [(date(2024, 1, 6), 1000.0)]
    rate, warnings = simulation.compute_ticker_comparison_mwrr(
        cash_flows, "VOO", date(2025, 1, 8)
    )
    assert rate is not None
    assert rate > 0


def test_compute_comparison_uncached_ticker_returns_none():
    rate, warnings = simulation.compute_ticker_comparison_mwrr(
        [(date(2024, 1, 1), 1000.0)], "NOPE", date(2024, 6, 1)
    )
    assert rate is None
    assert any("not cached" in w for w in warnings)


def test_compute_comparison_cache_gap_returns_none():
    """Cash flow predates earliest cached price -> warning, no rate."""
    _seed_ticker("VOO", [
        ("2024-01-02", 100.0),
        ("2024-06-01", 110.0),
    ])
    cash_flows = [(date(2023, 1, 1), 1000.0)]   # before cache start
    rate, warnings = simulation.compute_ticker_comparison_mwrr(
        cash_flows, "VOO", date(2024, 6, 1)
    )
    assert rate is None
    assert any("Cache for" in w for w in warnings)


def test_compute_comparison_no_cash_flows_returns_none():
    _seed_ticker("VOO", [("2024-01-02", 100.0)])
    rate, warnings = simulation.compute_ticker_comparison_mwrr(
        [], "VOO", date(2024, 6, 1)
    )
    assert rate is None
    assert any("No cash flows" in w for w in warnings)


def test_compute_comparison_with_withdrawal():
    """End-to-end: deposit, withdrawal, valuation, with realistic prices."""
    _seed_ticker("VOO", [
        ("2023-01-02", 100.0),
        ("2023-07-01", 110.0),
        ("2024-01-02", 120.0),
    ])
    cash_flows = [
        (date(2023, 1, 2), 1000.0),       # buy 10 shares at 100
        (date(2023, 7, 1), -550.0),       # sell value of 5 shares at 110 = $550
    ]
    rate, warnings = simulation.compute_ticker_comparison_mwrr(
        cash_flows, "VOO", date(2024, 1, 2)
    )
    # Final shares: 10 - 5 = 5; final value: 5 * 120 = 600.
    # Series: [+1000 storage, -550 storage] + final 600.
    # Flipped to XIRR: [-1000, +550, +600].
    # That's a profit of 150 on roughly 1000 invested for ~1 year and 450 for half year.
    # Should be a positive rate.
    assert rate is not None
    assert rate > 0
    assert warnings == []
