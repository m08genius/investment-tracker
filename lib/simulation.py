"""
Ticker comparison simulation.

Answers the question: "What MWRR would I have gotten if I'd put the same
money in this ticker on the same days?"

For each cash flow on date D with amount A (storage convention: deposit
positive, withdrawal negative):

    Look up price(D) using the ticker's configured price column.
    If D is not a trading day (e.g. weekend, holiday), use the next available
    trading day's price.
    delta_shares = A / price(D)
    shares_held += delta_shares     # positive A buys, negative A sells

At the valuation date:
    simulated_value = shares_held * price(valuation_date)

Then compute MWRR on the original cash flow series with `simulated_value`
as the final positive flow on the valuation date.
"""

from __future__ import annotations

import math
from datetime import date

import polars as pl

from lib import returns, storage


def _price_lookup(prices: pl.DataFrame, target: date, column: str) -> float | None:
    """
    Return the price on `target`, or the next available trading day's price
    if `target` is missing. Returns None if no data on or after `target`.

    `prices` is expected to be sorted by date ascending.
    """
    target_iso = target.isoformat()
    after = prices.filter(pl.col("date") >= target_iso)
    if after.is_empty():
        return None
    return float(after.row(0, named=True)[column])


def _price_on_or_before(
    prices: pl.DataFrame,
    target: date,
    column: str,
    max_days_back: int | None = 3,
) -> float | None:
    """Return the price on *target*, or the nearest prior trading day's price.

    *max_days_back* caps how stale the result can be (calendar days).  The
    default of 3 covers a weekend (Sat→Fri = 1, Sun→Fri = 2) plus one public
    holiday (Mon→Fri = 3).  Pass ``None`` for uncapped historical lookups.
    """
    target_iso = target.isoformat()
    before = prices.filter(pl.col("date") <= target_iso).sort("date", descending=True)
    if before.is_empty():
        return None
    row = before.row(0, named=True)
    effective = date.fromisoformat(row["date"])
    if max_days_back is not None and (target - effective).days > max_days_back:
        return None
    return float(row[column])


def simulate_ticker_position(
    cash_flows: list[tuple[date, float]],
    ticker_prices: pl.DataFrame,
    price_column: str,
) -> tuple[float, list[str]]:
    """
    Replay cash_flows into shares of the ticker. Returns (final_shares_held, warnings).

    Sign convention for cash_flows: storage convention (deposit positive,
    withdrawal negative), same as lib.storage.
    """
    if price_column not in ("open", "high", "low", "close"):
        raise ValueError(f"Invalid price column: {price_column!r}")

    warnings: list[str] = []
    shares = 0.0
    last_warned_negative = False

    for d, amount in sorted(cash_flows, key=lambda t: t[0]):
        if amount == 0:
            continue
        price = _price_lookup(ticker_prices, d, price_column)
        if price is None or price <= 0:
            warnings.append(
                f"No price available on or after {d.isoformat()}; "
                f"skipping this cash flow."
            )
            continue
        # Positive deposit -> buy shares. Negative withdrawal -> sell shares.
        delta = amount / price
        shares += delta

        if shares < 0 and not last_warned_negative:
            warnings.append(
                f"Simulated shares went negative on {d.isoformat()} "
                f"(withdrawal exceeded simulated holdings at that date). "
                f"Comparison may be misleading."
            )
            last_warned_negative = True
        elif shares >= 0:
            last_warned_negative = False

    return shares, warnings


def compute_ticker_comparison_mwrr(
    cash_flows: list[tuple[date, float]],
    ticker: str,
    valuation_date: date,
) -> tuple[float | None, list[str]]:
    """
    End-to-end: load ticker prices and metadata, simulate position,
    compute MWRR on the resulting cash flow + simulated value series.

    Returns (rate, warnings). Rate is None if computation is impossible
    (no data, no convergence, etc.); warnings is a list of human-readable
    notes that should be surfaced to the user.
    """
    warnings: list[str] = []

    if not cash_flows:
        warnings.append("No cash flows to simulate.")
        return None, warnings

    meta = storage.get_ticker_metadata(ticker)
    if meta is None:
        warnings.append(f"Ticker {ticker} not cached. Refresh ticker data.")
        return None, warnings

    prices = storage.load_ticker_prices(ticker)
    if prices.is_empty():
        warnings.append(f"No prices cached for {ticker}.")
        return None, warnings

    earliest_flow = min(d for d, _ in cash_flows)
    earliest_cached = date.fromisoformat(meta["earliest_date"])
    # Allow a small grace window: if the earliest flow is at most 7 days before
    # the earliest cached price, the next-trading-day lookup will still find a
    # valid price (covers weekends, market holidays). Beyond that, it's a real
    # cache gap and we ask the user to refresh.
    if (earliest_cached - earliest_flow).days > 7:
        warnings.append(
            f"Cache for {ticker} starts {earliest_cached.isoformat()}, "
            f"but earliest cash flow is {earliest_flow.isoformat()}. "
            f"Refresh ticker data with an earlier start date."
        )
        return None, warnings

    price_column = meta["price_type"]

    shares, sim_warnings = simulate_ticker_position(
        cash_flows, prices, price_column
    )
    warnings.extend(sim_warnings)

    valuation_price = _price_on_or_before(prices, valuation_date, price_column)
    if valuation_price is None or valuation_price <= 0:
        warnings.append(
            f"No {ticker} price available on or before {valuation_date.isoformat()}."
        )
        return None, warnings

    simulated_value = shares * valuation_price

    rate = returns.compute_mwrr(cash_flows, simulated_value, valuation_date)
    if rate is None:
        warnings.append(
            "MWRR did not converge for the simulated series."
        )
    return rate, warnings


def compute_ticker_comparison_twrr(
    snapshots: list[tuple[date, float]],
    ticker: str,
) -> tuple[float | None, list[str]]:
    """
    TWRR the ticker would have earned over the same sub-periods as the
    account's snapshots.

    For each sub-period [T_{i-1}, T_i]:
        ticker_HPR = price(T_i) / price(T_{i-1}) - 1
    using _price_on_or_before for both endpoints.

    Parameters
    ----------
    snapshots
        List of (date, value) pairs defining the sub-period boundaries.
        At least 2 required.
    ticker
        Cached ticker symbol.

    Returns
    -------
    (annualized_rate, warnings). Rate is None if computation is impossible.
    """
    warnings: list[str] = []

    if len(snapshots) < 2:
        warnings.append("Need at least 2 snapshots to compute ticker TWRR.")
        return None, warnings

    snaps = sorted(snapshots, key=lambda t: t[0])
    t0 = snaps[0][0]

    meta = storage.get_ticker_metadata(ticker)
    if meta is None:
        warnings.append(f"Ticker {ticker} not cached. Refresh ticker data.")
        return None, warnings

    prices = storage.load_ticker_prices(ticker)
    if prices.is_empty():
        warnings.append(f"No prices cached for {ticker}.")
        return None, warnings

    # Cache-gap guard: earliest snapshot must be coverable by the cache.
    if meta["earliest_date"]:
        earliest_cached = date.fromisoformat(meta["earliest_date"])
        if (earliest_cached - t0).days > 7:
            warnings.append(
                f"Cache for {ticker} starts {earliest_cached.isoformat()}, "
                f"but earliest snapshot is {t0.isoformat()}. "
                f"Refresh ticker data with an earlier start date."
            )
            return None, warnings

    price_column = meta["price_type"]

    cumulative = 1.0
    for i in range(1, len(snaps)):
        t_prev = snaps[i - 1][0]
        t_cur = snaps[i][0]

        p_start = _price_on_or_before(prices, t_prev, price_column, max_days_back=None)
        p_end = _price_on_or_before(prices, t_cur, price_column, max_days_back=None)

        if p_start is None or p_start <= 0:
            warnings.append(
                f"No {ticker} price available on or before {t_prev.isoformat()}."
            )
            return None, warnings
        if p_end is None or p_end <= 0:
            warnings.append(
                f"No {ticker} price available on or before {t_cur.isoformat()}."
            )
            return None, warnings

        hpr = p_end / p_start - 1
        cumulative *= 1.0 + hpr

    total_days = (snaps[-1][0] - snaps[0][0]).days
    if total_days == 0:
        warnings.append("All snapshots are on the same date; cannot compute TWRR.")
        return None, warnings

    twrr_cumulative = cumulative - 1.0
    if twrr_cumulative == -1.0:
        return None, warnings

    annualized = (1.0 + twrr_cumulative) ** (365.0 / total_days) - 1.0
    if not math.isfinite(annualized):
        warnings.append(f"TWRR result was not finite for {ticker}.")
        return None, warnings

    return annualized, warnings
