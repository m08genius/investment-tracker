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


def _price_on_or_before(prices: pl.DataFrame, target: date, column: str) -> float | None:
    """
    Return the price on `target`, or the most recent prior trading day's price
    if `target` is missing. Used for valuation date when target might be a
    weekend/holiday.
    """
    target_iso = target.isoformat()
    before = prices.filter(pl.col("date") <= target_iso)
    if before.is_empty():
        return None
    return float(before.sort("date", descending=True).row(0, named=True)[column])


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
