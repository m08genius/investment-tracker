"""
Ticker price fetching via yfinance.

Responsibilities:
- Fetch OHLC history from yfinance and convert to polars.
- Detect close-only tickers (mutual funds, where O/H/L equal close).
- Add new tickers (full fetch from a configurable start date).
- Refresh existing tickers (intelligent gap-fill from latest_date forward).

All persistence goes through lib.storage. This module never touches CSVs
directly.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Callable

import polars as pl

from lib import storage

# Default first-fetch start date per the design.
DEFAULT_START_DATE = date(2012, 10, 1)


# ---------------------------------------------------------------------------
# yfinance fetcher (injectable for tests)
# ---------------------------------------------------------------------------

def _yfinance_fetcher(ticker: str, start: date, end: date) -> pl.DataFrame:
    """
    Fetch OHLC from yfinance and return as a polars DataFrame matching
    storage.TICKER_PRICES_SCHEMA.

    yfinance is imported lazily so unit tests can run without it installed.
    """
    import yfinance as yf

    # yfinance's `end` is exclusive, so add one day to make our range inclusive.
    yf_end = end + timedelta(days=1)
    t = yf.Ticker(ticker)
    pdf = t.history(
        start=start.isoformat(),
        end=yf_end.isoformat(),
        auto_adjust=True,
    )

    if pdf is None or pdf.empty:
        return pl.DataFrame(schema=storage.TICKER_PRICES_SCHEMA)

    # pdf.index is a DatetimeIndex; we want ISO date strings.
    pdf = pdf.reset_index()
    # The date column is named "Date" in yfinance output.
    pdf["Date"] = pdf["Date"].dt.strftime("%Y-%m-%d")

    df = pl.from_pandas(
        pdf[["Date", "Open", "High", "Low", "Close"]]
    ).rename(
        {"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close"}
    )
    # Cast to our schema's dtypes.
    df = df.with_columns(
        pl.col("date").cast(pl.Utf8),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
    )
    return df.select(list(storage.TICKER_PRICES_SCHEMA.keys()))


# Module-level fetcher allows tests to swap in a fake.
_fetcher: Callable[[str, date, date], pl.DataFrame] = _yfinance_fetcher


def set_fetcher(fn: Callable[[str, date, date], pl.DataFrame]) -> None:
    """Override the price-fetcher (used by tests). Pass None to reset."""
    global _fetcher
    _fetcher = fn if fn is not None else _yfinance_fetcher


# ---------------------------------------------------------------------------
# Close-only detection
# ---------------------------------------------------------------------------

def detect_close_only(prices: pl.DataFrame, lookback_rows: int = 30) -> bool:
    """
    Return True if open == high == low == close for the most recent
    `lookback_rows` rows. This is the signature of mutual fund data, where
    only one NAV is reported per day.
    """
    if prices.is_empty():
        return False

    n = min(lookback_rows, prices.height)
    sample = prices.sort("date", descending=True).head(n)

    # Compare with a tiny tolerance because yfinance sometimes returns
    # floating-point nudges where it should report exact equality.
    eq = sample.select(
        ((pl.col("open") - pl.col("close")).abs() < 1e-9).all().alias("oc"),
        ((pl.col("high") - pl.col("close")).abs() < 1e-9).all().alias("hc"),
        ((pl.col("low") - pl.col("close")).abs() < 1e-9).all().alias("lc"),
    ).row(0, named=True)

    return bool(eq["oc"] and eq["hc"] and eq["lc"])


# ---------------------------------------------------------------------------
# Public API: add new ticker
# ---------------------------------------------------------------------------

def add_ticker(
    ticker: str,
    start: date | None = None,
    end: date | None = None,
) -> dict:
    """
    Add a brand-new ticker to the cache. Performs a full fetch from `start`
    (default: 2012-10-01) through `end` (default: today), detects close-only,
    persists prices and metadata, and returns the new metadata dict.

    Raises ValueError if the ticker returns no data.
    """
    ticker = ticker.upper().strip()
    if not ticker:
        raise ValueError("Ticker symbol cannot be empty.")

    if storage.get_ticker_metadata(ticker) is not None:
        raise ValueError(
            f"Ticker {ticker!r} is already cached. Use refresh_ticker() instead."
        )

    start = start or DEFAULT_START_DATE
    end = end or date.today()

    df = _fetcher(ticker, start, end)
    if df.is_empty():
        raise ValueError(
            f"No price data returned for {ticker!r}. "
            "Verify the symbol is correct."
        )

    storage.upsert_ticker_prices(ticker, df)

    close_only = detect_close_only(df)
    storage.upsert_ticker_metadata(
        ticker,
        price_type="close",
        close_only=close_only,
    )

    meta = storage.get_ticker_metadata(ticker)
    assert meta is not None
    return meta


# ---------------------------------------------------------------------------
# Public API: refresh existing ticker
# ---------------------------------------------------------------------------

def refresh_ticker(ticker: str, *, force_full_refresh: bool = False) -> dict:
    """
    Refresh an existing cached ticker by fetching only the gap from
    latest_date+1 through today. With force_full_refresh=True, refetches
    the entire range from earliest_date through today.

    Returns the updated metadata dict. Raises ValueError if the ticker
    is not yet cached (use add_ticker() first).
    """
    ticker = ticker.upper().strip()
    meta = storage.get_ticker_metadata(ticker)
    if meta is None:
        raise ValueError(
            f"Ticker {ticker!r} is not cached. Use add_ticker() first."
        )

    today = date.today()

    if force_full_refresh:
        start = (
            date.fromisoformat(meta["earliest_date"])
            if meta["earliest_date"]
            else DEFAULT_START_DATE
        )
    else:
        if not meta["latest_date"]:
            start = DEFAULT_START_DATE
        else:
            start = date.fromisoformat(meta["latest_date"]) + timedelta(days=1)

    if start > today:
        # Already up to date; just touch last_refreshed and return.
        storage.upsert_ticker_metadata(ticker)  # no fields = touch only
        updated = storage.get_ticker_metadata(ticker)
        assert updated is not None
        return updated

    df = _fetcher(ticker, start, today)
    if not df.is_empty():
        storage.upsert_ticker_prices(ticker, df)
        # Re-detect close-only on the merged dataset (in case of new behavior).
        full = storage.load_ticker_prices(ticker)
        close_only = detect_close_only(full)
        # Preserve user-set price_type, but refresh close_only.
        # If close_only just became True, also force price_type back to close.
        if close_only:
            storage.upsert_ticker_metadata(
                ticker, close_only=True, price_type="close"
            )
        else:
            storage.upsert_ticker_metadata(ticker, close_only=False)
    else:
        # No new data, just touch metadata.
        storage.upsert_ticker_metadata(ticker)

    updated = storage.get_ticker_metadata(ticker)
    assert updated is not None
    return updated


def refresh_all_tickers() -> dict[str, dict]:
    """Refresh every cached ticker. Returns ticker -> metadata dict."""
    out: dict[str, dict] = {}
    for t in storage.list_cached_tickers():
        try:
            out[t] = refresh_ticker(t)
        except Exception as e:  # noqa: BLE001 - surface error per ticker, don't fail batch
            out[t] = {"error": str(e)}
    return out
