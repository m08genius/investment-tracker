"""
Storage layer for the investment tracker.

All CSV file I/O goes through this module. Other modules and pages must not
read or write CSV files directly. If we ever swap CSV for SQLite or another
backend, this is the only file that needs to change.

See DESIGN.md for full schema documentation.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, date
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Data dir is always relative to the project root (parent of lib/).
_LIB_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _LIB_DIR.parent
DATA_DIR = _PROJECT_ROOT / "data"
TICKERS_DIR = DATA_DIR / "tickers"

ACCOUNTS_PATH = DATA_DIR / "accounts.csv"
ENTRIES_PATH = DATA_DIR / "entries.csv"
CURRENT_VALUES_PATH = DATA_DIR / "current_values.csv"
TICKER_METADATA_PATH = TICKERS_DIR / "_metadata.csv"


def set_data_dir(path: str | Path) -> None:
    """
    Override the data directory location. Used primarily by tests to point
    at a tmp directory. Must be called before any read/write.
    """
    global DATA_DIR, TICKERS_DIR, ACCOUNTS_PATH, ENTRIES_PATH
    global CURRENT_VALUES_PATH, TICKER_METADATA_PATH

    DATA_DIR = Path(path)
    TICKERS_DIR = DATA_DIR / "tickers"
    ACCOUNTS_PATH = DATA_DIR / "accounts.csv"
    ENTRIES_PATH = DATA_DIR / "entries.csv"
    CURRENT_VALUES_PATH = DATA_DIR / "current_values.csv"
    TICKER_METADATA_PATH = TICKERS_DIR / "_metadata.csv"


# ---------------------------------------------------------------------------
# Schemas (polars dtypes for both reading and creating empty frames)
# ---------------------------------------------------------------------------

ACCOUNTS_SCHEMA: dict[str, pl.DataType] = {
    "account_id": pl.Utf8,
    "name": pl.Utf8,
    "description": pl.Utf8,
    "created_at": pl.Utf8,
}

ENTRIES_SCHEMA: dict[str, pl.DataType] = {
    "entry_id": pl.Utf8,
    "account_id": pl.Utf8,
    "amount": pl.Float64,
    "date": pl.Utf8,
    "note": pl.Utf8,
}

CURRENT_VALUES_SCHEMA: dict[str, pl.DataType] = {
    "account_id": pl.Utf8,
    "value": pl.Float64,
    "as_of_date": pl.Utf8,
}

TICKER_PRICES_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
}

TICKER_METADATA_SCHEMA: dict[str, pl.DataType] = {
    "ticker": pl.Utf8,
    "last_refreshed": pl.Utf8,
    "earliest_date": pl.Utf8,
    "latest_date": pl.Utf8,
    "price_type": pl.Utf8,
    "close_only": pl.Boolean,
}

VALID_PRICE_TYPES = {"open", "high", "low", "close"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_dirs() -> None:
    """Create data directories if missing."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TICKERS_DIR.mkdir(parents=True, exist_ok=True)


def _read_or_empty(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Read a CSV with a forced schema, or return an empty frame matching it."""
    _ensure_dirs()
    if not path.exists():
        return pl.DataFrame(schema=schema)
    return pl.read_csv(path, schema=schema)


def _atomic_write_csv(df: pl.DataFrame, path: Path) -> None:
    """Write CSV atomically: write to .tmp, then rename. Survives crashes."""
    _ensure_dirs()
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    df.write_csv(tmp_path)
    os.replace(tmp_path, path)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _new_id() -> str:
    return str(uuid.uuid4())


def _coerce_date(d: date | str) -> date:
    if isinstance(d, str):
        return date.fromisoformat(d)
    return d


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------

def load_accounts() -> pl.DataFrame:
    """Load all accounts."""
    return _read_or_empty(ACCOUNTS_PATH, ACCOUNTS_SCHEMA)


def save_accounts(df: pl.DataFrame) -> None:
    """Overwrite the accounts file with df. Caller owns the full frame."""
    _atomic_write_csv(df.select(list(ACCOUNTS_SCHEMA.keys())), ACCOUNTS_PATH)


def add_account(name: str, description: str = "") -> str:
    """Create a new account; returns its account_id."""
    name = name.strip()
    if not name:
        raise ValueError("Account name cannot be empty.")

    accounts = load_accounts()
    existing = {n.lower() for n in accounts["name"].to_list()}
    if name.lower() in existing:
        raise ValueError(f"An account named {name!r} already exists.")

    new_id = _new_id()
    new_row = pl.DataFrame(
        {
            "account_id": [new_id],
            "name": [name],
            "description": [description.strip()],
            "created_at": [_now_iso()],
        },
        schema=ACCOUNTS_SCHEMA,
    )
    save_accounts(pl.concat([accounts, new_row], how="vertical"))
    return new_id


def remove_account(account_id: str, *, cascade: bool = True) -> None:
    """
    Delete an account. If cascade=True (default), also delete its entries
    and current-value snapshots.
    """
    save_accounts(load_accounts().filter(pl.col("account_id") != account_id))

    if cascade:
        save_entries(load_entries().filter(pl.col("account_id") != account_id))
        save_current_values(
            load_current_values().filter(pl.col("account_id") != account_id)
        )


def get_account(account_id: str) -> dict | None:
    """Return one account as a dict, or None."""
    df = load_accounts().filter(pl.col("account_id") == account_id)
    if df.is_empty():
        return None
    return df.row(0, named=True)


# ---------------------------------------------------------------------------
# Entries (cash flows)
# ---------------------------------------------------------------------------

def load_entries(account_id: str | None = None) -> pl.DataFrame:
    """Load entries, optionally filtered to one account, sorted by date."""
    df = _read_or_empty(ENTRIES_PATH, ENTRIES_SCHEMA)
    if account_id is not None:
        df = df.filter(pl.col("account_id") == account_id)
    return df.sort("date")


def save_entries(df: pl.DataFrame) -> None:
    """Overwrite the entries file."""
    _atomic_write_csv(df.select(list(ENTRIES_SCHEMA.keys())), ENTRIES_PATH)


def add_entry(
    account_id: str,
    amount: float,
    entry_date: date | str,
    note: str = "",
) -> str:
    """
    Add a cash flow. Sign convention: positive = deposit, negative = withdrawal.
    Future-dated entries are rejected.
    """
    if get_account(account_id) is None:
        raise ValueError(f"No account with id {account_id!r}.")

    entry_date = _coerce_date(entry_date)
    if entry_date > date.today():
        raise ValueError("Entry date cannot be in the future.")

    new_id = _new_id()
    new_row = pl.DataFrame(
        {
            "entry_id": [new_id],
            "account_id": [account_id],
            "amount": [float(amount)],
            "date": [entry_date.isoformat()],
            "note": [note.strip()],
        },
        schema=ENTRIES_SCHEMA,
    )
    save_entries(pl.concat([load_entries(), new_row], how="vertical"))
    return new_id


def add_entries_bulk(rows: list[dict]) -> list[str]:
    """
    Add multiple entries in a single write. Each row dict must have keys:
    account_id, amount, date (str or date), note (optional).
    Returns the list of generated entry_ids in the same order.

    Used by the recurring-deposit feature to write all generated entries
    in one atomic write rather than N appends.
    """
    if not rows:
        return []

    accounts = {r["account_id"] for r in rows}
    for aid in accounts:
        if get_account(aid) is None:
            raise ValueError(f"No account with id {aid!r}.")

    today = date.today()
    new_ids: list[str] = []
    new_records = {
        "entry_id": [],
        "account_id": [],
        "amount": [],
        "date": [],
        "note": [],
    }
    for r in rows:
        d = _coerce_date(r["date"])
        if d > today:
            raise ValueError("Entry date cannot be in the future.")
        eid = _new_id()
        new_ids.append(eid)
        new_records["entry_id"].append(eid)
        new_records["account_id"].append(r["account_id"])
        new_records["amount"].append(float(r["amount"]))
        new_records["date"].append(d.isoformat())
        new_records["note"].append(r.get("note", "").strip())

    new_df = pl.DataFrame(new_records, schema=ENTRIES_SCHEMA)
    save_entries(pl.concat([load_entries(), new_df], how="vertical"))
    return new_ids


def remove_entry(entry_id: str) -> None:
    """Delete a single entry by id."""
    save_entries(load_entries().filter(pl.col("entry_id") != entry_id))


def remove_entries(entry_ids: list[str]) -> None:
    """Bulk delete entries by id list."""
    if not entry_ids:
        return
    df = load_entries().filter(~pl.col("entry_id").is_in(list(entry_ids)))
    save_entries(df)


# ---------------------------------------------------------------------------
# Current values (snapshots)
# ---------------------------------------------------------------------------

def load_current_values(account_id: str | None = None) -> pl.DataFrame:
    """Load current-value snapshots, optionally filtered to one account."""
    df = _read_or_empty(CURRENT_VALUES_PATH, CURRENT_VALUES_SCHEMA)
    if account_id is not None:
        df = df.filter(pl.col("account_id") == account_id)
    return df.sort("as_of_date")


def save_current_values(df: pl.DataFrame) -> None:
    _atomic_write_csv(
        df.select(list(CURRENT_VALUES_SCHEMA.keys())), CURRENT_VALUES_PATH
    )


def set_current_value(
    account_id: str,
    value: float,
    as_of_date: date | str | None = None,
) -> None:
    """
    Record the current market value of an account on a given date.
    If a snapshot already exists for (account_id, as_of_date), it is replaced.
    """
    if get_account(account_id) is None:
        raise ValueError(f"No account with id {account_id!r}.")

    if as_of_date is None:
        as_of_date = date.today()
    as_of_date = _coerce_date(as_of_date)
    date_str = as_of_date.isoformat()

    df = load_current_values().filter(
        ~((pl.col("account_id") == account_id) & (pl.col("as_of_date") == date_str))
    )
    new_row = pl.DataFrame(
        {
            "account_id": [account_id],
            "value": [float(value)],
            "as_of_date": [date_str],
        },
        schema=CURRENT_VALUES_SCHEMA,
    )
    save_current_values(pl.concat([df, new_row], how="vertical"))


def get_latest_current_value(account_id: str) -> dict | None:
    """Return the most recent snapshot for an account, or None."""
    df = load_current_values(account_id)
    if df.is_empty():
        return None
    return df.sort("as_of_date", descending=True).row(0, named=True)


def remove_current_value(account_id: str, as_of_date: date | str) -> None:
    """Delete the snapshot for (account_id, as_of_date) if it exists."""
    date_str = _coerce_date(as_of_date).isoformat()
    df = load_current_values().filter(
        ~((pl.col("account_id") == account_id) & (pl.col("as_of_date") == date_str))
    )
    save_current_values(df)


# ---------------------------------------------------------------------------
# Ticker prices
# ---------------------------------------------------------------------------

def _ticker_path(ticker: str) -> Path:
    return TICKERS_DIR / f"{ticker.upper()}.csv"


def load_ticker_prices(ticker: str) -> pl.DataFrame:
    """Load cached price history for a ticker, sorted by date."""
    return _read_or_empty(_ticker_path(ticker), TICKER_PRICES_SCHEMA).sort("date")


def save_ticker_prices(ticker: str, df: pl.DataFrame) -> None:
    """Overwrite cached prices for a ticker."""
    _atomic_write_csv(
        df.select(list(TICKER_PRICES_SCHEMA.keys())).sort("date"),
        _ticker_path(ticker),
    )


def upsert_ticker_prices(ticker: str, new_df: pl.DataFrame) -> None:
    """
    Merge new price rows into the existing cache. On date conflicts, the new
    row wins. Also refreshes _metadata.csv for this ticker.
    """
    if new_df.is_empty():
        return

    existing = load_ticker_prices(ticker)
    combined = (
        pl.concat(
            [
                new_df.select(list(TICKER_PRICES_SCHEMA.keys())),
                existing,
            ],
            how="vertical",
        )
        .unique(subset=["date"], keep="first")
        .sort("date")
    )
    save_ticker_prices(ticker, combined)
    _refresh_ticker_metadata_dates(ticker, combined)


def list_cached_tickers() -> list[str]:
    """List tickers we have price files for."""
    if not TICKERS_DIR.exists():
        return []
    return sorted(
        p.stem for p in TICKERS_DIR.glob("*.csv") if not p.stem.startswith("_")
    )


# ---------------------------------------------------------------------------
# Ticker metadata
# ---------------------------------------------------------------------------

def load_ticker_metadata() -> pl.DataFrame:
    return _read_or_empty(TICKER_METADATA_PATH, TICKER_METADATA_SCHEMA)


def save_ticker_metadata(df: pl.DataFrame) -> None:
    _atomic_write_csv(
        df.select(list(TICKER_METADATA_SCHEMA.keys())), TICKER_METADATA_PATH
    )


def get_ticker_metadata(ticker: str) -> dict | None:
    df = load_ticker_metadata().filter(pl.col("ticker") == ticker.upper())
    if df.is_empty():
        return None
    return df.row(0, named=True)


def upsert_ticker_metadata(
    ticker: str,
    *,
    price_type: str | None = None,
    close_only: bool | None = None,
) -> None:
    """
    Set price_type and/or close_only for a ticker. Also touches last_refreshed.
    Recomputes earliest_date and latest_date from the price file.
    """
    ticker = ticker.upper()

    if price_type is not None and price_type not in VALID_PRICE_TYPES:
        raise ValueError(
            f"price_type must be one of {sorted(VALID_PRICE_TYPES)}, got {price_type!r}"
        )

    prices = load_ticker_prices(ticker)
    if prices.is_empty():
        earliest = ""
        latest = ""
    else:
        earliest = prices["date"].min()
        latest = prices["date"].max()

    existing = get_ticker_metadata(ticker)

    final_price_type = price_type if price_type is not None else (
        existing["price_type"] if existing else "close"
    )
    final_close_only = close_only if close_only is not None else (
        existing["close_only"] if existing else False
    )

    meta = load_ticker_metadata().filter(pl.col("ticker") != ticker)
    new_row = pl.DataFrame(
        {
            "ticker": [ticker],
            "last_refreshed": [_now_iso()],
            "earliest_date": [earliest],
            "latest_date": [latest],
            "price_type": [final_price_type],
            "close_only": [bool(final_close_only)],
        },
        schema=TICKER_METADATA_SCHEMA,
    )
    save_ticker_metadata(pl.concat([meta, new_row], how="vertical"))


def set_ticker_price_type(ticker: str, price_type: str) -> None:
    """Convenience wrapper for changing just the price_type."""
    upsert_ticker_metadata(ticker, price_type=price_type)


def _refresh_ticker_metadata_dates(ticker: str, prices_df: pl.DataFrame) -> None:
    """
    Internal: called after upserting prices. Updates earliest_date,
    latest_date, and last_refreshed without touching price_type/close_only.
    """
    ticker = ticker.upper()
    existing = get_ticker_metadata(ticker)

    if prices_df.is_empty():
        earliest = ""
        latest = ""
    else:
        earliest = prices_df["date"].min()
        latest = prices_df["date"].max()

    price_type = existing["price_type"] if existing else "close"
    close_only = existing["close_only"] if existing else False

    meta = load_ticker_metadata().filter(pl.col("ticker") != ticker)
    new_row = pl.DataFrame(
        {
            "ticker": [ticker],
            "last_refreshed": [_now_iso()],
            "earliest_date": [earliest],
            "latest_date": [latest],
            "price_type": [price_type],
            "close_only": [bool(close_only)],
        },
        schema=TICKER_METADATA_SCHEMA,
    )
    save_ticker_metadata(pl.concat([meta, new_row], how="vertical"))


def remove_ticker(ticker: str) -> None:
    """Remove a ticker from the UI (metadata only). Price file is kept on disk
    so re-adding the ticker can refresh from where it left off."""
    meta = load_ticker_metadata().filter(pl.col("ticker") != ticker.upper())
    save_ticker_metadata(meta)
