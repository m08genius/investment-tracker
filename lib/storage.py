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
TICKER_METADATA_PATH = TICKERS_DIR / "_metadata.csv"

# Legacy path — only used by the one-time migration from the old two-table layout.
_LEGACY_CURRENT_VALUES_PATH = DATA_DIR / "current_values.csv"
_LEGACY_CURRENT_VALUES_SCHEMA: dict[str, pl.DataType] = {
    "account_id": pl.Utf8,
    "value": pl.Float64,
    "as_of_date": pl.Utf8,
}


def set_data_dir(path: str | Path) -> None:
    """
    Override the data directory location. Used primarily by tests to point
    at a tmp directory. Must be called before any read/write.
    """
    global DATA_DIR, TICKERS_DIR, ACCOUNTS_PATH, ENTRIES_PATH
    global _LEGACY_CURRENT_VALUES_PATH, TICKER_METADATA_PATH

    DATA_DIR = Path(path)
    TICKERS_DIR = DATA_DIR / "tickers"
    ACCOUNTS_PATH = DATA_DIR / "accounts.csv"
    ENTRIES_PATH = DATA_DIR / "entries.csv"
    _LEGACY_CURRENT_VALUES_PATH = DATA_DIR / "current_values.csv"
    TICKER_METADATA_PATH = TICKERS_DIR / "_metadata.csv"


# ---------------------------------------------------------------------------
# Schemas (polars dtypes for both reading and creating empty frames)
# ---------------------------------------------------------------------------

ACCOUNTS_SCHEMA: dict[str, pl.DataType] = {
    "account_id": pl.Utf8,
    "group_name": pl.Utf8,    # "Account Group"
    "security":   pl.Utf8,    # "Security Name"
    "is_ticker":  pl.Boolean, # True = ticker security
    "ticker":     pl.Utf8,    # ticker symbol when is_ticker=True, "" otherwise
    "created_at": pl.Utf8,
}

ENTRIES_SCHEMA: dict[str, pl.DataType] = {
    "entry_id":        pl.Utf8,
    "account_id":      pl.Utf8,
    "amount":          pl.Float64,
    "entry_time":      pl.Utf8,
    "note":            pl.Utf8,
    "snapshot_value":  pl.Float64,
    "shares":          pl.Float64,
    "price_per_share": pl.Float64,
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
# One-time migration: current_values.csv → entries.csv snapshot columns
# ---------------------------------------------------------------------------

def _maybe_migrate_accounts_schema() -> None:
    """
    If accounts.csv has the old schema (name + description columns), rename:
      name        → security
      description → group_name
    and add is_ticker=False, ticker="" columns.
    """
    if not ACCOUNTS_PATH.exists():
        return
    df = pl.read_csv(ACCOUNTS_PATH)
    if "name" not in df.columns and "description" not in df.columns:
        return
    # Rename old columns
    if "name" in df.columns:
        df = df.rename({"name": "security"})
    if "description" in df.columns:
        df = df.rename({"description": "group_name"})
    # Add new columns if missing
    if "is_ticker" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("is_ticker"))
    if "ticker" not in df.columns:
        df = df.with_columns(pl.lit("").alias("ticker"))
    _atomic_write_csv(df.select(list(ACCOUNTS_SCHEMA.keys())), ACCOUNTS_PATH)


def _maybe_migrate_entries_add_share_columns() -> None:
    """
    If entries.csv lacks shares/price_per_share columns, back-fill them:
      shares          = 1.0
      price_per_share = abs(amount)   (preserves existing dollar values)
    Pure snapshot rows (amount=0) get shares=0, price_per_share=0.
    """
    if not ENTRIES_PATH.exists():
        return
    df = pl.read_csv(ENTRIES_PATH)
    if "shares" in df.columns and "price_per_share" in df.columns:
        return
    if "shares" not in df.columns:
        df = df.with_columns(
            pl.when(pl.col("amount") != 0.0)
            .then(pl.lit(1.0))
            .otherwise(pl.lit(0.0))
            .alias("shares")
        )
    if "price_per_share" not in df.columns:
        df = df.with_columns(
            pl.col("amount").abs().alias("price_per_share")
        )
    _atomic_write_csv(df.select(list(ENTRIES_SCHEMA.keys())), ENTRIES_PATH)


def _maybe_drop_snapshot_time_column() -> None:
    """If entries.csv has the legacy snapshot_time column, drop it and rewrite."""
    if not ENTRIES_PATH.exists():
        return
    df = pl.read_csv(ENTRIES_PATH)
    if "snapshot_time" not in df.columns:
        return
    df = df.drop("snapshot_time")
    _atomic_write_csv(df.select(list(ENTRIES_SCHEMA.keys())), ENTRIES_PATH)


def _maybe_migrate_current_values() -> None:
    """
    If the legacy current_values.csv still exists, fold its rows into the
    entries table as snapshot columns, then delete the old file.

    Called automatically by load_entries() on the first read after upgrade.
    """
    if not _LEGACY_CURRENT_VALUES_PATH.exists():
        return

    cv = _read_or_empty(_LEGACY_CURRENT_VALUES_PATH, _LEGACY_CURRENT_VALUES_SCHEMA)
    if cv.is_empty():
        _LEGACY_CURRENT_VALUES_PATH.unlink(missing_ok=True)
        return

    df = _read_or_empty(ENTRIES_PATH, ENTRIES_SCHEMA)

    for row in cv.iter_rows(named=True):
        aid = row["account_id"]
        date_str = row["as_of_date"]
        value = float(row["value"])

        # Try to attach to an existing entry on the same account + date that
        # doesn't already have a snapshot.
        mask = (
            (pl.col("account_id") == aid)
            & (pl.col("entry_time") == date_str)
            & pl.col("snapshot_value").is_null()
        )
        if df.filter(mask).height > 0:
            df = df.with_columns(
                pl.when(mask).then(pl.lit(value)).otherwise(pl.col("snapshot_value")).alias("snapshot_value"),
            )
        else:
            # No matching entry — create a pure snapshot row (amount=0).
            new_row = pl.DataFrame(
                {
                    "entry_id":        [_new_id()],
                    "account_id":      [aid],
                    "amount":          [0.0],
                    "entry_time":      [date_str],
                    "note":            [""],
                    "snapshot_value":  [value],
                    "shares":          [0.0],
                    "price_per_share": [0.0],
                },
                schema=ENTRIES_SCHEMA,
            )
            df = pl.concat([df, new_row], how="vertical")

    _atomic_write_csv(df.select(list(ENTRIES_SCHEMA.keys())), ENTRIES_PATH)
    _LEGACY_CURRENT_VALUES_PATH.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------

def load_accounts() -> pl.DataFrame:
    """Load all accounts."""
    _maybe_migrate_accounts_schema()
    return _read_or_empty(ACCOUNTS_PATH, ACCOUNTS_SCHEMA)


def save_accounts(df: pl.DataFrame) -> None:
    """Overwrite the accounts file with df. Caller owns the full frame."""
    _atomic_write_csv(df.select(list(ACCOUNTS_SCHEMA.keys())), ACCOUNTS_PATH)


def add_account(
    group_name: str,
    security: str,
    is_ticker: bool = False,
    ticker: str = "",
) -> str:
    """
    Create a new security under an account group. Returns its account_id.
    group_name: the Account Group label (e.g. "Fidelity Brokerage").
    security:   the Security Name (e.g. "FXAIX", "Cash").
    is_ticker:  True if this security is a ticker with cached price data.
    ticker:     the ticker symbol when is_ticker=True.
    """
    group_name = group_name.strip()
    security = security.strip()
    if not group_name:
        raise ValueError("Account Group cannot be empty.")
    if not security:
        raise ValueError("Security name cannot be empty.")
    if is_ticker and not ticker.strip():
        raise ValueError("Ticker symbol required when is_ticker=True.")

    accounts = load_accounts()
    existing_pairs = {
        (r["group_name"].lower(), r["security"].lower())
        for r in accounts.iter_rows(named=True)
    }
    if (group_name.lower(), security.lower()) in existing_pairs:
        raise ValueError(
            f"A security named {security!r} already exists in group {group_name!r}."
        )

    new_id = _new_id()
    new_row = pl.DataFrame(
        {
            "account_id": [new_id],
            "group_name": [group_name],
            "security":   [security],
            "is_ticker":  [bool(is_ticker)],
            "ticker":     [ticker.strip().upper() if is_ticker else ""],
            "created_at": [_now_iso()],
        },
        schema=ACCOUNTS_SCHEMA,
    )
    save_accounts(pl.concat([accounts, new_row], how="vertical"))
    return new_id


def remove_account(account_id: str, *, cascade: bool = True) -> None:
    """
    Delete an account. If cascade=True (default), also delete its entries
    (which now embed any snapshot data).
    """
    save_accounts(load_accounts().filter(pl.col("account_id") != account_id))

    if cascade:
        save_entries(load_entries().filter(pl.col("account_id") != account_id))


def get_account(account_id: str) -> dict | None:
    """Return one account as a dict, or None."""
    df = load_accounts().filter(pl.col("account_id") == account_id)
    if df.is_empty():
        return None
    return df.row(0, named=True)


# ---------------------------------------------------------------------------
# Entries (cash flows + optional snapshot)
# ---------------------------------------------------------------------------

def load_entries(account_id: str | None = None) -> pl.DataFrame:
    """Load entries, optionally filtered to one account, sorted by entry_time."""
    _maybe_drop_snapshot_time_column()
    _maybe_migrate_current_values()
    _maybe_migrate_entries_add_share_columns()
    df = _read_or_empty(ENTRIES_PATH, ENTRIES_SCHEMA)
    if account_id is not None:
        df = df.filter(pl.col("account_id") == account_id)
    return df.sort("entry_time")


def save_entries(df: pl.DataFrame) -> None:
    """Overwrite the entries file."""
    _atomic_write_csv(df.select(list(ENTRIES_SCHEMA.keys())), ENTRIES_PATH)


def add_entry(
    account_id: str,
    amount: float,
    entry_date: date | str,
    note: str = "",
    *,
    snapshot_value: float | None = None,
    shares: float | None = None,
    price_per_share: float | None = None,
) -> str:
    """
    Add a cash flow. Sign convention: positive = deposit, negative = withdrawal.
    Future-dated entries are rejected.

    shares and price_per_share are always stored. If not provided they default to
    1.0 and abs(amount) respectively (for migration compatibility and simple entries).
    If either is provided, both must be; price_per_share must be > 0.

    If a pure snapshot row (amount=0) already exists on entry_date, the cash
    flow is merged into that row rather than creating a duplicate.
    """
    if get_account(account_id) is None:
        raise ValueError(f"No account with id {account_id!r}.")

    entry_date = _coerce_date(entry_date)
    if entry_date > date.today():
        raise ValueError("Entry date cannot be in the future.")

    # Resolve shares / price_per_share
    amt = float(amount)
    if shares is not None or price_per_share is not None:
        if shares is None or price_per_share is None:
            raise ValueError("Provide both shares and price_per_share together.")
        shares = float(shares)
        price_per_share = float(price_per_share)
        if price_per_share <= 0:
            raise ValueError("price_per_share must be greater than 0.")
    else:
        shares = 1.0 if amt != 0.0 else 0.0
        price_per_share = abs(amt) if amt != 0.0 else 0.0

    snap_val = float(snapshot_value) if snapshot_value is not None else None
    existing = load_entries(account_id)
    date_str = entry_date.isoformat()

    if amt != 0.0:
        # Reject if a non-zero entry already occupies this date.
        if existing.filter(
            (pl.col("amount") != 0.0) & (pl.col("entry_time") == date_str)
        ).height > 0:
            raise ValueError(
                f"An entry already exists for {date_str} on this account. "
                "Delete the existing entry first or use a different date."
            )

        # If a pure snapshot row exists on the same date, merge into it.
        pure_snaps = existing.filter(
            (pl.col("amount") == 0.0) & (pl.col("entry_time") == date_str)
        )
        if pure_snaps.height > 0:
            existing_row = pure_snaps.row(0, named=True)
            merged_snap = snap_val if snap_val is not None else existing_row["snapshot_value"]
            then_snap = pl.lit(float(merged_snap)) if merged_snap is not None else pl.lit(None, dtype=pl.Float64)
            df = load_entries()
            mask = (
                (pl.col("account_id") == account_id)
                & (pl.col("entry_time") == date_str)
                & (pl.col("amount") == 0.0)
            )
            df = df.with_columns(
                pl.when(mask).then(pl.lit(amt)).otherwise(pl.col("amount")).alias("amount"),
                pl.when(mask).then(pl.lit(note.strip())).otherwise(pl.col("note")).alias("note"),
                pl.when(mask).then(then_snap).otherwise(pl.col("snapshot_value")).alias("snapshot_value"),
                pl.when(mask).then(pl.lit(shares)).otherwise(pl.col("shares")).alias("shares"),
                pl.when(mask).then(pl.lit(price_per_share)).otherwise(pl.col("price_per_share")).alias("price_per_share"),
            )
            save_entries(df)
            return existing_row["entry_id"]

    new_id = _new_id()
    new_row = pl.DataFrame(
        {
            "entry_id":        [new_id],
            "account_id":      [account_id],
            "amount":          [amt],
            "entry_time":      [date_str],
            "note":            [note.strip()],
            "snapshot_value":  [snap_val],
            "shares":          [shares],
            "price_per_share": [price_per_share],
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
    new_records: dict[str, list] = {
        "entry_id":        [],
        "account_id":      [],
        "amount":          [],
        "entry_time":      [],
        "note":            [],
        "snapshot_value":  [],
        "shares":          [],
        "price_per_share": [],
    }
    for r in rows:
        d = _coerce_date(r["date"])
        if d > today:
            raise ValueError("Entry date cannot be in the future.")
        amt = float(r["amount"])
        eid = _new_id()
        new_ids.append(eid)
        new_records["entry_id"].append(eid)
        new_records["account_id"].append(r["account_id"])
        new_records["amount"].append(amt)
        new_records["entry_time"].append(d.isoformat())
        new_records["note"].append(r.get("note", "").strip())
        new_records["snapshot_value"].append(None)
        new_records["shares"].append(1.0)
        new_records["price_per_share"].append(abs(amt))

    new_df = pl.DataFrame(new_records, schema=ENTRIES_SCHEMA)
    save_entries(pl.concat([load_entries(), new_df], how="vertical"))
    return new_ids


def remove_entry(entry_id: str) -> None:
    """Delete a single entry by id (and any snapshot attached to it)."""
    save_entries(load_entries().filter(pl.col("entry_id") != entry_id))


def remove_entries(entry_ids: list[str]) -> None:
    """Bulk delete entries by id list."""
    if not entry_ids:
        return
    df = load_entries().filter(~pl.col("entry_id").is_in(list(entry_ids)))
    save_entries(df)


# ---------------------------------------------------------------------------
# Snapshots (views over the entries table)
# ---------------------------------------------------------------------------

def load_snapshots(account_id: str | None = None) -> pl.DataFrame:
    """
    Return all rows that carry a snapshot value, as a DataFrame with columns
    'as_of_date' and 'value' (matching the old current_values shape for
    drop-in compatibility).
    """
    df = load_entries(account_id)
    return (
        df.filter(pl.col("snapshot_value").is_not_null())
        .select(
            pl.col("entry_time").alias("as_of_date"),
            pl.col("snapshot_value").alias("value"),
        )
        .sort("as_of_date")
    )


def set_snapshot(
    account_id: str,
    value: float,
    as_of_date: date | str | None = None,
    note: str = "",
) -> None:
    """
    Record a portfolio snapshot for the given account and date.
    If a row already exists on as_of_date for this account, its snapshot_value
    (and note, if provided) are updated. Otherwise a new amount=0 row is created.
    """
    if get_account(account_id) is None:
        raise ValueError(f"No account with id {account_id!r}.")

    if as_of_date is None:
        as_of_date = date.today()
    date_str = _coerce_date(as_of_date).isoformat()

    df = load_entries()
    mask = (pl.col("account_id") == account_id) & (pl.col("entry_time") == date_str)

    if df.filter(mask).height > 0:
        updates = [
            pl.when(mask)
            .then(pl.lit(float(value)))
            .otherwise(pl.col("snapshot_value"))
            .alias("snapshot_value")
        ]
        if note.strip():
            updates.append(
                pl.when(mask)
                .then(pl.lit(note.strip()))
                .otherwise(pl.col("note"))
                .alias("note")
            )
        df = df.with_columns(updates)
    else:
        new_row = pl.DataFrame(
            {
                "entry_id":        [_new_id()],
                "account_id":      [account_id],
                "amount":          [0.0],
                "entry_time":      [date_str],
                "note":            [note.strip()],
                "snapshot_value":  [float(value)],
                "shares":          [0.0],
                "price_per_share": [0.0],
            },
            schema=ENTRIES_SCHEMA,
        )
        df = pl.concat([df, new_row], how="vertical")

    save_entries(df)


def get_latest_snapshot(account_id: str) -> dict | None:
    """
    Return the most recent snapshot for an account as
    {'as_of_date': str, 'value': float}, or None.
    """
    snaps = load_snapshots(account_id)
    if snaps.is_empty():
        return None
    return snaps.sort("as_of_date", descending=True).row(0, named=True)


def remove_snapshot(account_id: str, as_of_date: date | str) -> None:
    """
    Remove a snapshot.
    - Pure snapshot rows (amount == 0): the whole row is deleted.
    - Entries with an attached snapshot (amount != 0): snapshot fields are
      nulled out; the entry itself is kept.
    """
    date_str = _coerce_date(as_of_date).isoformat()
    df = load_entries()
    mask = (pl.col("account_id") == account_id) & (pl.col("entry_time") == date_str)

    matched = df.filter(mask)
    if matched.is_empty():
        return

    # Pure snapshot rows (amount==0) are deleted entirely; entries keep their cash flow.
    if (matched["amount"] == 0.0).all():
        df = df.filter(~mask)
    else:
        df = df.with_columns(
            pl.when(mask)
            .then(pl.lit(None, dtype=pl.Float64))
            .otherwise(pl.col("snapshot_value"))
            .alias("snapshot_value"),
        )

    save_entries(df)


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
    """List tickers that have a price file on disk (including hidden ones)."""
    if not TICKERS_DIR.exists():
        return []
    return sorted(
        p.stem for p in TICKERS_DIR.glob("*.csv") if not p.stem.startswith("_")
    )


def list_active_tickers() -> list[str]:
    """List tickers that are active in the UI (present in metadata)."""
    meta = load_ticker_metadata()
    if meta.is_empty():
        return []
    return sorted(meta["ticker"].to_list())


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


def get_ticker_price_on_date(
    ticker: str, on_date: date | str, price_type: str = "close"
) -> float | None:
    """Return the cached price for a ticker on a specific date.

    If the market was closed on that date (weekend / holiday), returns the
    price from the most recent prior trading day that is available in the
    cache.  Returns None if no price exists on or before the requested date.
    """
    if price_type not in VALID_PRICE_TYPES:
        raise ValueError(f"price_type must be one of {sorted(VALID_PRICE_TYPES)}, got {price_type!r}")
    prices = load_ticker_prices(ticker)
    if prices.is_empty():
        return None
    date_str = _coerce_date(on_date).isoformat()
    # Keep rows on or before the requested date, pick the latest one.
    candidates = prices.filter(pl.col("date") <= date_str).sort("date")
    if candidates.is_empty():
        return None
    return float(candidates.row(-1, named=True)[price_type])


def compute_ticker_snapshots(
    account_id: str,
    ticker: str,
    through_date: date | None = None,
) -> list[tuple[date, float]]:
    """Compute portfolio-value snapshots for a ticker account on demand.

    For each trade date (sorted), returns (date, cumulative_shares × close_price).
    Multiple trades on the same date are collapsed into one point.
    A final point is appended for *through_date* (defaults to today).

    Returns an empty list if no cached prices are available.
    This function is intentionally side-effect-free — call it any time without
    worrying about stale stored data.
    """
    if through_date is None:
        through_date = date.today()

    entries = (
        load_entries(account_id)
        .filter(pl.col("shares") != 0.0)
        .sort("entry_time")
    )

    snaps: list[tuple[date, float]] = []
    cumulative_shares = 0.0

    for row in entries.iter_rows(named=True):
        cumulative_shares += row["shares"]
        entry_date = _coerce_date(row["entry_time"])
        close_price = get_ticker_price_on_date(ticker, entry_date, "close")
        if close_price is None:
            continue
        value = cumulative_shares * close_price
        if snaps and snaps[-1][0] == entry_date:
            snaps[-1] = (entry_date, value)   # multiple trades same day → overwrite
        else:
            snaps.append((entry_date, value))

    # Always append the terminal point at through_date.
    final_close = get_ticker_price_on_date(ticker, through_date, "close")
    if final_close is not None:
        final_value = cumulative_shares * final_close
        if snaps and snaps[-1][0] == through_date:
            snaps[-1] = (through_date, final_value)
        else:
            snaps.append((through_date, final_value))

    return snaps


def list_account_groups() -> list[str]:
    """Return sorted list of distinct group_names across all accounts."""
    accounts = load_accounts()
    if accounts.is_empty():
        return []
    return sorted(set(accounts["group_name"].to_list()))


def remove_ticker(ticker: str) -> None:
    """Remove a ticker from the UI (metadata only). Price file is kept on disk
    so re-adding the ticker can refresh from where it left off."""
    meta = load_ticker_metadata().filter(pl.col("ticker") != ticker.upper())
    save_ticker_metadata(meta)
