"""Tests for lib.storage. Each test gets a fresh tmp data dir."""

from __future__ import annotations

from datetime import date, timedelta
import polars as pl
import pytest

from lib import storage


@pytest.fixture(autouse=True)
def fresh_data_dir(tmp_path, monkeypatch):
    """Point storage at a tmp dir for every test, isolated from other tests."""
    storage.set_data_dir(tmp_path / "data")
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _acct(group="G", security="A1", **kw) -> str:
    """Shorthand: add an account and return its ID."""
    return storage.add_account(group, security, **kw)


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------

def test_accounts_empty_initially():
    df = storage.load_accounts()
    assert df.is_empty()
    assert list(df.columns) == list(storage.ACCOUNTS_SCHEMA.keys())


def test_add_account_returns_id_and_persists():
    aid = storage.add_account("Fidelity", "FXAIX")
    assert isinstance(aid, str) and len(aid) > 0

    accounts = storage.load_accounts()
    assert accounts.height == 1
    row = accounts.row(0, named=True)
    assert row["account_id"] == aid
    assert row["group_name"] == "Fidelity"
    assert row["security"] == "FXAIX"
    assert row["is_ticker"] is False
    assert row["ticker"] == ""


def test_add_account_with_ticker():
    aid = storage.add_account("Fidelity", "VOO", is_ticker=True, ticker="VOO")
    row = storage.get_account(aid)
    assert row["is_ticker"] is True
    assert row["ticker"] == "VOO"


def test_add_account_rejects_empty_group():
    with pytest.raises(ValueError, match="Account Group"):
        storage.add_account("   ", "Cash")


def test_add_account_rejects_empty_security():
    with pytest.raises(ValueError, match="Security"):
        storage.add_account("Fidelity", "  ")


def test_add_account_rejects_ticker_without_symbol():
    with pytest.raises(ValueError, match="Ticker symbol"):
        storage.add_account("Fidelity", "Fund", is_ticker=True, ticker="")


def test_add_account_rejects_duplicate_security_within_group():
    storage.add_account("Fidelity", "FXAIX")
    with pytest.raises(ValueError, match="already exists"):
        storage.add_account("Fidelity", "fxaix")   # case-insensitive


def test_add_account_allows_same_security_in_different_groups():
    storage.add_account("Fidelity", "VOO")
    storage.add_account("Vanguard", "VOO")   # same security, different group → OK
    assert storage.load_accounts().height == 2


def test_get_account_returns_none_for_missing():
    assert storage.get_account("does-not-exist") is None


def test_list_account_groups():
    storage.add_account("Fidelity", "FXAIX")
    storage.add_account("Fidelity", "Cash")
    storage.add_account("Vanguard", "VTI")
    assert storage.list_account_groups() == ["Fidelity", "Vanguard"]


def test_remove_account_cascades_entries_and_values():
    aid = _acct()
    storage.add_entry(aid, 1000.0, date(2024, 1, 15))
    storage.set_snapshot(aid, 1100.0, date(2024, 6, 1))

    storage.remove_account(aid)

    assert storage.load_accounts().is_empty()
    assert storage.load_entries().is_empty()
    assert storage.load_snapshots().is_empty()


def test_remove_account_no_cascade_leaves_entries():
    aid = _acct()
    storage.add_entry(aid, 1000.0, date(2024, 1, 15))

    storage.remove_account(aid, cascade=False)
    assert storage.load_accounts().is_empty()
    assert storage.load_entries().height == 1


# ---------------------------------------------------------------------------
# Entries
# ---------------------------------------------------------------------------

def test_add_entry_signed_amount():
    aid = _acct()
    storage.add_entry(aid, 500.0, date(2024, 1, 1))
    storage.add_entry(aid, -200.0, date(2024, 6, 1))

    entries = storage.load_entries(aid)
    amounts = sorted(entries["amount"].to_list())
    assert amounts == [-200.0, 500.0]


def test_add_entry_defaults_shares_and_price():
    aid = _acct()
    storage.add_entry(aid, 500.0, date(2024, 1, 1))
    row = storage.load_entries(aid).row(0, named=True)
    assert row["shares"] == 1.0
    assert row["price_per_share"] == 500.0


def test_add_entry_stores_shares_and_price():
    aid = _acct()
    storage.add_entry(aid, 1000.0, date(2024, 1, 1), shares=10.0, price_per_share=100.0)
    row = storage.load_entries(aid).row(0, named=True)
    assert row["shares"] == 10.0
    assert row["price_per_share"] == 100.0
    assert row["amount"] == 1000.0


def test_add_entry_rejects_invalid_price_per_share():
    aid = _acct()
    with pytest.raises(ValueError, match="price_per_share"):
        storage.add_entry(aid, 0.0, date(2024, 1, 1), shares=10.0, price_per_share=0.0)


def test_add_entry_rejects_partial_shares_args():
    aid = _acct()
    with pytest.raises(ValueError):
        storage.add_entry(aid, 500.0, date(2024, 1, 1), shares=5.0)  # missing price_per_share


def test_add_entry_rejects_unknown_account():
    with pytest.raises(ValueError):
        storage.add_entry("nope", 100.0, date(2024, 1, 1))


def test_add_entry_rejects_future_date():
    aid = _acct()
    future = date.today() + timedelta(days=1)
    with pytest.raises(ValueError):
        storage.add_entry(aid, 100.0, future)


def test_add_entry_rejects_duplicate_entry_time():
    aid = _acct()
    storage.add_entry(aid, 100.0, date(2024, 1, 1))
    with pytest.raises(ValueError, match="entry already exists"):
        storage.add_entry(aid, 200.0, date(2024, 1, 1))


def test_add_entry_merges_into_pure_snapshot_row():
    """Adding a cash flow on a date that already has a pure snapshot merges into it."""
    aid = _acct()
    storage.set_snapshot(aid, 1000.0, date(2024, 1, 1))
    storage.add_entry(aid, 200.0, date(2024, 1, 1))

    entries = storage.load_entries(aid)
    assert entries.height == 1
    row = entries.row(0, named=True)
    assert row["amount"] == 200.0
    assert row["snapshot_value"] == 1000.0


def test_add_entry_accepts_iso_date_string():
    aid = _acct()
    eid = storage.add_entry(aid, 100.0, "2024-03-15")
    df = storage.load_entries(aid)
    assert df.row(0, named=True)["entry_time"] == "2024-03-15"
    assert df.row(0, named=True)["entry_id"] == eid


def test_add_entries_bulk():
    aid = _acct()
    rows = [
        {"account_id": aid, "amount": 100.0, "date": date(2024, 1, 1), "note": "w1"},
        {"account_id": aid, "amount": 100.0, "date": date(2024, 1, 8), "note": "w2"},
        {"account_id": aid, "amount": 100.0, "date": "2024-01-15"},
    ]
    ids = storage.add_entries_bulk(rows)
    assert len(ids) == 3
    assert len(set(ids)) == 3
    df = storage.load_entries(aid)
    assert df.height == 3
    assert df["amount"].sum() == 300.0
    # Bulk entries get default shares=1, price_per_share=amount
    assert (df["shares"] == 1.0).all()


def test_add_entries_bulk_rejects_unknown_account():
    aid = _acct()
    rows = [
        {"account_id": aid, "amount": 100.0, "date": date(2024, 1, 1)},
        {"account_id": "ghost", "amount": 100.0, "date": date(2024, 1, 8)},
    ]
    with pytest.raises(ValueError):
        storage.add_entries_bulk(rows)
    assert storage.load_entries().is_empty()


def test_remove_entries_bulk():
    aid = _acct()
    e1 = storage.add_entry(aid, 100.0, date(2024, 1, 1))
    e2 = storage.add_entry(aid, 200.0, date(2024, 2, 1))
    e3 = storage.add_entry(aid, 300.0, date(2024, 3, 1))

    storage.remove_entries([e1, e3])

    df = storage.load_entries(aid)
    assert df.height == 1
    assert df.row(0, named=True)["entry_id"] == e2


def test_load_entries_filters_by_account():
    a1 = _acct("G", "A1")
    a2 = _acct("G", "A2")
    storage.add_entry(a1, 100.0, date(2024, 1, 1))
    storage.add_entry(a2, 200.0, date(2024, 1, 1))

    assert storage.load_entries(a1).height == 1
    assert storage.load_entries(a2).height == 1
    assert storage.load_entries().height == 2


def test_entries_sorted_by_date():
    aid = _acct()
    storage.add_entry(aid, 100.0, date(2024, 3, 1))
    storage.add_entry(aid, 100.0, date(2024, 1, 1))
    storage.add_entry(aid, 100.0, date(2024, 2, 1))

    dates = storage.load_entries(aid)["entry_time"].to_list()
    assert dates == ["2024-01-01", "2024-02-01", "2024-03-01"]


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def test_set_snapshot_creates_pure_snapshot_row():
    aid = _acct()
    storage.set_snapshot(aid, 1000.0, date(2024, 6, 1))

    snaps = storage.load_snapshots(aid)
    assert snaps.height == 1
    assert snaps.row(0, named=True)["value"] == 1000.0
    assert snaps.row(0, named=True)["as_of_date"] == "2024-06-01"


def test_set_snapshot_replaces_same_date():
    aid = _acct()
    storage.set_snapshot(aid, 1000.0, date(2024, 6, 1))
    storage.set_snapshot(aid, 1100.0, date(2024, 6, 1))

    snaps = storage.load_snapshots(aid)
    assert snaps.height == 1
    assert snaps.row(0, named=True)["value"] == 1100.0


def test_set_snapshot_keeps_other_dates():
    aid = _acct()
    storage.set_snapshot(aid, 1000.0, date(2024, 6, 1))
    storage.set_snapshot(aid, 1200.0, date(2024, 7, 1))

    assert storage.load_snapshots(aid).height == 2



def test_add_entry_with_snapshot_attaches_to_row():
    aid = _acct()
    storage.add_entry(aid, 500.0, date(2024, 3, 1), snapshot_value=1500.0)

    entries = storage.load_entries(aid)
    assert entries.height == 1
    row = entries.row(0, named=True)
    assert row["amount"] == 500.0
    assert row["snapshot_value"] == 1500.0

    snaps = storage.load_snapshots(aid)
    assert snaps.height == 1
    assert snaps.row(0, named=True)["value"] == 1500.0


def test_remove_snapshot_deletes_pure_snapshot_row():
    aid = _acct()
    storage.set_snapshot(aid, 1000.0, date(2024, 6, 1))
    storage.remove_snapshot(aid, date(2024, 6, 1))

    assert storage.load_snapshots(aid).is_empty()
    assert storage.load_entries(aid).is_empty()


def test_remove_snapshot_nulls_fields_on_real_entry():
    aid = _acct()
    storage.add_entry(aid, 500.0, date(2024, 3, 1), snapshot_value=1500.0)
    storage.remove_snapshot(aid, date(2024, 3, 1))

    entries = storage.load_entries(aid)
    assert entries.height == 1
    assert entries.row(0, named=True)["amount"] == 500.0
    assert storage.load_snapshots(aid).is_empty()


# ---------------------------------------------------------------------------
# Ticker price lookup
# ---------------------------------------------------------------------------

def test_get_ticker_price_on_date_returns_correct_price():
    df = pl.DataFrame(
        {"date": ["2024-01-02"], "open": [99.0], "high": [101.0], "low": [98.0], "close": [100.0]},
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    storage.upsert_ticker_prices("VOO", df)
    assert storage.get_ticker_price_on_date("VOO", date(2024, 1, 2), "close") == 100.0
    assert storage.get_ticker_price_on_date("VOO", date(2024, 1, 2), "open") == 99.0
    assert storage.get_ticker_price_on_date("VOO", date(2024, 1, 2), "high") == 101.0
    assert storage.get_ticker_price_on_date("VOO", date(2024, 1, 2), "low") == 98.0


def test_get_ticker_price_on_date_returns_none_for_missing():
    df = pl.DataFrame(
        {"date": ["2024-01-02"], "open": [99.0], "high": [101.0], "low": [98.0], "close": [100.0]},
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    storage.upsert_ticker_prices("VOO", df)
    # No data exists before 2024-01-02
    assert storage.get_ticker_price_on_date("VOO", date(2024, 1, 1)) is None


def test_get_ticker_price_on_date_falls_back_to_previous_trading_day():
    """Weekend / holiday dates should return the most recent prior trading day."""
    df = pl.DataFrame(
        {
            "date":  ["2024-01-05", "2024-01-08"],  # Friday, Monday
            "open":  [100.0, 102.0],
            "high":  [101.0, 103.0],
            "low":   [99.0,  101.0],
            "close": [100.5, 102.5],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    storage.upsert_ticker_prices("VOO", df)
    # Saturday 2024-01-06 → falls back to Friday 2024-01-05
    assert storage.get_ticker_price_on_date("VOO", date(2024, 1, 6)) == 100.5
    # Sunday 2024-01-07 → also falls back to Friday 2024-01-05
    assert storage.get_ticker_price_on_date("VOO", date(2024, 1, 7)) == 100.5
    # Monday itself returns Monday's price
    assert storage.get_ticker_price_on_date("VOO", date(2024, 1, 8)) == 102.5


# ---------------------------------------------------------------------------
# compute_ticker_snapshots
# ---------------------------------------------------------------------------

def _seed_prices(ticker: str, rows: list[tuple[str, float]]) -> None:
    """Seed close prices for a ticker. rows = [(date_str, close), ...]"""
    df = pl.DataFrame(
        {
            "date":  [r[0] for r in rows],
            "open":  [r[1] for r in rows],
            "high":  [r[1] for r in rows],
            "low":   [r[1] for r in rows],
            "close": [r[1] for r in rows],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )
    storage.upsert_ticker_prices(ticker, df)


def test_compute_ticker_snapshots_basic():
    """Buy 10, buy 5, sell 3 → cumulative shares at each trade date correct."""
    acct_id = storage.add_account("G", "VOO Fund", is_ticker=True, ticker="VOO")

    _seed_prices("VOO", [
        ("2024-01-02", 100.0),
        ("2024-01-09", 110.0),
        ("2024-01-16", 120.0),
        ("2024-01-23", 130.0),   # used as through_date
    ])

    storage.add_entry(acct_id, 10 * 100.0, date(2024, 1, 2), shares=10.0, price_per_share=100.0)
    storage.add_entry(acct_id, 5 * 110.0,  date(2024, 1, 9), shares=5.0,  price_per_share=110.0)
    storage.add_entry(acct_id, -3 * 120.0, date(2024, 1, 16), shares=-3.0, price_per_share=120.0)

    result = storage.compute_ticker_snapshots(acct_id, "VOO", through_date=date(2024, 1, 23))

    assert len(result) == 4
    d, v = result[0]; assert d == date(2024, 1, 2)  and abs(v - 10 * 100.0)  < 1e-9
    d, v = result[1]; assert d == date(2024, 1, 9)  and abs(v - 15 * 110.0)  < 1e-9
    d, v = result[2]; assert d == date(2024, 1, 16) and abs(v - 12 * 120.0)  < 1e-9
    d, v = result[3]; assert d == date(2024, 1, 23) and abs(v - 12 * 130.0)  < 1e-9


def test_compute_ticker_snapshots_reflects_deleted_entry():
    """After removing an entry the snapshot recomputes with updated share count."""
    acct_id = storage.add_account("G", "VOO2", is_ticker=True, ticker="VOO2")

    _seed_prices("VOO2", [
        ("2024-01-02", 100.0),
        ("2024-01-09", 110.0),
        ("2024-01-23", 130.0),
    ])

    storage.add_entry(acct_id, 1000.0, date(2024, 1, 2), shares=10.0, price_per_share=100.0)
    eid = storage.add_entry(acct_id, 550.0, date(2024, 1, 9), shares=5.0, price_per_share=110.0)

    result_before = storage.compute_ticker_snapshots(acct_id, "VOO2", through_date=date(2024, 1, 23))
    assert dict(result_before)[date(2024, 1, 23)] == 15 * 130.0  # 15 shares

    storage.remove_entries([eid])
    result_after = storage.compute_ticker_snapshots(acct_id, "VOO2", through_date=date(2024, 1, 23))
    assert dict(result_after)[date(2024, 1, 23)] == 10 * 130.0   # 10 shares — updated


def test_compute_ticker_snapshots_empty_when_no_prices():
    """If no prices are cached the result is an empty list (not an error)."""
    acct_id = storage.add_account("G", "NOPRICE", is_ticker=True, ticker="NOPRICE")
    storage.add_entry(acct_id, 1000.0, date(2024, 1, 2), shares=10.0, price_per_share=100.0)
    assert storage.compute_ticker_snapshots(acct_id, "NOPRICE", through_date=date(2024, 1, 23)) == []



# ---------------------------------------------------------------------------
# Migrations
# ---------------------------------------------------------------------------

def test_migration_accounts_schema(tmp_path):
    storage.set_data_dir(tmp_path / "data_m")
    (tmp_path / "data_m").mkdir(parents=True)

    # Write old-schema accounts.csv
    old = pl.DataFrame(
        {
            "account_id": ["abc"],
            "name": ["FXAIX"],
            "description": ["Fidelity"],
            "created_at": ["2024-01-01T00:00:00"],
        }
    )
    old.write_csv(storage.ACCOUNTS_PATH)

    accounts = storage.load_accounts()
    assert list(accounts.columns) == list(storage.ACCOUNTS_SCHEMA.keys())
    row = accounts.row(0, named=True)
    assert row["security"] == "FXAIX"
    assert row["group_name"] == "Fidelity"
    assert row["is_ticker"] is False
    assert row["ticker"] == ""


def test_migration_entries_add_share_columns(tmp_path):
    storage.set_data_dir(tmp_path / "data_e")
    (tmp_path / "data_e").mkdir(parents=True)

    aid = _acct()
    # Write old-schema entries.csv (without shares/price_per_share)
    old = pl.DataFrame({
        "entry_id": ["x"], "account_id": [aid], "amount": [500.0],
        "entry_time": ["2024-01-01"], "note": [""], "snapshot_value": [None],
    })
    old.write_csv(storage.ENTRIES_PATH)

    entries = storage.load_entries(aid)
    assert "shares" in entries.columns
    assert "price_per_share" in entries.columns
    row = entries.row(0, named=True)
    assert row["shares"] == 1.0
    assert row["price_per_share"] == 500.0


def test_migration_from_legacy_current_values(tmp_path, monkeypatch):
    """Legacy current_values.csv is merged into entries on first load."""
    storage.set_data_dir(tmp_path / "data2")
    (tmp_path / "data2").mkdir(parents=True)

    aid = _acct()
    storage.add_entry(aid, 1000.0, date(2023, 1, 1))

    legacy = pl.DataFrame(
        {"account_id": [aid], "value": [1100.0], "as_of_date": ["2023-06-01"]},
        schema=storage._LEGACY_CURRENT_VALUES_SCHEMA,
    )
    legacy.write_csv(storage._LEGACY_CURRENT_VALUES_PATH)

    entries = storage.load_entries(aid)
    assert not storage._LEGACY_CURRENT_VALUES_PATH.exists()
    snaps = storage.load_snapshots(aid)
    assert snaps.height == 1
    assert snaps.row(0, named=True)["value"] == 1100.0


# ---------------------------------------------------------------------------
# Ticker prices
# ---------------------------------------------------------------------------

def _sample_prices(dates_and_close: list[tuple[str, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date":  [d for d, _ in dates_and_close],
            "open":  [c for _, c in dates_and_close],
            "high":  [c for _, c in dates_and_close],
            "low":   [c for _, c in dates_and_close],
            "close": [c for _, c in dates_and_close],
        },
        schema=storage.TICKER_PRICES_SCHEMA,
    )


def test_upsert_ticker_prices_adds_metadata():
    df = _sample_prices([("2024-01-02", 100.0), ("2024-01-03", 101.0)])
    storage.upsert_ticker_prices("VOO", df)

    meta = storage.get_ticker_metadata("VOO")
    assert meta is not None
    assert meta["ticker"] == "VOO"
    assert meta["earliest_date"] == "2024-01-02"
    assert meta["latest_date"] == "2024-01-03"
    assert meta["price_type"] == "close"
    assert meta["close_only"] is False


def test_upsert_ticker_prices_dedupes_on_date():
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 100.0)]))
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 999.0)]))

    df = storage.load_ticker_prices("VOO")
    assert df.height == 1
    assert df.row(0, named=True)["close"] == 999.0


def test_upsert_ticker_prices_extends_range():
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 100.0)]))
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-03", 101.0)]))

    meta = storage.get_ticker_metadata("VOO")
    assert meta["earliest_date"] == "2024-01-02"
    assert meta["latest_date"] == "2024-01-03"


def test_set_ticker_price_type_preserves_close_only_flag():
    storage.upsert_ticker_prices("FXAIX", _sample_prices([("2024-01-02", 100.0)]))
    storage.upsert_ticker_metadata("FXAIX", close_only=True, price_type="close")

    storage.set_ticker_price_type("FXAIX", "close")
    meta = storage.get_ticker_metadata("FXAIX")
    assert meta["close_only"] is True
    assert meta["price_type"] == "close"


def test_set_ticker_price_type_validates():
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 100.0)]))
    with pytest.raises(ValueError):
        storage.set_ticker_price_type("VOO", "midprice")


def test_list_cached_tickers_excludes_metadata():
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 100.0)]))
    storage.upsert_ticker_prices("FXAIX", _sample_prices([("2024-01-02", 50.0)]))

    cached = storage.list_cached_tickers()
    assert cached == ["FXAIX", "VOO"]


def test_remove_ticker_removes_metadata_keeps_price_file():
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 100.0)]))
    storage.upsert_ticker_metadata("VOO", price_type="close", close_only=False)
    storage.remove_ticker("VOO")

    assert storage.get_ticker_metadata("VOO") is None
    assert not storage.load_ticker_prices("VOO").is_empty()
    assert "VOO" in storage.list_cached_tickers()


# ---------------------------------------------------------------------------
# Round-trip through CSV
# ---------------------------------------------------------------------------

def test_full_round_trip_through_csv():
    a1 = storage.add_account("Fidelity", "FXAIX")
    a2 = storage.add_account("Vanguard", "VTI")
    storage.add_entry(a1, 1000.0, date(2023, 1, 1), "initial")
    storage.add_entry(a1, -100.0, date(2023, 6, 1), "withdrawal")
    storage.add_entry(a2, 5000.0, date(2023, 2, 1))
    storage.set_snapshot(a1, 1100.0, date(2024, 1, 1))
    storage.set_snapshot(a2, 5500.0, date(2024, 1, 1))
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 450.0)]))
    storage.upsert_ticker_metadata("VOO", close_only=False, price_type="close")

    accounts = storage.load_accounts()
    entries = storage.load_entries()
    snaps_a1 = storage.load_snapshots(a1)
    snaps_a2 = storage.load_snapshots(a2)
    voo = storage.load_ticker_prices("VOO")
    meta = storage.load_ticker_metadata()

    assert accounts.height == 2
    assert entries.height == 5   # 3 cash-flow entries + 2 pure-snapshot rows
    assert snaps_a1.height == 1
    assert snaps_a2.height == 1
    assert voo.height == 1
    assert meta.height == 1
    assert meta.row(0, named=True)["ticker"] == "VOO"
