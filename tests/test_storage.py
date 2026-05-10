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
# Accounts
# ---------------------------------------------------------------------------

def test_accounts_empty_initially():
    df = storage.load_accounts()
    assert df.is_empty()
    assert list(df.columns) == list(storage.ACCOUNTS_SCHEMA.keys())


def test_add_account_returns_id_and_persists():
    aid = storage.add_account("Brokerage", "Main taxable account")
    assert isinstance(aid, str) and len(aid) > 0

    accounts = storage.load_accounts()
    assert accounts.height == 1
    row = accounts.row(0, named=True)
    assert row["account_id"] == aid
    assert row["name"] == "Brokerage"
    assert row["description"] == "Main taxable account"


def test_add_account_rejects_empty_name():
    with pytest.raises(ValueError):
        storage.add_account("   ")


def test_add_account_rejects_duplicate_name_case_insensitive():
    storage.add_account("Roth IRA")
    with pytest.raises(ValueError):
        storage.add_account("roth ira")


def test_get_account_returns_none_for_missing():
    assert storage.get_account("does-not-exist") is None


def test_remove_account_cascades_entries_and_values():
    aid = storage.add_account("A1")
    storage.add_entry(aid, 1000.0, date(2024, 1, 15))
    storage.set_current_value(aid, 1100.0, date(2024, 6, 1))

    storage.remove_account(aid)

    assert storage.load_accounts().is_empty()
    assert storage.load_entries().is_empty()
    assert storage.load_current_values().is_empty()


def test_remove_account_no_cascade_leaves_entries():
    aid = storage.add_account("A1")
    storage.add_entry(aid, 1000.0, date(2024, 1, 15))

    storage.remove_account(aid, cascade=False)
    assert storage.load_accounts().is_empty()
    # Entry remains orphaned (intentional with cascade=False)
    assert storage.load_entries().height == 1


# ---------------------------------------------------------------------------
# Entries
# ---------------------------------------------------------------------------

def test_add_entry_signed_amount():
    aid = storage.add_account("A1")
    storage.add_entry(aid, 500.0, date(2024, 1, 1))      # deposit
    storage.add_entry(aid, -200.0, date(2024, 6, 1))     # withdrawal

    entries = storage.load_entries(aid)
    amounts = sorted(entries["amount"].to_list())
    assert amounts == [-200.0, 500.0]


def test_add_entry_rejects_unknown_account():
    with pytest.raises(ValueError):
        storage.add_entry("nope", 100.0, date(2024, 1, 1))


def test_add_entry_rejects_future_date():
    aid = storage.add_account("A1")
    future = date.today() + timedelta(days=1)
    with pytest.raises(ValueError):
        storage.add_entry(aid, 100.0, future)


def test_add_entry_accepts_iso_date_string():
    aid = storage.add_account("A1")
    eid = storage.add_entry(aid, 100.0, "2024-03-15")
    df = storage.load_entries(aid)
    assert df.row(0, named=True)["date"] == "2024-03-15"
    assert df.row(0, named=True)["entry_id"] == eid


def test_add_entries_bulk():
    aid = storage.add_account("A1")
    rows = [
        {"account_id": aid, "amount": 100.0, "date": date(2024, 1, 1), "note": "w1"},
        {"account_id": aid, "amount": 100.0, "date": date(2024, 1, 8), "note": "w2"},
        {"account_id": aid, "amount": 100.0, "date": "2024-01-15"},  # str date, no note
    ]
    ids = storage.add_entries_bulk(rows)
    assert len(ids) == 3
    assert len(set(ids)) == 3   # all unique
    df = storage.load_entries(aid)
    assert df.height == 3
    assert df["amount"].sum() == 300.0


def test_add_entries_bulk_rejects_unknown_account():
    aid = storage.add_account("A1")
    rows = [
        {"account_id": aid, "amount": 100.0, "date": date(2024, 1, 1)},
        {"account_id": "ghost", "amount": 100.0, "date": date(2024, 1, 8)},
    ]
    with pytest.raises(ValueError):
        storage.add_entries_bulk(rows)
    # Atomic: no entries should have been written
    assert storage.load_entries().is_empty()


def test_remove_entries_bulk():
    aid = storage.add_account("A1")
    e1 = storage.add_entry(aid, 100.0, date(2024, 1, 1))
    e2 = storage.add_entry(aid, 200.0, date(2024, 2, 1))
    e3 = storage.add_entry(aid, 300.0, date(2024, 3, 1))

    storage.remove_entries([e1, e3])

    df = storage.load_entries(aid)
    assert df.height == 1
    assert df.row(0, named=True)["entry_id"] == e2


def test_load_entries_filters_by_account():
    a1 = storage.add_account("A1")
    a2 = storage.add_account("A2")
    storage.add_entry(a1, 100.0, date(2024, 1, 1))
    storage.add_entry(a2, 200.0, date(2024, 1, 1))

    assert storage.load_entries(a1).height == 1
    assert storage.load_entries(a2).height == 1
    assert storage.load_entries().height == 2


def test_entries_sorted_by_date():
    aid = storage.add_account("A1")
    storage.add_entry(aid, 100.0, date(2024, 3, 1))
    storage.add_entry(aid, 100.0, date(2024, 1, 1))
    storage.add_entry(aid, 100.0, date(2024, 2, 1))

    dates = storage.load_entries(aid)["date"].to_list()
    assert dates == ["2024-01-01", "2024-02-01", "2024-03-01"]


# ---------------------------------------------------------------------------
# Current values
# ---------------------------------------------------------------------------

def test_set_current_value_replaces_same_date():
    aid = storage.add_account("A1")
    storage.set_current_value(aid, 1000.0, date(2024, 6, 1))
    storage.set_current_value(aid, 1100.0, date(2024, 6, 1))   # replace

    df = storage.load_current_values(aid)
    assert df.height == 1
    assert df.row(0, named=True)["value"] == 1100.0


def test_set_current_value_keeps_other_dates():
    aid = storage.add_account("A1")
    storage.set_current_value(aid, 1000.0, date(2024, 6, 1))
    storage.set_current_value(aid, 1200.0, date(2024, 7, 1))

    df = storage.load_current_values(aid)
    assert df.height == 2


def test_get_latest_current_value():
    aid = storage.add_account("A1")
    storage.set_current_value(aid, 1000.0, date(2024, 6, 1))
    storage.set_current_value(aid, 1200.0, date(2024, 7, 1))
    storage.set_current_value(aid, 1100.0, date(2024, 5, 1))

    latest = storage.get_latest_current_value(aid)
    assert latest is not None
    assert latest["as_of_date"] == "2024-07-01"
    assert latest["value"] == 1200.0


def test_get_latest_current_value_none_for_no_snapshots():
    aid = storage.add_account("A1")
    assert storage.get_latest_current_value(aid) is None


# ---------------------------------------------------------------------------
# Ticker prices
# ---------------------------------------------------------------------------

def _sample_prices(dates_and_close: list[tuple[str, float]]) -> pl.DataFrame:
    """Build a sample OHLC frame where O=H=L=C for simplicity."""
    return pl.DataFrame(
        {
            "date": [d for d, _ in dates_and_close],
            "open": [c for _, c in dates_and_close],
            "high": [c for _, c in dates_and_close],
            "low": [c for _, c in dates_and_close],
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
    # Defaults set on first insert
    assert meta["price_type"] == "close"
    assert meta["close_only"] is False


def test_upsert_ticker_prices_dedupes_on_date():
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 100.0)]))
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 999.0)]))

    df = storage.load_ticker_prices("VOO")
    assert df.height == 1
    assert df.row(0, named=True)["close"] == 999.0   # new row wins


def test_upsert_ticker_prices_extends_range():
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 100.0)]))
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-03", 101.0)]))

    meta = storage.get_ticker_metadata("VOO")
    assert meta["earliest_date"] == "2024-01-02"
    assert meta["latest_date"] == "2024-01-03"


def test_set_ticker_price_type_preserves_close_only_flag():
    storage.upsert_ticker_prices("FXAIX", _sample_prices([("2024-01-02", 100.0)]))
    storage.upsert_ticker_metadata("FXAIX", close_only=True, price_type="close")

    storage.set_ticker_price_type("FXAIX", "close")  # idempotent for mutual fund
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
    assert cached == ["FXAIX", "VOO"]   # sorted, no _metadata


def test_remove_ticker_deletes_file_and_metadata():
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 100.0)]))
    storage.remove_ticker("VOO")

    assert storage.list_cached_tickers() == []
    assert storage.get_ticker_metadata("VOO") is None


# ---------------------------------------------------------------------------
# Round-trip through CSV
# ---------------------------------------------------------------------------

def test_full_round_trip_through_csv():
    """Write a bunch of stuff, simulate a fresh load, verify everything reads back."""
    a1 = storage.add_account("Brokerage", "main")
    a2 = storage.add_account("Roth")
    storage.add_entry(a1, 1000.0, date(2023, 1, 1), "initial")
    storage.add_entry(a1, -100.0, date(2023, 6, 1), "withdrawal")
    storage.add_entry(a2, 5000.0, date(2023, 2, 1))
    storage.set_current_value(a1, 1100.0, date(2024, 1, 1))
    storage.set_current_value(a2, 5500.0, date(2024, 1, 1))
    storage.upsert_ticker_prices("VOO", _sample_prices([("2024-01-02", 450.0)]))
    storage.upsert_ticker_metadata("VOO", close_only=False, price_type="close")

    # The set_data_dir trick: nothing changes, but reload everything fresh.
    accounts = storage.load_accounts()
    entries = storage.load_entries()
    cv = storage.load_current_values()
    voo = storage.load_ticker_prices("VOO")
    meta = storage.load_ticker_metadata()

    assert accounts.height == 2
    assert entries.height == 3
    assert cv.height == 2
    assert voo.height == 1
    assert meta.height == 1
    assert meta.row(0, named=True)["ticker"] == "VOO"
