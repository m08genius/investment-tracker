# Investment Tracker — Design Document

A local-first app for tracking investment accounts, recording cash flows, and
comparing performance against market index tickers using Money-Weighted Rate
of Return (MWRR / XIRR).

This document captures all design decisions made during planning. It is the
source of truth for v1 and the handoff doc if the build moves to a different
tool or session.

---

## 1. Goals and scope

### v1 (this build)

- Track multiple investment accounts (each is a name + description).
- Record cash flows per account: deposits and withdrawals, with dates.
- Support recurring deposits (weekly, biweekly, semi-monthly, monthly).
- Record current market values for accounts (snapshots over time).
- Compute MWRR per account and an aggregate across all accounts.
- Compare MWRR to one or more market index tickers (e.g. FXAIX, VOO).
- Cache ticker price history locally to handle rate limits and offline use.
- Run locally on the user's machine.

### Deferred to v2 (designed for, not built)

- Time-Weighted Rate of Return (TWRR) and/or Modified Dietz.
- Per-position holdings tracking (individual securities within an account).
- Multi-currency support / FX conversion.

The v1 storage schema is forward-compatible with TWRR, no migration will
be required.

---

## 2. Technology stack

| Concern               | Choice                       | Rationale                                                     |
| --------------------- | ---------------------------- | ------------------------------------------------------------- |
| Language              | Python 3.12                  | Best ecosystem for this combo                                 |
| UI                    | Streamlit                    | Multi-page apps from pure Python; minimal boilerplate         |
| Data manipulation     | Polars                       | Faster than pandas, cleaner expression API                    |
| Storage               | CSV files (one per table)    | Inspectable by hand; portable; right-sized for personal use   |
| Ticker data           | yfinance                     | Free, no API key; covers ETFs and mutual funds                |
| Numerical math        | scipy + numpy_financial      | XIRR via Brent's method or numpy_financial.xirr               |
| Dev environment       | conda (`environment.yml`)    | Rapid iteration, matches user's existing workflow             |
| Deployment (later)    | Docker + docker-compose      | Always-on local service when v1 is stable                     |

### Why CSV not SQLite

For a single-user personal tool, CSV's transparency (open in Excel, edit by
hand, diff in git) outweighs SQLite's transactional guarantees. Data volumes
are small (hundreds of entries, low thousands of cached ticker prices). All
file I/O goes through `lib/storage.py`, so swapping to SQLite later would be a
single-file change.

### Why Polars not pandas

Cleaner API for the kinds of operations we do (filter, concat, unique, sort),
faster on cold starts, and the expression syntax composes better.

### Why conda for dev, Docker for deployment

Conda gives fast local iteration in an env the user is already comfortable
with. Docker is reserved for "always-on" deployment later, where a slim
Python base image (not a conda image) keeps the container small.

---

## 3. Project structure

```
investment-tracker/
├── environment.yml             # conda env definition
├── DESIGN.md                   # this file
├── README.md                   # quickstart for end user
├── app.py                      # Streamlit entry point (home page)
├── pages/
│   ├── 1_Accounts.py           # add/remove accounts and entries
│   ├── 2_View_Performance.py   # MWRR + ticker comparison + aggregate
│   └── 3_Ticker_Data.py        # ticker cache management
├── lib/
│   ├── __init__.py
│   ├── storage.py              # all CSV I/O (the only place that touches files)
│   ├── returns.py              # MWRR / XIRR (named for future TWRR addition)
│   ├── tickers.py              # yfinance fetching + close-only detection
│   └── simulation.py           # simulate buying/selling a ticker on cash flow dates
├── tests/
│   ├── test_storage.py
│   ├── test_returns.py
│   └── test_simulation.py
└── data/                       # gitignored; created at runtime
    ├── accounts.csv
    ├── entries.csv
    ├── current_values.csv
    └── tickers/
        ├── _metadata.csv
        ├── FXAIX.csv
        └── VOO.csv
```

---

## 4. Storage schemas

All files are CSV with headers. Dates are ISO 8601 strings (`YYYY-MM-DD`).
Timestamps are ISO 8601 with seconds precision.

All writes are atomic: write to `<path>.tmp`, then `os.replace()` to final
path. This prevents corruption if the process crashes mid-write.

### `accounts.csv`

| Column        | Type   | Notes                                |
| ------------- | ------ | ------------------------------------ |
| account_id    | str    | uuid4                                |
| name          | str    | Unique (case-insensitive)            |
| description   | str    | Optional, may be empty               |
| created_at    | str    | ISO 8601 timestamp                   |

### `entries.csv`

| Column      | Type   | Notes                                                  |
| ----------- | ------ | ------------------------------------------------------ |
| entry_id    | str    | uuid4                                                  |
| account_id  | str    | FK to accounts.account_id                              |
| amount      | float  | **Signed**: positive = deposit, negative = withdrawal  |
| date        | str    | ISO 8601 date                                          |
| note        | str    | Optional                                               |

The signed-amount convention matches XIRR's convention exactly. Withdrawals
are stored as negative numbers, but the UI uses a Deposit/Withdrawal selector
+ positive amount field; the conversion to signed happens in the UI layer
before calling `storage.add_entry()`.

### `current_values.csv`

| Column      | Type   | Notes                                       |
| ----------- | ------ | ------------------------------------------- |
| account_id  | str    | FK to accounts.account_id                   |
| value       | float  | Market value, **net of withdrawals to date** |
| as_of_date  | str    | ISO 8601 date                               |

Multiple snapshots per account are supported (one per `(account_id, as_of_date)`
pair). v1 mostly uses just the latest snapshot for MWRR; v2 will use the full
series for TWRR.

### `tickers/{TICKER}.csv`

| Column | Type   | Notes                                                              |
| ------ | ------ | ------------------------------------------------------------------ |
| date   | str    | ISO 8601 date                                                      |
| open   | float  | For close-only tickers (mutual funds), open == high == low == close |
| high   | float  |                                                                    |
| low    | float  |                                                                    |
| close  | float  | Adjusted close from yfinance                                       |

Filenames are uppercase ticker symbols. Always store all four OHLC columns,
even for close-only tickers, to keep the schema uniform.

### `tickers/_metadata.csv`

| Column          | Type   | Notes                                                            |
| --------------- | ------ | ---------------------------------------------------------------- |
| ticker          | str    | Uppercase                                                        |
| last_refreshed  | str    | ISO 8601 timestamp                                               |
| earliest_date   | str    | Earliest date in cached prices                                   |
| latest_date     | str    | Latest date in cached prices                                     |
| price_type      | str    | One of: open, high, low, close                                   |
| close_only      | bool   | True if open/high/low were detected as duplicates of close       |

---

## 5. Conventions

| Topic                          | Convention                                                      |
| ------------------------------ | --------------------------------------------------------------- |
| Currency                       | USD only; no FX                                                 |
| Cash flow signs                | Positive = deposit, negative = withdrawal                       |
| Current value                  | Net of all withdrawals to date                                  |
| Dividends / interest           | Folded into current value, not tracked as separate entries      |
| Future-dated entries           | Not allowed; recurring expansion caps end date at today         |
| Non-trading days               | Cash flow date stays as entered; ticker simulation uses next available trading day's price |
| Day-of-month edge cases (EOM)  | Monthly on 31st in February → use last day of month             |
| Default price type             | Close (and the only choice for close-only tickers)              |
| First-fetch start date         | 2012-10-01                                                      |

---

## 6. MWRR (Money-Weighted Rate of Return)

### Formula

MWRR is XIRR: solve for `r` in

```
sum over i of (CF_i / (1 + r) ** ((d_i - d_0) / 365)) = 0
```

where `CF_i` is cash flow `i` on date `d_i`, `d_0` is the first date.

### Cash flow series for an account

For an account with entries E and a current value V on date D:

```
flows = [(date_i, amount_i) for entry_i in E]
flows.append((D, V))   # final positive flow representing current value
```

The signs work out: deposits and current value are positive (money "out"
from XIRR's perspective if you treat the investment as the counterparty),
withdrawals are negative. Convention is consistent and matches numpy_financial.

### Implementation

- Primary: `numpy_financial.xirr(values, dates)` if it works.
- Fallback: scipy `brentq` with bracketing on `[-0.99, 100]` for robustness.
- Catch convergence failures and `ValueError` exceptions; return `None`.
- UI displays `None` as `"—"` with an explanatory tooltip.

### Aggregate MWRR

The aggregate is computed by unioning all accounts' cash flow series:

```
all_flows = concat(per-account flows)
aggregate_value = sum(per-account current values)
all_flows.append((today, aggregate_value))
```

**Strict gating**: aggregate is shown only if every account has a current
value entered. Otherwise display "Aggregate unavailable: 2 of 3 accounts
missing current value" or similar.

### Sanity / edge cases

- All-same-sign cash flows have no XIRR solution → return None.
- Single-entry accounts with no current value → return None.
- Convergence failure → return None.
- Result wildly out of range (e.g. > 1000% or < -99%) → return value but flag as suspect.

---

## 7. Ticker comparison (MWRR against an index)

### Question being answered

"What MWRR would I have gotten if I'd put the same money in this ticker on
the same days?"

### Simulation

Given an account's entries and a ticker's price history:

1. Initialize `shares_held = 0`.
2. For each entry on date D with amount A (signed):
   - Look up `price(D, price_type)` (with next-trading-day fallback if D is
     not a trading day).
   - `shares_held += A / price(D)`.  (Positive A buys shares, negative A
     reduces shares.)
3. At the valuation date `D_v` (today, or the as-of date of the current value):
   - `simulated_value = shares_held * price(D_v)`.
4. Construct a cash flow series: same dates and amounts as the account's
   entries, but with a final flow of `(D_v, simulated_value)`.
5. Compute XIRR on that series.

### Edge cases

- **Negative shares**: if a withdrawal exceeds simulated holdings, shares go
  negative. Allow it mathematically; surface a warning ("simulated shares
  went negative — withdrawal exceeded simulated value at that date").
- **Cache gap**: if the ticker cache doesn't cover the earliest entry date,
  the View page must prompt the user to refresh ticker data before computing.
- **Zero cash flows**: if an account has no entries, no comparison is possible.

### Price type per ticker

Each ticker has a configured `price_type` in `_metadata.csv`. The simulation
uses that column from the ticker price file. For close-only tickers
(`close_only = true`), `price_type` is locked to "close".

---

## 8. Ticker data fetching (yfinance)

### Library

`yfinance.Ticker(symbol).history(start, end, auto_adjust=True)` returns a
pandas DataFrame with Open/High/Low/Close columns. We convert to polars before
storing.

### First-time fetch

When user adds a new ticker:

1. Fetch from `2012-10-01` through today.
2. Detect close-only: check if `open == close` and `high == close` and
   `low == close` for the most recent 30 rows. If yes, set
   `close_only = true` and `price_type = "close"` in metadata. If no,
   `close_only = false` and prompt user to choose price type (default close).
3. Store all four OHLC columns regardless (uniform schema).
4. Update `_metadata.csv`.

### Refresh (intelligent gap-fill)

When user clicks Refresh on an existing ticker:

1. Read `latest_date` from `_metadata.csv`.
2. Fetch from `latest_date + 1 day` through today.
3. Upsert into `tickers/{TICKER}.csv` (`storage.upsert_ticker_prices`
   handles dedup on date).
4. Update `last_refreshed` and `latest_date` in metadata.

If `latest_date` is already today, the fetch is a no-op.

### Rate limit handling

yfinance has unpublished but real rate limits. Strategy:

- Cache aggressively (the whole point).
- On `requests.exceptions.HTTPError` (429 or similar), surface error to
  user with "try again in a minute" message.
- Don't retry automatically inside one click.

### Failure handling

- Network failure → user sees error, no partial writes.
- Empty response → don't update cache, surface "no new data" message.
- Yahoo returns malformed/empty frame → don't update cache.

---

## 9. Pages and UI behavior

### Home (`app.py`)

Brief intro, link to other pages, summary count of accounts and tickers
cached.

### Accounts page (`pages/1_Accounts.py`)

- List of existing accounts with delete button (cascade deletes entries and
  current value snapshots; require confirmation).
- "Add account" form: name + description.
- Account selector to drill into one account.

For the selected account:
- **Add single entry** form:
  - Type selector: Deposit / Withdrawal
  - Amount: positive number
  - Date: date picker, default today, max = today
  - Note: optional
  - On submit: convert to signed amount and call `storage.add_entry()`
- **Add recurring deposits** form:
  - Frequency: Weekly / Biweekly / Semi-monthly / Monthly
  - For weekly/biweekly: day-of-week selector
  - For monthly: day-of-month selector (1–31, with EOM convention noted)
  - For semi-monthly: fixed (1st and 15th)
  - Amount: positive number
  - Start date, end date (capped at today)
  - Note: optional, applied to all generated entries
  - "Preview" button shows how many entries will be created and the total
    amount.
  - "Confirm" button writes all entries.
- **Entries table**: sorted by date descending, with checkbox column. Show
  withdrawals visually distinct (red, minus sign, "Withdrawal" label).
  "Delete selected" button below the table.

### View Performance page (`pages/2_View_Performance.py`)

- For each account, show:
  - Most recent current value snapshot (with date)
  - "Update current value" form (value + as-of-date, default today)
- "Compare against tickers" section:
  - Multi-select of cached tickers
  - "Add new ticker" link → directs to Ticker Data page
- **Comparison table**:
  - Rows: each account, plus an "All Accounts (Aggregate)" row
  - Columns: Account MWRR, then one column per selected ticker
    showing the simulated MWRR for that ticker
  - Cells with no result show "—" with a tooltip explaining why
- Aggregate row: shown only if all accounts have current values; otherwise
  shown grayed out with explanation.
- Cache-gap detection: if the earliest entry date across selected
  accounts/tickers predates the ticker's `earliest_date`, show a banner:
  "Need TICKER prices from YYYY-MM-DD. Refresh ticker data?" with a link to
  the Ticker Data page.

### Ticker Data page (`pages/3_Ticker_Data.py`)

- Table of cached tickers showing: ticker, last refreshed, date range,
  price type, close-only flag.
- Per-row: Refresh button, Remove button, price type dropdown (disabled if
  close-only).
- "Add new ticker" form: ticker symbol input, fetches from 2012-10-01 on
  submit, runs close-only detection, prompts for price type if not close-only.
- Global "Refresh all" button.

---

## 10. `lib/storage.py` API surface

All file I/O goes through this module. Other modules and pages must not read
or write CSV files directly.

```python
# Accounts
load_accounts() -> pl.DataFrame
save_accounts(df: pl.DataFrame) -> None
add_account(name: str, description: str = "") -> str  # returns account_id
remove_account(account_id: str, *, cascade: bool = True) -> None
get_account(account_id: str) -> dict | None

# Entries
load_entries(account_id: str | None = None) -> pl.DataFrame
save_entries(df: pl.DataFrame) -> None
add_entry(account_id: str, amount: float, entry_date: date | str, note: str = "") -> str
remove_entry(entry_id: str) -> None
remove_entries(entry_ids: list[str]) -> None  # bulk delete

# Current values (snapshots)
load_current_values(account_id: str | None = None) -> pl.DataFrame
save_current_values(df: pl.DataFrame) -> None
set_current_value(account_id: str, value: float, as_of_date: date | str | None = None) -> None
get_latest_current_value(account_id: str) -> dict | None

# Ticker prices
load_ticker_prices(ticker: str) -> pl.DataFrame
save_ticker_prices(ticker: str, df: pl.DataFrame) -> None
upsert_ticker_prices(ticker: str, new_df: pl.DataFrame) -> None
list_cached_tickers() -> list[str]
remove_ticker(ticker: str) -> None

# Ticker metadata
load_ticker_metadata() -> pl.DataFrame
save_ticker_metadata(df: pl.DataFrame) -> None
get_ticker_metadata(ticker: str) -> dict | None
set_ticker_price_type(ticker: str, price_type: str) -> None
```

---

## 11. `lib/returns.py` API

```python
def compute_mwrr(
    cash_flows: list[tuple[date, float]],
    current_value: float,
    valuation_date: date,
) -> float | None:
    """
    Compute MWRR (XIRR) given a list of (date, signed_amount) cash flows
    plus a final current value on the valuation date.

    Returns annualized rate as a decimal (e.g. 0.085 = 8.5%), or None on
    convergence failure or insufficient data.
    """

# Future v2 additions:
# def compute_twrr(...)
# def compute_modified_dietz(...)
```

---

## 12. `lib/tickers.py` API

```python
def fetch_ticker_history(
    ticker: str,
    start: date,
    end: date,
) -> pl.DataFrame:
    """Fetch OHLC from yfinance. Raises on network/API errors."""

def detect_close_only(prices: pl.DataFrame, lookback_rows: int = 30) -> bool:
    """True if open == high == low == close in recent rows."""

def add_or_refresh_ticker(ticker: str, *, force_full_refresh: bool = False) -> dict:
    """
    Add a new ticker (full fetch from 2012-10-01) or refresh existing.
    Returns metadata dict for the ticker.
    """
```

---

## 13. `lib/simulation.py` API

```python
def simulate_ticker_position(
    cash_flows: list[tuple[date, float]],
    ticker_prices: pl.DataFrame,
    price_column: str,
) -> tuple[float, list[str]]:
    """
    Simulate buying/selling shares of a ticker on the given cash flow dates.
    Returns (final_shares_held, warnings).
    Uses next-trading-day fallback for non-trading-day cash flow dates.
    """

def compute_ticker_comparison_mwrr(
    cash_flows: list[tuple[date, float]],
    ticker: str,
    valuation_date: date,
) -> tuple[float | None, list[str]]:
    """
    End-to-end: load ticker prices and metadata, simulate position,
    compute MWRR on the simulated cash flow series. Returns (rate, warnings).
    """
```

---

## 14. Build order

0. `environment.yml` (conda env definition)
1. `lib/__init__.py` + `lib/storage.py` + tests
2. `lib/returns.py` + tests
3. `lib/tickers.py` + tests (with mocked yfinance for unit tests)
4. `lib/simulation.py` + tests
5. `pages/1_Accounts.py` (add/remove account, single entry, then recurring, then bulk delete)
6. `pages/3_Ticker_Data.py` (cache management)
7. `pages/2_View_Performance.py` (per-account, then aggregate, then comparison)
8. `app.py` (home page) and `README.md`
9. *(later)* `Dockerfile` + `docker-compose.yml`

---

## 15. Testing strategy

- **Storage**: round-trip tests (write → read → assert equal); atomic-write
  verification; cascade delete behavior.
- **Returns**: known-answer tests against published XIRR examples; edge
  cases (single flow, all-same-sign, convergence failure).
- **Simulation**: deterministic mock price frames; verify next-trading-day
  fallback; verify negative-share warnings.
- **Tickers**: mocked yfinance responses; close-only detection; gap-fill
  refresh logic.
- Streamlit pages are not unit tested (UI smoke tests are the user's
  job in v1).

---

## 16. Open issues / known limitations

- yfinance is unofficial and occasionally breaks when Yahoo changes their
  internal API. If it stops working, the alternative is Stooq or Alpha
  Vantage (free tier, requires key).
- Linear-interpolated TWRR (deferred to v2) is approximate, not
  brokerage-grade.
- No multi-currency support. All values assumed USD.
- No backups beyond what the user manages (CSV file copies).
