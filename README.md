# Investment Tracker

A local-first app for tracking investment accounts, recording cash flows
(deposits and withdrawals), and comparing performance against market index
tickers using the Money-Weighted Rate of Return (MWRR / XIRR).

Everything runs on your machine. Data is stored in CSV files in `data/`.

## Quickstart

You need [conda](https://docs.conda.io/) (or mamba/miniforge — anything that
reads `environment.yml`).

```bash
# Create the environment
conda env create -f environment.yml

# Activate it
conda activate investment-tracker

# Run the app
streamlit run app.py
```

Streamlit will open the app at <http://localhost:8501> in your browser.

## Workflow

1. **Add an account** on the *Accounts* page (e.g. "Brokerage", "Roth IRA").
2. **Add cash flows** — single deposits/withdrawals, or set up recurring
   deposits (weekly, biweekly, semi-monthly, or monthly).
3. **Add a ticker** on the *Ticker Data* page (e.g. `VOO`, `FXAIX`, `SPY`).
   The first fetch grabs history from October 2012 through today.
4. **Enter your current account values** on the *View Performance* page.
5. Pick tickers to compare against and read off the table:
   - **Own MWRR** — your account's actual money-weighted return.
   - **{TICKER} MWRR** — what you would have earned if the same dollars had
     gone into that ticker on the same dates.
   - **Aggregate row** — same metrics across all accounts combined (only
     shown when every account has a current value).

## Data

CSV files live in `data/`:

```
data/
├── accounts.csv          # account metadata
├── entries.csv           # cash flows (signed: positive=deposit, negative=withdrawal)
├── current_values.csv    # market value snapshots over time
└── tickers/
    ├── _metadata.csv     # one row per cached ticker
    └── {TICKER}.csv      # daily OHLC prices, one file per ticker
```

These files are git-ignored. Back them up however you back up the rest of
your important files.

## Conventions

- **Currency**: USD only.
- **Sign conventions** (in storage and the UI):
  - Deposit amounts are positive.
  - Withdrawal amounts are negative (entered as a positive amount with the
    Withdrawal type selector).
  - Current values are positive and net of withdrawals to date.
- **Dividends and interest** are folded into your current value, not tracked
  as separate entries.
- **Future dates** are not allowed.

## Running tests

```bash
conda activate investment-tracker
pytest
```

79 tests cover the storage, returns, simulation, ticker, and recurring-date
modules. They use a temp directory for storage isolation and a fake fetcher
for ticker tests, so no network is required.

## Architecture

See [DESIGN.md](DESIGN.md) for the full design document — schemas, sign
conventions, MWRR math, ticker simulation, page behavior, and the API
surface of every `lib/` module.

## Stack

- Python 3.12
- Streamlit (UI)
- Polars (data manipulation)
- scipy (XIRR via Brent's method)
- yfinance (price data)
