"""
View Performance page: see MWRR (and optionally TWRR) per account and in
aggregate, and compare against cached tickers.
"""

from __future__ import annotations

import math
from datetime import date

import pandas as pd
import streamlit as st

from lib import returns, simulation, storage


st.set_page_config(page_title="View Performance", page_icon="📈", layout="wide")
st.title("📈 View Performance")

st.caption(
    "Current values are entered on the **Accounts** page (Snapshot entries tab). "
    "MWRR uses the most recent snapshot; TWRR uses all snapshots over time."
)

accounts = storage.load_accounts()
if accounts.is_empty():
    st.info("No accounts yet. Head to **Accounts** to create one.")
    st.stop()


# ---------------------------------------------------------------------------
# Ticker selection + options
# ---------------------------------------------------------------------------

cached_tickers = storage.list_active_tickers()

if not cached_tickers:
    st.info(
        "No cached tickers yet. Visit **Ticker Data** to add one (e.g. VOO, FXAIX) "
        "to compare your performance against an index."
    )
    selected_tickers: list[str] = []
else:
    selected_tickers = st.multiselect(
        "Compare against tickers",
        options=cached_tickers,
        default=cached_tickers[:1] if cached_tickers else [],
        help="Choose one or more cached tickers to simulate as a comparison.",
    )

show_twrr = st.checkbox(
    "Show TWRR columns",
    value=False,
    help="Time-Weighted Rate of Return. Requires ≥ 2 snapshots per account. "
         "Add snapshots on the Accounts page.",
)

valuation_date = date.today()

_GREEN = "background-color: #d4edda"
_RED   = "background-color: #f8d7da"


def _fmt(x: object) -> str:
    """Format a rate cell: float → percentage string, None/NaN → '—'."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{float(x) * 100:.2f}%"


# ---------------------------------------------------------------------------
# Build the comparison table (raw floats, None for missing)
# ---------------------------------------------------------------------------

rows: list[dict] = []
all_warnings: list[str] = []

aggregate_flows: list[tuple[date, float]] = []
aggregate_value = 0.0
aggregate_eligible = True

for row in accounts.iter_rows(named=True):
    aid = row["account_id"]
    name = row["name"]

    entries = storage.load_entries(aid)
    cash_flows: list[tuple[date, float]] = [
        (date.fromisoformat(d), float(a))
        for d, a in zip(entries["date"].to_list(), entries["amount"].to_list())
    ]
    latest = storage.get_latest_current_value(aid)

    if latest is None:
        aggregate_eligible = False
        own_mwrr = None
    else:
        own_mwrr = returns.compute_mwrr(
            cash_flows,
            float(latest["value"]),
            date.fromisoformat(latest["as_of_date"]),
        )
        aggregate_flows.extend(cash_flows)
        aggregate_value += float(latest["value"])

    row_data: dict = {"Account": name, "Own MWRR": own_mwrr}

    snaps: list[tuple[date, float]] = []
    if show_twrr:
        snaps = [
            (date.fromisoformat(r["as_of_date"]), float(r["value"]))
            for r in storage.load_current_values(aid).iter_rows(named=True)
        ]
        row_data["Own TWRR"] = returns.compute_twrr(snaps, cash_flows)

    for tk in selected_tickers:
        mwrr_rate, warns = simulation.compute_ticker_comparison_mwrr(
            cash_flows, tk, valuation_date,
        )
        row_data[f"{tk} MWRR"] = mwrr_rate
        for w in warns:
            all_warnings.append(f"[{name} vs {tk}] {w}")

        if show_twrr:
            twrr_rate, twrr_warns = simulation.compute_ticker_comparison_twrr(snaps, tk)
            row_data[f"{tk} TWRR"] = twrr_rate
            for w in twrr_warns:
                all_warnings.append(f"[{name} vs {tk} TWRR] {w}")

    rows.append(row_data)


# Aggregate row (TWRR always None — not meaningful across accounts)
agg_own_mwrr: float | None = None
if aggregate_eligible and aggregate_flows:
    agg_own_mwrr = returns.compute_mwrr(aggregate_flows, aggregate_value, valuation_date)

agg_row: dict = {"Account": "All accounts (aggregate)", "Own MWRR": agg_own_mwrr}
if show_twrr:
    agg_row["Own TWRR"] = None

for tk in selected_tickers:
    if aggregate_eligible and aggregate_flows:
        rate, warns = simulation.compute_ticker_comparison_mwrr(
            aggregate_flows, tk, valuation_date,
        )
        agg_row[f"{tk} MWRR"] = rate
        for w in warns:
            all_warnings.append(f"[Aggregate vs {tk}] {w}")
    else:
        agg_row[f"{tk} MWRR"] = None
    if show_twrr:
        agg_row[f"{tk} TWRR"] = None

rows.append(agg_row)


# ---------------------------------------------------------------------------
# Style and display
# ---------------------------------------------------------------------------

df = pd.DataFrame(rows)
rate_cols = [c for c in df.columns if c != "Account"]

# Replace Python None with NaN so pandas handles it uniformly.
df[rate_cols] = df[rate_cols].astype(float)

mwrr_ticker_cols = [c for c in rate_cols if c not in ("Own MWRR", "Own TWRR") and "MWRR" in c]
twrr_ticker_cols = [c for c in rate_cols if c not in ("Own MWRR", "Own TWRR") and "TWRR" in c]


def _color_row(row: pd.Series) -> list[str]:
    styles = [""] * len(row)
    col_idx = {c: i for i, c in enumerate(row.index)}

    own_mwrr = row.get("Own MWRR")
    own_twrr = row.get("Own TWRR") if "Own TWRR" in row.index else float("nan")

    for col in mwrr_ticker_cols:
        val = row[col]
        if pd.isna(val) or pd.isna(own_mwrr):
            continue
        styles[col_idx[col]] = _GREEN if val > own_mwrr else (_RED if val < own_mwrr else "")

    for col in twrr_ticker_cols:
        val = row[col]
        if pd.isna(val) or pd.isna(own_twrr):
            continue
        styles[col_idx[col]] = _GREEN if val > own_twrr else (_RED if val < own_twrr else "")

    return styles


fmt_dict = {col: _fmt for col in rate_cols}
styled = df.style.apply(_color_row, axis=1).format(fmt_dict, na_rep="—")

st.dataframe(styled, use_container_width=True, hide_index=True)

if show_twrr:
    st.caption(
        "TWRR shows '—' when an account has fewer than 2 snapshots. "
        "Aggregate TWRR is not computed — it is not meaningful across accounts."
    )

if not aggregate_eligible:
    n_missing = sum(
        1 for r in accounts.iter_rows(named=True)
        if storage.get_latest_current_value(r["account_id"]) is None
    )
    st.caption(
        f"_Aggregate row unavailable: {n_missing} of {accounts.height} accounts "
        f"are missing a current value._"
    )

# Show warnings (deduplicated, capped)
if all_warnings:
    seen: set[str] = set()
    deduped: list[str] = []
    for w in all_warnings:
        if w not in seen:
            seen.add(w)
            deduped.append(w)

    with st.expander(f"⚠️ {len(deduped)} warning{'s' if len(deduped) != 1 else ''}"):
        for w in deduped[:50]:
            st.caption(w)
        if len(deduped) > 50:
            st.caption(f"_...and {len(deduped) - 50} more._")


# ---------------------------------------------------------------------------
# Cache-gap helper
# ---------------------------------------------------------------------------

gap_messages: list[str] = []
for tk in selected_tickers:
    meta = storage.get_ticker_metadata(tk)
    if meta is None or not meta["earliest_date"]:
        continue
    earliest_cached = date.fromisoformat(meta["earliest_date"])
    for row in accounts.iter_rows(named=True):
        entries = storage.load_entries(row["account_id"])
        if entries.is_empty():
            continue
        first_entry = date.fromisoformat(entries["date"].min())
        if (earliest_cached - first_entry).days > 7:
            gap_messages.append(
                f"**{tk}** cache starts {meta['earliest_date']}, "
                f"but **{row['name']}** has entries from {first_entry.isoformat()}. "
                f"Refresh {tk} with an earlier start date on the Ticker Data page."
            )

if gap_messages:
    with st.container(border=True):
        st.warning("Some tickers don't cover all your cash flows:")
        for m in gap_messages:
            st.caption(m)
