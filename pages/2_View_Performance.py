"""
View Performance page: enter current values, see MWRR per account and in
aggregate, and compare against cached tickers.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import streamlit as st

from lib import returns, simulation, storage


st.set_page_config(page_title="View Performance", page_icon="📈", layout="wide")
st.title("📈 View Performance")


accounts = storage.load_accounts()
if accounts.is_empty():
    st.info("No accounts yet. Head to **Accounts** to create one.")
    st.stop()


# ---------------------------------------------------------------------------
# Per-account current value entry
# ---------------------------------------------------------------------------

st.header("Current values")
st.caption(
    "Record the latest market value of each account. Used as the final cash "
    "flow when computing MWRR. Enter the value **net of withdrawals to date** "
    "(don't subtract withdrawals manually — they're already in your entries)."
)

for row in accounts.iter_rows(named=True):
    aid = row["account_id"]
    name = row["name"]
    latest = storage.get_latest_current_value(aid)

    with st.container(border=True):
        c1, c2, c3, c4 = st.columns([2, 2, 2, 3])
        c1.markdown(f"**{name}**")

        if latest:
            c2.caption("Latest snapshot")
            c2.write(f"${latest['value']:,.2f}")
            c3.caption("As of")
            c3.write(latest["as_of_date"])
        else:
            c2.caption("Latest snapshot")
            c2.write("_none yet_")
            c3.empty()

        with c4:
            with st.form(f"cv_form_{aid}", clear_on_submit=True):
                fc1, fc2, fc3 = st.columns([2, 2, 1])
                new_val = fc1.number_input(
                    "Value ($)", min_value=0.0, step=100.0,
                    value=float(latest["value"]) if latest else 0.0,
                    key=f"cv_val_{aid}",
                    label_visibility="collapsed",
                )
                new_as_of = fc2.date_input(
                    "As of",
                    value=date.today(),
                    max_value=date.today(),
                    key=f"cv_date_{aid}",
                    label_visibility="collapsed",
                )
                if fc3.form_submit_button("Save"):
                    storage.set_current_value(aid, new_val, new_as_of)
                    st.rerun()


# ---------------------------------------------------------------------------
# Comparison table
# ---------------------------------------------------------------------------

st.divider()
st.header("Performance")

cached_tickers = storage.list_cached_tickers()

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

valuation_date = date.today()


def _format_rate(r: float | None) -> str:
    if r is None:
        return "—"
    return f"{r * 100:.2f}%"


# Build the comparison table.
# Rows: each account + an aggregate row.
# Columns: Account MWRR (own), then one per selected ticker.

rows = []
all_warnings: list[str] = []

# Track aggregate-eligibility: every account must have a current value.
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
        own_rate = None
    else:
        own_rate = returns.compute_mwrr(
            cash_flows,
            float(latest["value"]),
            date.fromisoformat(latest["as_of_date"]),
        )
        # Accumulate for aggregate computation
        aggregate_flows.extend(cash_flows)
        aggregate_value += float(latest["value"])

    row_data = {"Account": name, "Own MWRR": _format_rate(own_rate)}

    for tk in selected_tickers:
        rate, warns = simulation.compute_ticker_comparison_mwrr(
            cash_flows, tk, valuation_date,
        )
        row_data[f"{tk} MWRR"] = _format_rate(rate)
        for w in warns:
            all_warnings.append(f"[{name} vs {tk}] {w}")

    rows.append(row_data)


# Aggregate row
agg_row = {"Account": "**All accounts (aggregate)**"}
if aggregate_eligible and aggregate_flows:
    agg_rate = returns.compute_mwrr(aggregate_flows, aggregate_value, valuation_date)
    agg_row["Own MWRR"] = _format_rate(agg_rate)
    for tk in selected_tickers:
        rate, warns = simulation.compute_ticker_comparison_mwrr(
            aggregate_flows, tk, valuation_date,
        )
        agg_row[f"{tk} MWRR"] = _format_rate(rate)
        for w in warns:
            all_warnings.append(f"[Aggregate vs {tk}] {w}")
else:
    agg_row["Own MWRR"] = "—"
    for tk in selected_tickers:
        agg_row[f"{tk} MWRR"] = "—"

rows.append(agg_row)

display_df = pl.DataFrame(rows)
st.dataframe(display_df, use_container_width=True, hide_index=True)

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

# If any account has entries earlier than the cache start of any selected ticker,
# point the user at the Ticker Data page.
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
