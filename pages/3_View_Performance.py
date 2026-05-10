"""
View Performance page: see MWRR (and optionally TWRR) per account and in
aggregate, and compare against cached tickers.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import streamlit as st

from lib import returns, simulation, storage


st.set_page_config(page_title="View Performance", page_icon="📈", layout="wide")
st.title("📈 View Performance")

st.caption(
    "Current values are entered on the **Snapshots** page. "
    "MWRR uses the most recent snapshot; TWRR uses all snapshots over time."
)

accounts = storage.load_accounts()
if accounts.is_empty():
    st.info("No accounts yet. Head to **Accounts** to create one.")
    st.stop()


# ---------------------------------------------------------------------------
# Ticker selection + options
# ---------------------------------------------------------------------------

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

show_twrr = st.checkbox(
    "Show TWRR columns",
    value=False,
    help="Time-Weighted Rate of Return. Requires ≥ 2 snapshots per account. "
         "Add snapshots on the Snapshots page.",
)

valuation_date = date.today()


def _format_rate(r: float | None) -> str:
    if r is None:
        return "—"
    return f"{r * 100:.2f}%"


# ---------------------------------------------------------------------------
# Build the comparison table
# ---------------------------------------------------------------------------

rows = []
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

    row_data: dict[str, str] = {"Account": name, "Own MWRR": _format_rate(own_mwrr)}

    snaps: list[tuple[date, float]] = []
    if show_twrr:
        snaps = [
            (date.fromisoformat(r["as_of_date"]), float(r["value"]))
            for r in storage.load_current_values(aid).iter_rows(named=True)
        ]
        row_data["Own TWRR"] = _format_rate(returns.compute_twrr(snaps, cash_flows))

    for tk in selected_tickers:
        mwrr_rate, warns = simulation.compute_ticker_comparison_mwrr(
            cash_flows, tk, valuation_date,
        )
        row_data[f"{tk} MWRR"] = _format_rate(mwrr_rate)
        for w in warns:
            all_warnings.append(f"[{name} vs {tk}] {w}")

        if show_twrr:
            twrr_rate, twrr_warns = simulation.compute_ticker_comparison_twrr(snaps, tk)
            row_data[f"{tk} TWRR"] = _format_rate(twrr_rate)
            for w in twrr_warns:
                all_warnings.append(f"[{name} vs {tk} TWRR] {w}")

    rows.append(row_data)


# Aggregate row
agg_row: dict[str, str] = {"Account": "**All accounts (aggregate)**"}
if aggregate_eligible and aggregate_flows:
    agg_rate = returns.compute_mwrr(aggregate_flows, aggregate_value, valuation_date)
    agg_row["Own MWRR"] = _format_rate(agg_rate)
    if show_twrr:
        agg_row["Own TWRR"] = "—"
    for tk in selected_tickers:
        rate, warns = simulation.compute_ticker_comparison_mwrr(
            aggregate_flows, tk, valuation_date,
        )
        agg_row[f"{tk} MWRR"] = _format_rate(rate)
        for w in warns:
            all_warnings.append(f"[Aggregate vs {tk}] {w}")
        if show_twrr:
            agg_row[f"{tk} TWRR"] = "—"
else:
    agg_row["Own MWRR"] = "—"
    if show_twrr:
        agg_row["Own TWRR"] = "—"
    for tk in selected_tickers:
        agg_row[f"{tk} MWRR"] = "—"
        if show_twrr:
            agg_row[f"{tk} TWRR"] = "—"

rows.append(agg_row)

display_df = pl.DataFrame(rows)
st.dataframe(display_df, use_container_width=True, hide_index=True)

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
