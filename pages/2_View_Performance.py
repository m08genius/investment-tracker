"""
View Performance page: see MWRR (and optionally TWRR) per account and in
aggregate, and compare against cached tickers.
"""

from __future__ import annotations

import math
from datetime import date

import pandas as pd
import polars as pl
import streamlit as st

from lib import returns, simulation, storage


st.set_page_config(page_title="View Performance", page_icon="📈", layout="wide")
st.title("📈 View Performance")

st.caption(
    "Snapshot entries are added on the **Accounts** page. "
    "MWRR uses the most recent snapshot; TWRR uses all snapshots over time."
)

accounts = storage.load_accounts()
if accounts.is_empty():
    st.info("No accounts yet. Head to **Accounts** to create one.")
    st.stop()

all_account_ids   = [r["account_id"] for r in accounts.iter_rows(named=True)]
all_account_names = {r["account_id"]: r["security"]  for r in accounts.iter_rows(named=True)}
all_account_info  = {r["account_id"]: r               for r in accounts.iter_rows(named=True)}

# ---------------------------------------------------------------------------
# Ticker selection + options
# ---------------------------------------------------------------------------

cached_tickers = storage.list_active_tickers()

if "perf_tickers_saved" not in st.session_state:
    st.session_state["perf_tickers_saved"] = cached_tickers[:1] if cached_tickers else []
if "perf_twrr_saved" not in st.session_state:
    st.session_state["perf_twrr_saved"] = False

saved_tickers = [t for t in st.session_state["perf_tickers_saved"] if t in cached_tickers]

if "perf_tickers_widget" not in st.session_state:
    st.session_state["perf_tickers_widget"] = saved_tickers
if "perf_twrr_widget" not in st.session_state:
    st.session_state["perf_twrr_widget"] = st.session_state["perf_twrr_saved"]

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
        key="perf_tickers_widget",
        help="Choose one or more cached tickers to simulate as a comparison.",
    )
    st.session_state["perf_tickers_saved"] = selected_tickers

show_twrr = st.checkbox(
    "Show TWRR columns",
    key="perf_twrr_widget",
    help="Time-Weighted Rate of Return. Requires ≥ 2 snapshots per account. "
         "Add snapshots on the Accounts page.",
)
st.session_state["perf_twrr_saved"] = show_twrr

# Initialise sticky state for the date-range controls
if "perf_start_mode_widget" not in st.session_state:
    st.session_state["perf_start_mode_widget"] = "Since Inception"
if "perf_end_mode_widget" not in st.session_state:
    st.session_state["perf_end_mode_widget"] = "Current Date"
if "perf_start_date_widget" not in st.session_state:
    st.session_state["perf_start_date_widget"] = date.today()
if "perf_valuation_date_widget" not in st.session_state:
    st.session_state["perf_valuation_date_widget"] = date.today()

col_start, col_end = st.columns(2)
with col_start:
    start_mode = st.selectbox(
        "From",
        ["Since Inception", "Custom"],
        key="perf_start_mode_widget",
    )
    start_date: date | None
    if start_mode == "Custom":
        start_date = st.date_input(
            "Start date",
            key="perf_start_date_widget",
            max_value=date.today(),
        )
    else:
        start_date = None

with col_end:
    end_mode = st.selectbox(
        "To",
        ["Current Date", "Custom"],
        key="perf_end_mode_widget",
    )
    valuation_date: date
    if end_mode == "Custom":
        valuation_date = st.date_input(
            "End date",
            key="perf_valuation_date_widget",
            max_value=date.today(),
        )
    else:
        valuation_date = date.today()

if start_date is not None and start_date >= valuation_date:
    st.warning("Start date must be before end date.")
    st.stop()

_GREEN = "background-color: #d4edda"
_RED   = "background-color: #f8d7da"


def _fmt(x: object) -> str:
    """Format a rate cell: float → percentage string, None/NaN → '—'."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{float(x) * 100:.2f}%"


def _render_table(rows: list[dict], warnings: list[str]) -> None:
    df = pd.DataFrame(rows)
    rate_cols = [c for c in df.columns if c not in ("Account Group", "Account")]
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

    if warnings:
        seen: set[str] = set()
        deduped = [w for w in warnings if not (w in seen or seen.add(w))]  # type: ignore[func-returns-value]
        with st.expander(f"⚠️ {len(deduped)} warning{'s' if len(deduped) != 1 else ''}"):
            for w in deduped[:50]:
                st.caption(w)
            if len(deduped) > 50:
                st.caption(f"_...and {len(deduped) - 50} more._")


# ---------------------------------------------------------------------------
# Per-account table
# ---------------------------------------------------------------------------

st.subheader("Per-account")

rows: list[dict] = []
all_warnings: list[str] = []

for row in accounts.iter_rows(named=True):
    aid = row["account_id"]
    name = row["security"]
    group = row["group_name"]
    is_ticker = row["is_ticker"]
    ticker_sym = row["ticker"]

    entries = storage.load_entries(aid)
    # Cash flows: strictly after start_date (captures only the measured period)
    flow_lo = start_date if start_date is not None else date.min
    cash_flows: list[tuple[date, float]] = [
        (date.fromisoformat(d), float(a))
        for d, a in zip(entries["entry_time"].to_list(), entries["amount"].to_list())
        if float(a) != 0.0 and flow_lo < date.fromisoformat(d) <= valuation_date
    ]

    # Determine current portfolio value and the date it applies to.
    # Ticker accounts: computed live as total_shares × close price.
    # Non-ticker accounts: exact snapshot on valuation_date.
    current_val: float | None = None
    current_val_date: date = valuation_date
    if is_ticker and ticker_sym:
        total_shares = float(
            entries.filter(pl.col("entry_time") <= valuation_date.isoformat())["shares"].sum() or 0.0
        )
        price_result = storage.get_ticker_price_and_date(ticker_sym, valuation_date, "close")
        if price_result is not None:
            current_val_date, close_price = price_result
            current_val = total_shares * close_price
    else:
        end_snaps = storage.load_snapshots(aid).filter(
            pl.col("as_of_date") == valuation_date.isoformat()
        )
        if not end_snaps.is_empty():
            current_val = float(end_snaps["value"][0])
            current_val_date = date.fromisoformat(end_snaps["as_of_date"][0])

    # Opening portfolio value at start_date (None when start_date not set).
    v_start: float | None = None
    if start_date is not None:
        if is_ticker and ticker_sym:
            shares_at_start = float(
                entries.filter(pl.col("entry_time") <= start_date.isoformat())["shares"].sum() or 0.0
            )
            start_price_result = storage.get_ticker_price_and_date(ticker_sym, start_date, "close")
            if shares_at_start > 0 and start_price_result is not None:
                _, start_price = start_price_result
                v_start = shares_at_start * start_price
            # else v_start stays None → "—"
        else:
            start_snaps = storage.load_snapshots(aid).filter(
                pl.col("as_of_date") == start_date.isoformat()
            )
            if not start_snaps.is_empty():
                v_start = float(start_snaps["value"][0])
            # else v_start stays None → "—"

    # Build MWRR flows: prepend opening position when start_date is set.
    # compute_mwrr sign-flips deposits, so (start_date, v_start) becomes −v_start
    # in XIRR convention — exactly the cost of "buying" the portfolio at start.
    if start_date is not None:
        if v_start is not None:
            mwrr_flows: list[tuple[date, float]] = [(start_date, v_start)] + cash_flows
        else:
            mwrr_flows = []  # missing opening value → result will be None
    else:
        mwrr_flows = cash_flows

    own_mwrr = None
    if current_val is not None and mwrr_flows:
        own_mwrr = returns.compute_mwrr(mwrr_flows, current_val, current_val_date)

    row_data: dict = {"Account Group": group, "Account": name, "Own MWRR": own_mwrr}

    snaps: list[tuple[date, float]] = []
    if show_twrr:
        if is_ticker and ticker_sym:
            all_ticker_snaps = storage.compute_ticker_snapshots(aid, ticker_sym, valuation_date)
            if start_date is not None and v_start is not None:
                # Prepend opening snapshot; drop any snapshots on or before start_date.
                later_snaps = [(d, v) for d, v in all_ticker_snaps if d > start_date]
                snaps = [(start_date, v_start)] + later_snaps
            elif start_date is None:
                snaps = all_ticker_snaps
            # else: start_date set but v_start is None → snaps stays []
        else:
            if start_date is not None:
                # Both first and last snapshot must be exact boundary dates.
                all_snaps_filtered = storage.load_snapshots(aid).filter(
                    (pl.col("as_of_date") >= start_date.isoformat()) &
                    (pl.col("as_of_date") <= valuation_date.isoformat())
                ).sort("as_of_date")
                if (
                    not all_snaps_filtered.is_empty()
                    and all_snaps_filtered["as_of_date"][0] == start_date.isoformat()
                    and all_snaps_filtered["as_of_date"][-1] == valuation_date.isoformat()
                ):
                    snaps = [
                        (date.fromisoformat(r["as_of_date"]), float(r["value"]))
                        for r in all_snaps_filtered.iter_rows(named=True)
                    ]
            else:
                all_snaps = storage.load_snapshots(aid).filter(
                    pl.col("as_of_date") <= valuation_date.isoformat()
                ).sort("as_of_date")
                # Only compute TWRR if the last snapshot is exactly on the valuation date.
                if (
                    not all_snaps.is_empty()
                    and all_snaps["as_of_date"][-1] == valuation_date.isoformat()
                ):
                    snaps = [
                        (date.fromisoformat(r["as_of_date"]), float(r["value"]))
                        for r in all_snaps.iter_rows(named=True)
                    ]
        row_data["Own TWRR"] = returns.compute_twrr(snaps, cash_flows)

    for tk in selected_tickers:
        # Ticker comparison uses the same adjusted flows (opening position included).
        mwrr_rate, warns = simulation.compute_ticker_comparison_mwrr(
            mwrr_flows, tk, current_val_date,
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

_render_table(rows, all_warnings)

if show_twrr:
    st.caption(
        "TWRR shows '—' when an account has fewer than 2 snapshots. "
    )


# ---------------------------------------------------------------------------
# Aggregate section
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Aggregate")

# Sticky multiselect for accounts included in aggregate.
if "perf_agg_saved" not in st.session_state:
    st.session_state["perf_agg_saved"] = all_account_ids
if "perf_agg_widget" not in st.session_state:
    valid_saved = [a for a in st.session_state["perf_agg_saved"] if a in all_account_ids]
    st.session_state["perf_agg_widget"] = valid_saved

selected_agg_ids: list[str] = st.multiselect(
    "Accounts to include",
    options=all_account_ids,
    format_func=lambda aid: all_account_names[aid],
    key="perf_agg_widget",
    help="Choose which accounts to roll up into the aggregate row.",
)
st.session_state["perf_agg_saved"] = selected_agg_ids

if not selected_agg_ids:
    st.caption("Select at least one account above.")
else:
    agg_flows: list[tuple[date, float]] = []
    agg_value = 0.0
    agg_start_value = 0.0
    agg_val_date: date = valuation_date
    missing: list[str] = []
    agg_warnings: list[str] = []

    for aid in selected_agg_ids:
        acct = all_account_info[aid]
        agg_entries = storage.load_entries(aid)
        agg_flow_lo = start_date if start_date is not None else date.min
        acct_flows = [
            (date.fromisoformat(d), float(a))
            for d, a in zip(
                agg_entries["entry_time"].to_list(),
                agg_entries["amount"].to_list(),
            )
            if float(a) != 0.0 and agg_flow_lo < date.fromisoformat(d) <= valuation_date
        ]
        if acct["is_ticker"] and acct["ticker"]:
            total_shares = float(
                agg_entries.filter(
                    pl.col("entry_time") <= valuation_date.isoformat()
                )["shares"].sum() or 0.0
            )
            price_result = storage.get_ticker_price_and_date(acct["ticker"], valuation_date, "close")
            if price_result is None:
                missing.append(all_account_names[aid])
                continue
            effective_date, close_price = price_result

            # Start value for ticker accounts
            if start_date is not None:
                shares_at_start = float(
                    agg_entries.filter(
                        pl.col("entry_time") <= start_date.isoformat()
                    )["shares"].sum() or 0.0
                )
                start_pr = storage.get_ticker_price_and_date(acct["ticker"], start_date, "close")
                if shares_at_start > 0 and start_pr is not None:
                    _, s_price = start_pr
                    agg_start_value += shares_at_start * s_price
                else:
                    missing.append(all_account_names[aid])
                    continue

            agg_flows.extend(acct_flows)
            agg_value += total_shares * close_price
            if effective_date < agg_val_date:
                agg_val_date = effective_date
        else:
            end_snap_q = storage.load_snapshots(aid).filter(
                pl.col("as_of_date") == valuation_date.isoformat()
            )
            if end_snap_q.is_empty():
                missing.append(all_account_names[aid])
                continue

            # Start value for non-ticker accounts
            if start_date is not None:
                start_snap_q = storage.load_snapshots(aid).filter(
                    pl.col("as_of_date") == start_date.isoformat()
                )
                if start_snap_q.is_empty():
                    missing.append(all_account_names[aid])
                    continue
                agg_start_value += float(start_snap_q["value"][0])

            snap_date = date.fromisoformat(end_snap_q["as_of_date"][0])
            agg_flows.extend(acct_flows)
            agg_value += float(end_snap_q["value"][0])
            if snap_date < agg_val_date:
                agg_val_date = snap_date

    if missing:
        reason = "no snapshot on start or end date" if start_date is not None else "no snapshot recorded"
        st.caption(
            f"_Aggregate excludes **{', '.join(missing)}** — {reason}._"
        )

    eligible = not missing or len(missing) < len(selected_agg_ids)

    # Build aggregate MWRR flows (same opening-position trick as per-account).
    if start_date is not None:
        agg_mwrr_flows: list[tuple[date, float]] = (
            [(start_date, agg_start_value)] + agg_flows if agg_flows else []
        )
    else:
        agg_mwrr_flows = agg_flows

    agg_own_mwrr: float | None = None
    if eligible and agg_mwrr_flows:
        agg_own_mwrr = returns.compute_mwrr(agg_mwrr_flows, agg_value, agg_val_date)

    agg_row: dict = {"Account": "Aggregate", "Own MWRR": agg_own_mwrr}
    if show_twrr:
        agg_row["Own TWRR"] = None   # not meaningful across accounts

    for tk in selected_tickers:
        if eligible and agg_mwrr_flows:
            rate, warns = simulation.compute_ticker_comparison_mwrr(
                agg_mwrr_flows, tk, agg_val_date,
            )
            agg_row[f"{tk} MWRR"] = rate
            for w in warns:
                agg_warnings.append(f"[Aggregate vs {tk}] {w}")
        else:
            agg_row[f"{tk} MWRR"] = None
        if show_twrr:
            agg_row[f"{tk} TWRR"] = None   # not meaningful across accounts

    _render_table([agg_row], agg_warnings)

    if show_twrr:
        st.caption("TWRR is not computed for aggregate — it is not meaningful across accounts.")

    if not agg_flows:
        st.caption("_No cash flows found for the selected accounts._")


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
        flow_dates = entries.filter(pl.col("amount") != 0.0)["entry_time"].drop_nulls()
        if flow_dates.is_empty():
            continue
        first_entry = date.fromisoformat(flow_dates.min())
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
