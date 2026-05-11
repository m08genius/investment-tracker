"""
View Performance page: see MWRR (and optionally TWRR) per account group,
with starting value, ending value, net deposits and withdrawals.
Compare against cached tickers.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date
from typing import Any

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
all_account_names = {r["account_id"]: r["security"] for r in accounts.iter_rows(named=True)}
all_account_info  = {r["account_id"]: r              for r in accounts.iter_rows(named=True)}

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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GREEN = "background-color: #d4edda"
_RED   = "background-color: #f8d7da"
_DOLLAR_COLS = {"Start Value", "End Value", "Net Deposits", "Net Withdrawals"}


def _fmt_rate(x: object) -> str:
    """Format a rate cell: float → percentage string, None/NaN → '—'."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{float(x) * 100:.2f}%"


def _fmt_dollar(x: object) -> str:
    """Format a dollar cell: float → currency string, None/NaN → '—'."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"${float(x):,.0f}"


def _render_table(rows: list[dict], warnings: list[str]) -> None:
    df = pd.DataFrame(rows)

    # Enforce canonical column order
    _label_order  = ["Account Group", "Account"]
    _dollar_order = ["Start Value", "Net Deposits", "Net Withdrawals", "End Value"]
    _mwrr_cols    = [c for c in df.columns if "MWRR" in c]   # Own MWRR first, then tickers
    _twrr_cols    = [c for c in df.columns if "TWRR" in c]   # Own TWRR first, then tickers
    _ordered = (
        [c for c in _label_order  if c in df.columns]
        + [c for c in _dollar_order if c in df.columns]
        + _mwrr_cols
        + _twrr_cols
    )
    df = df[_ordered]

    non_data_cols = {"Account Group", "Account"}
    dollar_cols_present = [c for c in df.columns if c in _DOLLAR_COLS]
    rate_cols = [c for c in df.columns if c not in non_data_cols and c not in _DOLLAR_COLS]

    for col in rate_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in dollar_cols_present:
        df[col] = pd.to_numeric(df[col], errors="coerce")

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

    fmt_dict: dict[str, Any] = {col: _fmt_rate for col in rate_cols}
    fmt_dict.update({col: _fmt_dollar for col in dollar_cols_present})
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
# Per-account computation (main table)
# ---------------------------------------------------------------------------

def _compute_account_rows(
    group_accounts: dict[str, list[dict]],
    start_date_: date | None,
    valuation_date_: date,
    selected_tickers_: list[str],
    show_twrr_: bool,
) -> tuple[list[dict], list[str]]:
    """Return (rows, warnings) — one row per account, ordered by group."""
    rows: list[dict] = []
    all_warnings: list[str] = []
    flow_lo = start_date_ if start_date_ is not None else date.min

    for group_name, acct_rows in group_accounts.items():
        for acct_row in acct_rows:
            aid = acct_row["account_id"]
            name = acct_row["security"]
            is_ticker = acct_row["is_ticker"]
            ticker_sym = acct_row["ticker"]

            entries = storage.load_entries(aid)
            cash_flows: list[tuple[date, float]] = [
                (date.fromisoformat(d), float(a))
                for d, a in zip(entries["entry_time"].to_list(), entries["amount"].to_list())
                if float(a) != 0.0 and flow_lo < date.fromisoformat(d) <= valuation_date_
            ]

            # ── End value ──────────────────────────────────────────────────
            current_val: float | None = None
            current_val_date: date = valuation_date_
            if is_ticker and ticker_sym:
                total_shares = float(
                    entries.filter(
                        pl.col("entry_time") <= valuation_date_.isoformat()
                    )["shares"].sum() or 0.0
                )
                price_result = storage.get_ticker_price_and_date(ticker_sym, valuation_date_, "close")
                if price_result is not None:
                    current_val_date, close_price = price_result
                    current_val = total_shares * close_price
            else:
                end_snaps = storage.load_snapshots(aid).filter(
                    pl.col("as_of_date") == valuation_date_.isoformat()
                )
                if not end_snaps.is_empty():
                    current_val = float(end_snaps["value"][0])
                    current_val_date = date.fromisoformat(end_snaps["as_of_date"][0])

            # ── Start value ─────────────────────────────────────────────────
            v_start: float | None = None
            if start_date_ is not None:
                if is_ticker and ticker_sym:
                    shares_at_start = float(
                        entries.filter(
                            pl.col("entry_time") <= start_date_.isoformat()
                        )["shares"].sum() or 0.0
                    )
                    sp_result = storage.get_ticker_price_and_date(ticker_sym, start_date_, "close")
                    if shares_at_start > 0 and sp_result is not None:
                        _, start_price = sp_result
                        v_start = shares_at_start * start_price
                else:
                    start_snaps_q = storage.load_snapshots(aid).filter(
                        pl.col("as_of_date") == start_date_.isoformat()
                    )
                    if not start_snaps_q.is_empty():
                        v_start = float(start_snaps_q["value"][0])

            # ── Net deposits / withdrawals ───────────────────────────────────
            net_deposits = sum(a for _, a in cash_flows if a > 0)
            net_withdrawals = sum(abs(a) for _, a in cash_flows if a < 0)

            # ── MWRR flows ──────────────────────────────────────────────────
            if start_date_ is not None:
                if v_start is not None:
                    mwrr_flows: list[tuple[date, float]] = [(start_date_, v_start)] + cash_flows
                else:
                    mwrr_flows = []
            else:
                mwrr_flows = cash_flows

            own_mwrr: float | None = None
            if current_val is not None and mwrr_flows:
                own_mwrr = returns.compute_mwrr(mwrr_flows, current_val, current_val_date)

            # ── TWRR snaps ──────────────────────────────────────────────────
            snaps: list[tuple[date, float]] = []
            if show_twrr_:
                if is_ticker and ticker_sym:
                    all_ticker_snaps = storage.compute_ticker_snapshots(aid, ticker_sym, valuation_date_)
                    if start_date_ is not None and v_start is not None:
                        later = [(d, v) for d, v in all_ticker_snaps if d > start_date_]
                        snaps = [(start_date_, v_start)] + later
                    elif start_date_ is None:
                        snaps = all_ticker_snaps
                else:
                    if start_date_ is not None:
                        all_snaps_f = storage.load_snapshots(aid).filter(
                            (pl.col("as_of_date") >= start_date_.isoformat()) &
                            (pl.col("as_of_date") <= valuation_date_.isoformat())
                        ).sort("as_of_date")
                        if (
                            not all_snaps_f.is_empty()
                            and all_snaps_f["as_of_date"][0] == start_date_.isoformat()
                            and all_snaps_f["as_of_date"][-1] == valuation_date_.isoformat()
                        ):
                            snaps = [
                                (date.fromisoformat(r["as_of_date"]), float(r["value"]))
                                for r in all_snaps_f.iter_rows(named=True)
                            ]
                    else:
                        all_snaps = storage.load_snapshots(aid).filter(
                            pl.col("as_of_date") <= valuation_date_.isoformat()
                        ).sort("as_of_date")
                        if (
                            not all_snaps.is_empty()
                            and all_snaps["as_of_date"][-1] == valuation_date_.isoformat()
                        ):
                            snaps = [
                                (date.fromisoformat(r["as_of_date"]), float(r["value"]))
                                for r in all_snaps.iter_rows(named=True)
                            ]

            # ── Build row ────────────────────────────────────────────────────
            row_data: dict = {"Account Group": group_name, "Account": name}
            row_data["Start Value"] = v_start if start_date_ is not None else 0.0
            row_data["End Value"] = current_val
            row_data["Net Deposits"] = net_deposits
            row_data["Net Withdrawals"] = net_withdrawals
            row_data["Own MWRR"] = own_mwrr

            if show_twrr_:
                row_data["Own TWRR"] = returns.compute_twrr(snaps, cash_flows)

            for tk in selected_tickers_:
                mwrr_rate, warns = simulation.compute_ticker_comparison_mwrr(
                    mwrr_flows, tk, current_val_date,
                )
                row_data[f"{tk} MWRR"] = mwrr_rate
                for w in warns:
                    all_warnings.append(f"[{name} vs {tk}] {w}")

                if show_twrr_:
                    twrr_rate, twrr_warns = simulation.compute_ticker_comparison_twrr(snaps, tk)
                    row_data[f"{tk} TWRR"] = twrr_rate
                    for w in twrr_warns:
                        all_warnings.append(f"[{name} vs {tk} TWRR] {w}")

            rows.append(row_data)

    return rows, all_warnings


# ---------------------------------------------------------------------------
# Core per-group computation
# ---------------------------------------------------------------------------

def _compute_group_rows(
    group_accounts: dict[str, list[dict]],
    start_date_: date | None,
    valuation_date_: date,
    selected_tickers_: list[str],
    show_twrr_: bool,
    excluded_ids: set[str] | None = None,
) -> tuple[list[dict], list[str]]:
    """Return (rows, warnings) — one row per account group.

    Each row aggregates all non-excluded accounts in the group.
    Accounts with missing snapshot/price data are noted in warnings and
    excluded from the group totals.
    """
    excluded_ids = excluded_ids or set()
    rows: list[dict] = []
    all_warnings: list[str] = []
    flow_lo = start_date_ if start_date_ is not None else date.min

    for group_name, acct_rows in group_accounts.items():
        active_rows = [r for r in acct_rows if r["account_id"] not in excluded_ids]
        if not active_rows:
            continue

        group_end_value = 0.0
        group_start_value = 0.0
        group_net_deposits = 0.0
        group_net_withdrawals = 0.0
        group_period_flows: list[tuple[date, float]] = []
        group_end_date: date = valuation_date_
        missing_names: list[str] = []
        missing_ids: set[str] = set()

        for acct_row in active_rows:
            aid = acct_row["account_id"]
            name = acct_row["security"]
            is_ticker = acct_row["is_ticker"]
            ticker_sym = acct_row["ticker"]

            entries = storage.load_entries(aid)
            cash_flows: list[tuple[date, float]] = [
                (date.fromisoformat(d), float(a))
                for d, a in zip(entries["entry_time"].to_list(), entries["amount"].to_list())
                if float(a) != 0.0 and flow_lo < date.fromisoformat(d) <= valuation_date_
            ]

            # ── End value ──────────────────────────────────────────────────
            current_val: float | None = None
            current_val_date: date = valuation_date_
            if is_ticker and ticker_sym:
                total_shares = float(
                    entries.filter(
                        pl.col("entry_time") <= valuation_date_.isoformat()
                    )["shares"].sum() or 0.0
                )
                price_result = storage.get_ticker_price_and_date(ticker_sym, valuation_date_, "close")
                if price_result is not None:
                    current_val_date, close_price = price_result
                    current_val = total_shares * close_price
            else:
                end_snaps = storage.load_snapshots(aid).filter(
                    pl.col("as_of_date") == valuation_date_.isoformat()
                )
                if not end_snaps.is_empty():
                    current_val = float(end_snaps["value"][0])
                    current_val_date = date.fromisoformat(end_snaps["as_of_date"][0])

            if current_val is None:
                missing_names.append(name)
                missing_ids.add(aid)
                continue

            # ── Start value (when a period start date is set) ───────────────
            v_start: float | None = None
            if start_date_ is not None:
                if is_ticker and ticker_sym:
                    shares_at_start = float(
                        entries.filter(
                            pl.col("entry_time") <= start_date_.isoformat()
                        )["shares"].sum() or 0.0
                    )
                    sp_result = storage.get_ticker_price_and_date(ticker_sym, start_date_, "close")
                    if shares_at_start > 0 and sp_result is not None:
                        _, start_price = sp_result
                        v_start = shares_at_start * start_price
                else:
                    start_snaps_q = storage.load_snapshots(aid).filter(
                        pl.col("as_of_date") == start_date_.isoformat()
                    )
                    if not start_snaps_q.is_empty():
                        v_start = float(start_snaps_q["value"][0])

                if v_start is None:
                    missing_names.append(name)
                    missing_ids.add(aid)
                    continue

            # ── Accumulate ──────────────────────────────────────────────────
            group_end_value += current_val
            if v_start is not None:
                group_start_value += v_start
            group_period_flows.extend(cash_flows)
            if current_val_date < group_end_date:
                group_end_date = current_val_date

            for _, amt in cash_flows:
                if amt > 0:
                    group_net_deposits += amt
                else:
                    group_net_withdrawals += abs(amt)

        if missing_names:
            reason = "missing snapshot/price on start or end date" if start_date_ else "missing snapshot/price"
            all_warnings.append(
                f"[{group_name}] Skipped: {', '.join(missing_names)} — {reason}"
            )

        included_count = len(active_rows) - len(missing_names)
        if included_count == 0:
            continue  # entire group has no data

        # ── MWRR ────────────────────────────────────────────────────────────
        # Prepend opening position so XIRR treats V_start as the initial cost.
        if start_date_ is not None:
            mwrr_flows: list[tuple[date, float]] = (
                [(start_date_, group_start_value)] + group_period_flows
            )
        else:
            mwrr_flows = group_period_flows

        own_mwrr: float | None = None
        if mwrr_flows:
            own_mwrr = returns.compute_mwrr(mwrr_flows, group_end_value, group_end_date)

        # ── TWRR snaps (single-account groups only) ─────────────────────────
        # Multi-account groups lack portfolio-level snapshot chains, so TWRR
        # stays None for those.
        snaps: list[tuple[date, float]] = []
        if show_twrr_ and included_count == 1:
            only_row = next(r for r in active_rows if r["account_id"] not in missing_ids)
            o_aid = only_row["account_id"]
            o_is_ticker = only_row["is_ticker"]
            o_ticker = only_row["ticker"]
            o_entries = storage.load_entries(o_aid)

            if o_is_ticker and o_ticker:
                all_ticker_snaps = storage.compute_ticker_snapshots(o_aid, o_ticker, valuation_date_)
                if start_date_ is not None:
                    later = [(d, v) for d, v in all_ticker_snaps if d > start_date_]
                    snaps = [(start_date_, group_start_value)] + later
                else:
                    snaps = all_ticker_snaps
            else:
                if start_date_ is not None:
                    all_snaps_f = storage.load_snapshots(o_aid).filter(
                        (pl.col("as_of_date") >= start_date_.isoformat()) &
                        (pl.col("as_of_date") <= valuation_date_.isoformat())
                    ).sort("as_of_date")
                    if (
                        not all_snaps_f.is_empty()
                        and all_snaps_f["as_of_date"][0] == start_date_.isoformat()
                        and all_snaps_f["as_of_date"][-1] == valuation_date_.isoformat()
                    ):
                        snaps = [
                            (date.fromisoformat(r["as_of_date"]), float(r["value"]))
                            for r in all_snaps_f.iter_rows(named=True)
                        ]
                else:
                    all_snaps = storage.load_snapshots(o_aid).filter(
                        pl.col("as_of_date") <= valuation_date_.isoformat()
                    ).sort("as_of_date")
                    if (
                        not all_snaps.is_empty()
                        and all_snaps["as_of_date"][-1] == valuation_date_.isoformat()
                    ):
                        snaps = [
                            (date.fromisoformat(r["as_of_date"]), float(r["value"]))
                            for r in all_snaps.iter_rows(named=True)
                        ]

        # ── Build row ────────────────────────────────────────────────────────
        row_data: dict = {"Account Group": group_name}
        row_data["Start Value"] = group_start_value if start_date_ is not None else 0.0
        row_data["End Value"] = group_end_value
        row_data["Net Deposits"] = group_net_deposits
        row_data["Net Withdrawals"] = group_net_withdrawals
        row_data["Own MWRR"] = own_mwrr

        if show_twrr_:
            row_data["Own TWRR"] = returns.compute_twrr(snaps, group_period_flows)

        for tk in selected_tickers_:
            mwrr_rate, warns = simulation.compute_ticker_comparison_mwrr(
                mwrr_flows, tk, group_end_date,
            )
            row_data[f"{tk} MWRR"] = mwrr_rate
            for w in warns:
                all_warnings.append(f"[{group_name} vs {tk}] {w}")

            if show_twrr_:
                twrr_rate, twrr_warns = simulation.compute_ticker_comparison_twrr(snaps, tk)
                row_data[f"{tk} TWRR"] = twrr_rate
                for w in twrr_warns:
                    all_warnings.append(f"[{group_name} vs {tk} TWRR] {w}")

        rows.append(row_data)

    return rows, all_warnings


# ---------------------------------------------------------------------------
# Group accounts by group_name
# ---------------------------------------------------------------------------

group_accounts: dict[str, list[dict]] = defaultdict(list)
for _acct in accounts.iter_rows(named=True):
    group_accounts[_acct["group_name"]].append(_acct)

# ---------------------------------------------------------------------------
# Per-group table
# ---------------------------------------------------------------------------

st.subheader("By account group")

# Sticky group filter
all_group_names = list(group_accounts.keys())
if "perf_groups_saved" not in st.session_state:
    st.session_state["perf_groups_saved"] = []
if "perf_groups_widget" not in st.session_state:
    valid_groups = [g for g in st.session_state["perf_groups_saved"] if g in all_group_names]
    st.session_state["perf_groups_widget"] = valid_groups

selected_groups: list[str] = st.multiselect(
    "Filter groups",
    options=all_group_names,
    key="perf_groups_widget",
    help="Select one or more groups to display. Leave empty to show all.",
)
st.session_state["perf_groups_saved"] = selected_groups

filtered_group_accounts = (
    {g: v for g, v in group_accounts.items() if g in selected_groups}
    if selected_groups
    else group_accounts
)

rows, all_warnings = _compute_account_rows(
    filtered_group_accounts,
    start_date,
    valuation_date,
    selected_tickers,
    show_twrr,
)

if rows:
    _render_table(rows, all_warnings)
else:
    st.caption("_No data available for the selected date range._")

if show_twrr:
    st.caption(
        "TWRR shows '—' when fewer than 2 snapshots cover the selected period."
    )


# ---------------------------------------------------------------------------
# Aggregate section
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Aggregate view")

st.caption(
    "Same as above but you can exclude specific accounts from any group's totals."
)

# Sticky exclusion list
if "perf_agg_exclude_saved" not in st.session_state:
    st.session_state["perf_agg_exclude_saved"] = []
if "perf_agg_exclude_widget" not in st.session_state:
    valid_excluded = [a for a in st.session_state["perf_agg_exclude_saved"] if a in all_account_ids]
    st.session_state["perf_agg_exclude_widget"] = valid_excluded

excluded_ids_list: list[str] = st.multiselect(
    "Exclude accounts",
    options=all_account_ids,
    format_func=lambda aid: all_account_names[aid],
    key="perf_agg_exclude_widget",
    help="Remove specific accounts from the aggregate group calculations.",
)
st.session_state["perf_agg_exclude_saved"] = excluded_ids_list

if excluded_ids_list:
    excluded_names = [all_account_names[a] for a in excluded_ids_list]
    st.caption(f"_Excluding: **{', '.join(excluded_names)}**_")

agg_rows, agg_warnings = _compute_group_rows(
    group_accounts,
    start_date,
    valuation_date,
    selected_tickers,
    show_twrr,
    excluded_ids=set(excluded_ids_list),
)

if agg_rows:
    _render_table(agg_rows, agg_warnings)
else:
    st.caption("_No data available for the selected configuration._")

if show_twrr:
    st.caption("TWRR is not computed for groups with multiple accounts.")


# ---------------------------------------------------------------------------
# Cache-gap helper
# ---------------------------------------------------------------------------

gap_messages: list[str] = []
for tk in selected_tickers:
    meta = storage.get_ticker_metadata(tk)
    if meta is None or not meta["earliest_date"]:
        continue
    earliest_cached = date.fromisoformat(meta["earliest_date"])
    for _row in accounts.iter_rows(named=True):
        _entries = storage.load_entries(_row["account_id"])
        flow_dates = _entries.filter(pl.col("amount") != 0.0)["entry_time"].drop_nulls()
        if flow_dates.is_empty():
            continue
        first_entry = date.fromisoformat(flow_dates.min())
        if (earliest_cached - first_entry).days > 7:
            gap_messages.append(
                f"**{tk}** cache starts {meta['earliest_date']}, "
                f"but **{_row['security']}** has entries from {first_entry.isoformat()}. "
                f"Refresh {tk} with an earlier start date on the Ticker Data page."
            )

if gap_messages:
    with st.container(border=True):
        st.warning("Some tickers don't cover all your cash flows:")
        for m in gap_messages:
            st.caption(m)
