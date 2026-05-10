"""
Accounts page: manage account groups and securities, record entries.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import streamlit as st

from lib import recurring, storage


def _parse_dollar(text: str) -> float:
    """Parse a dollar amount string, accepting commas and $ signs."""
    try:
        value = float(text.replace(",", "").replace("$", "").strip())
    except ValueError:
        raise ValueError(f"Invalid amount '{text}'. Enter a number like 1000 or 1,000.00")
    return value


def _parse_shares(text: str) -> float:
    try:
        value = float(text.replace(",", "").strip())
    except ValueError:
        raise ValueError(f"Invalid share count '{text}'.")
    return value


st.set_page_config(page_title="Accounts", page_icon="💼", layout="wide")
st.title("💼 Accounts")


# ---------------------------------------------------------------------------
# Account Group / Security selector
# ---------------------------------------------------------------------------

st.header("Your securities")

accounts_df = storage.load_accounts()

if accounts_df.is_empty():
    st.info("No securities yet. Add one below to get started.")
else:
    account_ids = [r["account_id"] for r in accounts_df.iter_rows(named=True)]
    if "selected_account_id" not in st.session_state or \
            st.session_state["selected_account_id"] not in account_ids:
        st.session_state["selected_account_id"] = account_ids[0]

    account_rows = list(accounts_df.iter_rows(named=True))

    # Group by group_name; named groups first (sorted), then ungrouped.
    groups: dict[str, list] = {}
    for row in account_rows:
        key = row["group_name"] or ""
        groups.setdefault(key, []).append(row)
    named_groups = sorted((k, v) for k, v in groups.items() if k)
    ungrouped = groups.get("", [])
    ordered_groups = named_groups + ([("", ungrouped)] if ungrouped else [])

    for g_idx, (group_label, group_rows) in enumerate(ordered_groups):
        if g_idx > 0:
            st.divider()
        if group_label:
            st.caption(group_label)
        for i in range(0, len(group_rows), 3):
            cols = st.columns(3)
            for col, row in zip(cols, group_rows[i:i + 3]):
                aid = row["account_id"]
                is_selected = st.session_state["selected_account_id"] == aid
                label = f"{'✓ ' if is_selected else ''}{row['security']}"
                if row["is_ticker"] and row["ticker"]:
                    label += f" ({row['ticker']})"
                if col.button(
                    label,
                    key=f"sel_{aid}",
                    type="primary" if is_selected else "secondary",
                    use_container_width=True,
                ):
                    st.session_state["selected_account_id"] = aid
                    st.session_state.pop("recurring_preview", None)
                    st.rerun()

# ---------------------------------------------------------------------------
# Add a new security
# ---------------------------------------------------------------------------

with st.expander("➕ Add a new security"):
    # is_ticker toggle outside the form so it reruns and reshapes the form.
    if "new_sec_is_ticker" not in st.session_state:
        st.session_state["new_sec_is_ticker"] = False

    is_ticker_toggle = st.radio(
        "Security type",
        ["Generic", "Ticker"],
        key="new_sec_is_ticker_radio",
        horizontal=True,
        index=1 if st.session_state["new_sec_is_ticker"] else 0,
    )
    st.session_state["new_sec_is_ticker"] = (is_ticker_toggle == "Ticker")

    with st.form("new_account_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        existing_groups = storage.list_account_groups()
        new_group = c1.text_input(
            "Account Group",
            placeholder="e.g. Fidelity Brokerage",
            help="Groups related securities together. Existing: " + (", ".join(existing_groups) if existing_groups else "none yet"),
        )
        new_security = c2.text_input(
            "Security Name",
            placeholder="e.g. Cash, S&P 500 Fund",
        )

        new_ticker = ""
        if st.session_state["new_sec_is_ticker"]:
            cached_tickers = storage.list_active_tickers()
            if not cached_tickers:
                st.warning(
                    "No tickers cached yet. Visit **Ticker Data** to add one before creating a ticker security."
                )
                ticker_choice = ""
            else:
                ticker_choice = st.selectbox(
                    "Ticker symbol",
                    options=[""] + cached_tickers,
                    format_func=lambda t: t if t else "— select —",
                )
            new_ticker = ticker_choice

        submitted = st.form_submit_button("Add security")
        if submitted:
            try:
                is_ticker = st.session_state["new_sec_is_ticker"]
                storage.add_account(
                    new_group,
                    new_security,
                    is_ticker=is_ticker,
                    ticker=new_ticker if is_ticker else "",
                )
                st.success(f"Added security: {new_security} under {new_group}.")
                st.rerun()
            except ValueError as e:
                st.error(str(e))


# ---------------------------------------------------------------------------
# Per-security section
# ---------------------------------------------------------------------------

if accounts_df.is_empty():
    st.stop()

selected_id = st.session_state["selected_account_id"]
selected_acct = storage.get_account(selected_id)
selected_security = selected_acct["security"]
selected_group = selected_acct["group_name"]
is_ticker_acct = selected_acct["is_ticker"]
ticker_symbol = selected_acct["ticker"]

st.divider()
st.header(f"{selected_group} — {selected_security}")
if is_ticker_acct and ticker_symbol:
    st.caption(f"Ticker: **{ticker_symbol}**")

# Summary stats
_all_entries = storage.load_entries(selected_id)
_total_in = _all_entries.filter(pl.col("amount") > 0)["amount"].sum() or 0
_total_out = -1 * (_all_entries.filter(pl.col("amount") < 0)["amount"].sum() or 0)
_net = _total_in - _total_out
st.dataframe(
    pl.DataFrame({
        "": ["Total deposited", "Total withdrawn", "Net contributed"],
        "Amount": [f"${_total_in:,.2f}", f"${_total_out:,.2f}", f"${_net:,.2f}"],
    }),
    hide_index=True,
    use_container_width=False,
)

st.divider()

# ===========================================================================
# Add entry
# ===========================================================================

_last = st.session_state.get("last_entry", {})

with st.expander("➕ Add entry", expanded=True):
    with st.form("entry_form", clear_on_submit=True):

        if is_ticker_acct and ticker_symbol:
            # --- Ticker entry form ---
            c1, c2, c3 = st.columns(3)
            entry_date = c1.date_input(
                "Date",
                value=_last.get("entry_date", date.today()),
                max_value=date.today(),
            )
            shares_str = c2.text_input(
                "Shares",
                value=_last.get("shares_str", ""),
                placeholder="e.g. 10 or -5 (sell)",
                help="Positive = buy, negative = sell.",
            )
            cost_basis_type = c3.selectbox(
                "Cost basis",
                ["Custom", "Close", "Open", "High", "Low"],
                index=["Custom", "Close", "Open", "High", "Low"].index(
                    _last.get("cost_basis_type", "Close")
                ),
            )
            custom_price_str = ""
            if cost_basis_type == "Custom":
                custom_price_str = st.text_input(
                    "Price per share ($)",
                    value=_last.get("custom_price_str", ""),
                    placeholder="e.g. 123.45",
                )
            else:
                looked_up = storage.get_ticker_price_on_date(
                    ticker_symbol, entry_date, cost_basis_type.lower()
                )
                if looked_up is not None:
                    st.info(f"{ticker_symbol} {cost_basis_type.lower()} on {entry_date}: **${looked_up:,.4f}**")
                else:
                    st.warning(
                        f"No cached price for {ticker_symbol} on {entry_date}. "
                        "Refresh ticker data or use Custom."
                    )
            snap_val_str = st.text_input(
                "Portfolio value ($) — optional snapshot",
                value=_last.get("snap_val_str", ""),
                placeholder="e.g. 12,345.67",
            )
            note = st.text_input("Note (optional)", value=_last.get("note", ""))

            if st.form_submit_button("Save", type="primary"):
                try:
                    shares = _parse_shares(shares_str) if shares_str.strip() else None
                    if shares is None:
                        raise ValueError("Enter number of shares.")
                    if cost_basis_type == "Custom":
                        price = _parse_dollar(custom_price_str) if custom_price_str.strip() else None
                        if price is None:
                            raise ValueError("Enter a custom price per share.")
                    else:
                        price = storage.get_ticker_price_on_date(
                            ticker_symbol, entry_date, cost_basis_type.lower()
                        )
                        if price is None:
                            raise ValueError(
                                f"No cached {cost_basis_type.lower()} price for {ticker_symbol} "
                                f"on {entry_date}. Use Custom or refresh ticker data."
                            )
                    if price <= 0:
                        raise ValueError("Price per share must be greater than 0.")
                    amount = shares * price
                    snap_val: float | None = None
                    if snap_val_str.strip():
                        snap_val = _parse_dollar(snap_val_str)
                        if snap_val < 0:
                            raise ValueError("Portfolio value cannot be negative.")
                    auto_note = note or f"{shares:g} shares @ ${price:,.4f}"
                    storage.add_entry(
                        selected_id, amount, entry_date, auto_note,
                        snapshot_value=snap_val,
                        shares=shares,
                        price_per_share=price,
                    )
                    st.session_state["last_entry"] = {
                        "entry_date": entry_date,
                        "shares_str": shares_str,
                        "cost_basis_type": cost_basis_type,
                        "custom_price_str": custom_price_str,
                        "snap_val_str": snap_val_str,
                        "note": note,
                    }
                    st.success(
                        f"{'Bought' if shares > 0 else 'Sold'} {abs(shares):g} shares "
                        f"@ ${price:,.4f} = ${abs(amount):,.2f} on {entry_date}."
                    )
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))

        else:
            # --- Generic entry form ---
            c1, c2, c3, c4 = st.columns(4)
            entry_date = c1.date_input(
                "Date",
                value=_last.get("entry_date", date.today()),
                max_value=date.today(),
            )
            shares_str = c2.text_input(
                "Shares",
                value=_last.get("shares_str", ""),
                placeholder="e.g. 10 or -5",
                help="Number of units. Positive = buy/deposit, negative = sell/withdraw.",
            )
            price_str = c3.text_input(
                "Price / share ($)",
                value=_last.get("price_str", ""),
                placeholder="e.g. 100.00",
                help="Price per share. Alternatively fill Total Cost Basis and leave this blank.",
            )
            total_cost_str = c4.text_input(
                "Total cost basis ($)",
                value=_last.get("total_cost_str", ""),
                placeholder="e.g. 1,000.00",
                help="Total dollar value. price/share = total ÷ shares.",
            )
            snap_val_str = st.text_input(
                "Portfolio value ($) — optional snapshot",
                value=_last.get("snap_val_str", ""),
                placeholder="e.g. 12,345.67",
            )
            note = st.text_input("Note (optional)", value=_last.get("note", ""))

            if st.form_submit_button("Save", type="primary"):
                try:
                    shares = _parse_shares(shares_str) if shares_str.strip() else None
                    if shares is None:
                        raise ValueError("Enter number of shares.")
                    price_ps: float | None = None
                    if price_str.strip():
                        price_ps = _parse_dollar(price_str)
                    elif total_cost_str.strip():
                        total = _parse_dollar(total_cost_str)
                        if shares == 0:
                            raise ValueError("Shares cannot be 0 when computing price from total cost.")
                        price_ps = total / shares
                    else:
                        raise ValueError("Enter either Price/share or Total cost basis.")
                    if price_ps <= 0:
                        raise ValueError("Price per share must be greater than 0.")
                    amount = shares * price_ps
                    snap_val = None
                    if snap_val_str.strip():
                        snap_val = _parse_dollar(snap_val_str)
                        if snap_val < 0:
                            raise ValueError("Portfolio value cannot be negative.")
                    storage.add_entry(
                        selected_id, amount, entry_date, note,
                        snapshot_value=snap_val,
                        shares=shares,
                        price_per_share=price_ps,
                    )
                    st.session_state["last_entry"] = {
                        "entry_date": entry_date,
                        "shares_str": shares_str,
                        "price_str": price_str,
                        "total_cost_str": total_cost_str,
                        "snap_val_str": snap_val_str,
                        "note": note,
                    }
                    st.success(
                        f"{'Buy' if shares > 0 else 'Sell'}: {abs(shares):g} × ${price_ps:,.2f} "
                        f"= ${abs(amount):,.2f} on {entry_date}."
                    )
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))

# ----- Add recurring deposits -----
with st.expander("➕ Add recurring deposits"):
    st.caption(
        "Generate multiple deposit entries at a regular cadence. End date is "
        "capped at today."
    )

    with st.form("recurring_form"):
        c1, c2 = st.columns(2)

        frequency_label = c1.selectbox(
            "Frequency",
            ["Weekly", "Biweekly (every 2 weeks)", "Semi-monthly (1st and 15th)", "Monthly"],
        )
        amount_each = c2.number_input(
            "Amount per occurrence ($)",
            min_value=0.01,
            step=50.0,
            value=100.0,
        )

        c3, c4 = st.columns(2)
        start_d = c3.date_input(
            "Start date",
            value=date.today(),
            max_value=date.today(),
            key="rec_start",
        )
        end_d = c4.date_input(
            "End date",
            value=date.today(),
            max_value=date.today(),
            key="rec_end",
        )

        day_of_week = None
        day_of_month = None
        freq_internal = None

        if frequency_label == "Weekly":
            freq_internal = "weekly"
            dow_name = st.selectbox(
                "Day of week",
                ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
            )
            day_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
                           "Saturday", "Sunday"].index(dow_name)
        elif frequency_label.startswith("Biweekly"):
            freq_internal = "biweekly"
            dow_name = st.selectbox(
                "Day of week",
                ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                key="biwk_dow",
            )
            day_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
                           "Saturday", "Sunday"].index(dow_name)
        elif frequency_label.startswith("Semi-monthly"):
            freq_internal = "semi_monthly"
            st.caption("Generates entries on the 1st and 15th of each month within range.")
        else:
            freq_internal = "monthly"
            day_of_month = st.number_input(
                "Day of month",
                min_value=1, max_value=31, value=1, step=1,
            )

        rec_note = st.text_input("Note for each entry (optional)", key="rec_note")
        preview_btn = st.form_submit_button("Preview")

    if preview_btn or st.session_state.get("recurring_preview"):
        if preview_btn:
            try:
                generated = recurring.generate_dates(
                    frequency=freq_internal,
                    start=start_d,
                    end=end_d,
                    day_of_week=day_of_week,
                    day_of_month=int(day_of_month) if day_of_month is not None else None,
                )
            except ValueError as e:
                st.error(str(e))
                generated = []

            st.session_state["recurring_preview"] = {
                "dates": [d.isoformat() for d in generated],
                "amount": float(amount_each),
                "note": rec_note,
                "account_id": selected_id,
                "account_name": selected_security,
            }

        preview = st.session_state["recurring_preview"]

        if preview["account_id"] != selected_id:
            st.session_state.pop("recurring_preview", None)
            st.warning("Account changed; please regenerate the preview.")
        else:
            n = len(preview["dates"])
            total = n * preview["amount"]
            if n == 0:
                st.warning("No dates generated for the chosen range / frequency.")
            else:
                st.info(
                    f"**Preview:** {n} entries totaling **${total:,.2f}** "
                    f"will be added to **{preview['account_name']}**."
                )
                preview_df = pl.DataFrame(
                    {
                        "Date": preview["dates"],
                        "Amount": [preview["amount"]] * n,
                        "Note": [preview["note"] or ""] * n,
                    }
                )
                st.dataframe(preview_df, use_container_width=True, hide_index=True)

                cc1, cc2, _ = st.columns([1, 1, 4])
                if cc1.button("✅ Confirm & add all", type="primary"):
                    rows = [
                        {
                            "account_id": selected_id,
                            "amount": preview["amount"],
                            "date": d,
                            "note": preview["note"],
                        }
                        for d in preview["dates"]
                    ]
                    storage.add_entries_bulk(rows)
                    st.session_state.pop("recurring_preview", None)
                    st.success(f"Added {n} entries.")
                    st.rerun()
                if cc2.button("Cancel"):
                    st.session_state.pop("recurring_preview", None)
                    st.rerun()

# ===========================================================================
# Entries table
# ===========================================================================

st.subheader("Entries")

all_entries = storage.load_entries(selected_id).sort("entry_time", descending=True)

if all_entries.is_empty():
    st.caption("No entries yet for this security.")
else:
    _fmt_dollar = lambda x: f"${x:,.2f}" if x is not None else ""
    _fmt_shares = lambda x: f"{x:g}" if x is not None else ""

    display = all_entries.with_columns(
        pl.when(pl.col("amount") > 0).then(pl.lit("Buy / Deposit"))
          .when(pl.col("amount") < 0).then(pl.lit("Sell / Withdraw"))
          .otherwise(pl.lit("Snapshot")).alias("Type"),
        pl.when(pl.col("shares") != 0.0)
          .then(pl.col("shares").map_elements(_fmt_shares, return_dtype=pl.Utf8))
          .otherwise(pl.lit("")).alias("Shares"),
        pl.when(pl.col("price_per_share") != 0.0)
          .then(pl.col("price_per_share").map_elements(_fmt_dollar, return_dtype=pl.Utf8))
          .otherwise(pl.lit("")).alias("$/Share"),
        pl.when(pl.col("amount") != 0.0)
          .then(pl.col("amount").abs().map_elements(_fmt_dollar, return_dtype=pl.Utf8))
          .otherwise(pl.lit("")).alias("Amount"),
        pl.when(pl.col("snapshot_value").is_not_null())
          .then(pl.col("snapshot_value").map_elements(_fmt_dollar, return_dtype=pl.Utf8))
          .otherwise(pl.lit("")).alias("Total Value"),
        pl.lit(False).alias("Delete?"),
    ).select(
        pl.col("Delete?"),
        pl.col("entry_time").alias("Date"),
        pl.col("Type"),
        pl.col("Shares"),
        pl.col("$/Share"),
        pl.col("Amount"),
        pl.col("Total Value"),
        pl.col("note").alias("Note"),
        pl.col("entry_id"),
    )

    edited = st.data_editor(
        display,
        column_config={
            "Delete?":     st.column_config.CheckboxColumn(default=False),
            "Date":        st.column_config.TextColumn(disabled=True),
            "Type":        st.column_config.TextColumn(disabled=True),
            "Shares":      st.column_config.TextColumn(disabled=True),
            "$/Share":     st.column_config.TextColumn(disabled=True),
            "Amount":      st.column_config.TextColumn(disabled=True),
            "Total Value": st.column_config.TextColumn(disabled=True),
            "Note":        st.column_config.TextColumn(disabled=True),
            "entry_id":    None,
        },
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
        key=f"entries_editor_{selected_id}",
    )

    if not isinstance(edited, pl.DataFrame):
        edited = pl.from_pandas(edited)

    selected_rows = edited.filter(pl.col("Delete?") == True)
    n_selected = selected_rows.height

    if st.button(
        f"🗑️ Delete selected ({n_selected})",
        disabled=n_selected == 0,
        type="primary" if n_selected > 0 else "secondary",
    ):
        to_delete = [r["entry_id"] for r in selected_rows.iter_rows(named=True) if r["entry_id"]]
        storage.remove_entries(to_delete)
        st.success(f"Deleted {n_selected} {'entry' if n_selected == 1 else 'entries'}.")
        st.rerun()


# ---------------------------------------------------------------------------
# Delete security
# ---------------------------------------------------------------------------

st.divider()
if st.button(f"Delete {selected_security}", type="secondary"):
    st.session_state["confirm_del_account"] = True

if st.session_state.get("confirm_del_account"):
    warn_col, yes_col, no_col = st.columns([4, 1, 1])
    warn_col.warning(
        f"Delete **{selected_security}** and all its entries? This cannot be undone."
    )
    if yes_col.button("Yes, delete", key="yes_del_account"):
        storage.remove_account(selected_id)
        st.session_state.pop("confirm_del_account", None)
        st.session_state.pop("selected_account_id", None)
        st.rerun()
    if no_col.button("Cancel", key="no_del_account"):
        st.session_state.pop("confirm_del_account", None)
        st.rerun()
