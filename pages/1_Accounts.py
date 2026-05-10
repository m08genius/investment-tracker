"""
Accounts page: manage account groups and securities, record entries.
"""

from __future__ import annotations

from datetime import date, datetime

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
# Add / Edit security
# ---------------------------------------------------------------------------

st.divider()

with st.expander("➕ Add a new security"):
    # All conditional widgets outside the form so they trigger reruns immediately.
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

    _add_existing_groups = storage.list_account_groups()
    _add_group_options = _add_existing_groups + (["+ New group..."] if _add_existing_groups else [])
    _add_group_sel = st.selectbox(
        "Account Group",
        options=_add_group_options if _add_group_options else ["+ New group..."],
        key="new_sec_group_sel",
    )
    _add_new_group_str = ""
    if _add_group_sel == "+ New group...":
        _add_new_group_str = st.text_input(
            "New group name",
            placeholder="e.g. Fidelity Brokerage",
            key="new_sec_group_text",
        )

    with st.form("new_account_form", clear_on_submit=True):
        new_security = st.text_input(
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
                new_group = (
                    _add_new_group_str if _add_group_sel == "+ New group..."
                    else _add_group_sel
                )
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

# Edit security (grouped with Add, displayed before per-security detail)
_edit_ticker_key = f"edit_is_ticker_{selected_id}"
if _edit_ticker_key not in st.session_state:
    st.session_state[_edit_ticker_key] = is_ticker_acct

with st.expander("✏️ Edit security"):
    _edit_type = st.radio(
        "Security type",
        ["Generic", "Ticker"],
        index=1 if st.session_state[_edit_ticker_key] else 0,
        key=f"edit_type_radio_{selected_id}",
        horizontal=True,
    )
    st.session_state[_edit_ticker_key] = (_edit_type == "Ticker")

    _edit_existing_groups = storage.list_account_groups()
    _edit_group_options = _edit_existing_groups + (["+ New group..."] if _edit_existing_groups else [])
    _edit_group_default = (
        selected_group if selected_group in _edit_existing_groups else "+ New group..."
    )
    _edit_group_sel = st.selectbox(
        "Account Group",
        options=_edit_group_options if _edit_group_options else ["+ New group..."],
        index=(_edit_group_options.index(_edit_group_default)
               if _edit_group_default in _edit_group_options else 0),
        key=f"edit_group_sel_{selected_id}",
    )
    _edit_new_group_str = ""
    if _edit_group_sel == "+ New group...":
        _edit_new_group_str = st.text_input(
            "New group name",
            value=selected_group if selected_group not in _edit_existing_groups else "",
            key=f"edit_group_text_{selected_id}",
        )

    with st.form("edit_security_form"):
        new_security_name = st.text_input("Security name", value=selected_security)

        new_ticker_val = ticker_symbol
        if st.session_state[_edit_ticker_key]:
            cached_tickers = storage.list_active_tickers()
            if not cached_tickers:
                st.warning("No tickers cached yet. Visit **Ticker Data** to add one.")
                new_ticker_val = ""
            else:
                _ticker_opts = [""] + cached_tickers
                _ticker_default_idx = (
                    _ticker_opts.index(ticker_symbol)
                    if ticker_symbol in _ticker_opts else 0
                )
                new_ticker_val = st.selectbox(
                    "Ticker symbol",
                    options=_ticker_opts,
                    index=_ticker_default_idx,
                    format_func=lambda t: t if t else "— select —",
                )

        if st.form_submit_button("Save changes"):
            try:
                _edit_is_ticker = st.session_state[_edit_ticker_key]
                _edit_group = (
                    _edit_new_group_str if _edit_group_sel == "+ New group..."
                    else _edit_group_sel
                )
                storage.update_account(
                    selected_id,
                    security=new_security_name,
                    group_name=_edit_group,
                    is_ticker=_edit_is_ticker,
                    ticker=new_ticker_val if _edit_is_ticker else "",
                )
                st.success("Security updated.")
                st.rerun()
            except ValueError as e:
                st.error(str(e))

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

    if is_ticker_acct and ticker_symbol:
        # --- Ticker entry ---
        # All widgets are outside st.form so selecting "Custom" or changing the date
        # triggers an immediate rerun and updates the price display / preview live.
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
        _cb_options = ["Custom", "Close", "Open", "High", "Low"]
        cost_basis_type = c3.selectbox(
            "Cost basis",
            _cb_options,
            index=_cb_options.index(_last.get("cost_basis_type", "Close")),
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

        # Recording amount ribbon: shares × cost basis price
        try:
            _record_shares = _parse_shares(shares_str) if shares_str.strip() else None
        except ValueError:
            _record_shares = None
        if _record_shares is not None:
            if cost_basis_type == "Custom":
                try:
                    _record_price = _parse_dollar(custom_price_str) if custom_price_str.strip() else None
                except ValueError:
                    _record_price = None
            else:
                _record_price = storage.get_ticker_price_on_date(
                    ticker_symbol, entry_date, cost_basis_type.lower()
                )
            if _record_price is not None and _record_price > 0:
                _record_total = abs(_record_shares * _record_price)
                st.info(
                    f"Recording: **{_record_shares:g} shares × ${_record_price:,.4f} "
                    f"= ${_record_total:,.2f}**"
                )


        note = st.text_input("Note (optional)", value=_last.get("note", ""))

        if st.button("Save", type="primary", key="ticker_save_btn"):
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
                auto_note = note or f"{shares:g} shares @ ${price:,.4f}"
                storage.add_entry(
                    selected_id, amount, entry_date, auto_note,
                    snapshot_value=None,
                    shares=shares,
                    price_per_share=price,
                )
                st.session_state["last_entry"] = {
                    "entry_date": entry_date,
                    "shares_str": shares_str,
                    "cost_basis_type": cost_basis_type,
                    "custom_price_str": custom_price_str,
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
        with st.form("entry_form", clear_on_submit=True):
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

# ----- Bulk import from CSV / TSV -----
with st.expander("📋 Bulk import from CSV / TSV"):
    import csv as _csv
    import io as _io

    raw_import = st.text_area(
        "Paste CSV or tab-separated data (first row = column headers)",
        height=180,
        key=f"import_raw_{selected_id}",
        placeholder="date,shares,price\n2024-01-02,10,150.00\n2024-02-15,5,155.00",
    )

    if not raw_import.strip():
        st.caption("Paste data above to begin mapping.")
    else:
        _lines = raw_import.strip().splitlines()
        _delim = "\t" if "\t" in _lines[0] else ","
        try:
            _reader = _csv.DictReader(_io.StringIO(raw_import.strip()), delimiter=_delim)
            _import_rows = list(_reader)
            _import_hdrs = list(_reader.fieldnames or [])
        except Exception as _e:
            st.error(f"Could not parse data: {_e}")
            _import_rows, _import_hdrs = [], []

        if _import_rows and _import_hdrs:
            st.caption(
                f"**{len(_import_rows)} rows** · "
                f"delimiter: {'tab' if _delim == chr(9) else 'comma'} · "
                f"columns: {', '.join(f'`{h}`' for h in _import_hdrs)}"
            )

            _SKIP = "— skip —"
            _opts = [_SKIP] + _import_hdrs

            def _guess(keywords: list[str]) -> int:
                """Return index in _opts of the first header that contains a keyword."""
                for kw in keywords:
                    for h in _import_hdrs:
                        if kw in h.lower():
                            return _opts.index(h)
                return 0

            st.markdown("**Map columns to fields:**")
            _mc1, _mc2, _mc3, _mc4, _mc5 = st.columns(5)
            _map_date   = _mc1.selectbox("Date",         _opts, index=_guess(["date","time","day"]),             key=f"imp_date_{selected_id}")
            _map_shares = _mc2.selectbox("Shares",       _opts, index=_guess(["share","qty","quantity","unit"]), key=f"imp_shares_{selected_id}")
            _map_price  = _mc3.selectbox("Price/share",  _opts, index=_guess(["price","rate"]),                  key=f"imp_price_{selected_id}")
            _map_amount = _mc4.selectbox("Total amount", _opts, index=_guess(["amount","total","value"]),        key=f"imp_amount_{selected_id}")
            _map_note   = _mc5.selectbox("Note",         _opts, index=_guess(["note","memo","desc","comment"]), key=f"imp_note_{selected_id}")

            # Parse preview
            _preview, _skip_msgs = [], []
            for _i, _row in enumerate(_import_rows):
                _lineno = _i + 2
                try:
                    _date_raw = (_row.get(_map_date) or "").strip() if _map_date != _SKIP else ""
                    if not _date_raw:
                        _skip_msgs.append(f"Row {_lineno}: date missing"); continue
                    # Try common date formats
                    for _fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y",
                                 "%Y/%m/%d", "%b-%d-%Y", "%b %d, %Y", "%B-%d-%Y",
                                 "%B %d, %Y", "%d-%b-%Y", "%d %b %Y"):
                        try:
                            _entry_dt = datetime.strptime(_date_raw, _fmt).date(); break
                        except ValueError:
                            pass
                    else:
                        raise ValueError(f"unrecognised date format: {_date_raw!r}")

                    def _num(col):
                        v = (_row.get(col) or "").strip().replace("$","").replace(",","")
                        return float(v) if v else None

                    _sh  = _num(_map_shares)  if _map_shares  != _SKIP else None
                    _pr  = _num(_map_price)   if _map_price   != _SKIP else None
                    _amt = _num(_map_amount)  if _map_amount  != _SKIP else None
                    _nt  = (_row.get(_map_note) or "").strip() if _map_note != _SKIP else ""

                    if _sh is not None and _pr is not None:
                        _fshares, _fprice, _famount = _sh, _pr, _sh * _pr
                    elif _sh is not None and _amt is not None:
                        if _sh == 0:
                            _skip_msgs.append(f"Row {_lineno}: shares=0, cannot compute price"); continue
                        _fshares, _fprice, _famount = _sh, _amt / _sh, _amt
                    elif _amt is not None:
                        _fshares, _fprice, _famount = 1.0, abs(_amt), _amt
                    else:
                        _skip_msgs.append(f"Row {_lineno}: need price/share or total amount"); continue

                    _preview.append({"Date": _entry_dt.isoformat(), "Shares": _fshares,
                                     "Price/share": _fprice, "Amount": _famount, "Note": _nt})
                except Exception as _e:
                    _skip_msgs.append(f"Row {_lineno}: {_e}")

            if _preview:
                st.markdown(f"**Preview** (first {min(5, len(_preview))} of {len(_preview)} rows):")
                st.dataframe(
                    pl.DataFrame(_preview[:5]).with_columns(
                        pl.col("Amount").map_elements(lambda x: f"${x:,.2f}", return_dtype=pl.Utf8),
                        pl.col("Price/share").map_elements(lambda x: f"${x:,.4f}", return_dtype=pl.Utf8),
                        pl.col("Shares").map_elements(lambda x: f"{x:g}", return_dtype=pl.Utf8),
                    ),
                    hide_index=True, use_container_width=True,
                )

            if _skip_msgs:
                with st.expander(f"⚠️ {len(_skip_msgs)} rows will be skipped"):
                    for _m in _skip_msgs[:30]:
                        st.caption(_m)

            if _preview and st.button(
                f"Import {len(_preview)} {'entry' if len(_preview)==1 else 'entries'}",
                key=f"imp_btn_{selected_id}", type="primary",
            ):
                _added, _errs = 0, []
                for _r in _preview:
                    try:
                        storage.add_entry(
                            selected_id, _r["Amount"], _r["Date"], _r["Note"],
                            shares=_r["Shares"], price_per_share=_r["Price/share"],
                        )
                        _added += 1
                    except Exception as _e:
                        _errs.append(str(_e))
                if _added:
                    st.success(f"Imported {_added} {'entry' if _added==1 else 'entries'}.")
                if _errs:
                    with st.expander(f"{len(_errs)} entries failed"):
                        for _m in _errs[:20]:
                            st.caption(_m)
                if _added:
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

    # For ticker accounts compute total value live (cumulative shares × close price).
    if is_ticker_acct and ticker_symbol:
        _snap_dict: dict[str, float] = {
            d.isoformat(): v
            for d, v in storage.compute_ticker_snapshots(selected_id, ticker_symbol)
        }
        _tv_series = pl.Series(
            "_tv",
            [_snap_dict.get(d) for d in all_entries["entry_time"].to_list()],
            dtype=pl.Float64,
        )
        _tv_col = (
            pl.when(pl.col("_tv").is_not_null())
            .then(pl.col("_tv").map_elements(_fmt_dollar, return_dtype=pl.Utf8))
            .otherwise(pl.lit(""))
            .alias("Total Value")
        )
        _base = all_entries.with_column(_tv_series)
    else:
        _tv_col = (
            pl.when(pl.col("snapshot_value").is_not_null())
            .then(pl.col("snapshot_value").map_elements(_fmt_dollar, return_dtype=pl.Utf8))
            .otherwise(pl.lit(""))
            .alias("Total Value")
        )
        _base = all_entries

    display = _base.with_columns(
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
        _tv_col,
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
