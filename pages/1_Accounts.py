"""
Accounts page: add/remove accounts, record deposit entries (single and
recurring), manage portfolio value snapshots.
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


st.set_page_config(page_title="Accounts", page_icon="💼", layout="wide")
st.title("💼 Accounts")


# ---------------------------------------------------------------------------
# Accounts list and creation
# ---------------------------------------------------------------------------

st.header("Your accounts")

accounts_df = storage.load_accounts()

if accounts_df.is_empty():
    st.info("No accounts yet. Add one below to get started.")
else:
    # Initialise selected account in session state.
    account_ids = [r["account_id"] for r in accounts_df.iter_rows(named=True)]
    if "selected_account_id" not in st.session_state or \
            st.session_state["selected_account_id"] not in account_ids:
        st.session_state["selected_account_id"] = account_ids[0]

    for row in accounts_df.iter_rows(named=True):
        aid = row["account_id"]
        is_selected = st.session_state["selected_account_id"] == aid

        c1, c2, c3 = st.columns([3, 5, 1])

        btn_label = f"{'✓ ' if is_selected else ''}{row['name']}"
        if c1.button(
            btn_label,
            key=f"sel_{aid}",
            type="primary" if is_selected else "secondary",
            use_container_width=True,
        ):
            st.session_state["selected_account_id"] = aid
            st.session_state.pop("recurring_preview", None)
            st.rerun()

        c2.caption(row["description"] or "—")

        if c3.button("🗑️", key=f"del_acct_{aid}",
                     help="Delete account and ALL its entries (cannot be undone)"):
            st.session_state[f"confirm_del_{aid}"] = True

        if st.session_state.get(f"confirm_del_{aid}"):
            warn_col, yes_col, no_col = st.columns([4, 1, 1])
            warn_col.warning(
                f"Delete **{row['name']}** and all its entries? This cannot be undone."
            )
            if yes_col.button("Yes, delete", key=f"yes_del_{aid}"):
                storage.remove_account(aid)
                st.session_state.pop(f"confirm_del_{aid}", None)
                if st.session_state.get("selected_account_id") == aid:
                    st.session_state.pop("selected_account_id", None)
                st.rerun()
            if no_col.button("Cancel", key=f"no_del_{aid}"):
                st.session_state.pop(f"confirm_del_{aid}", None)
                st.rerun()

with st.expander("➕ Add a new account"):
    with st.form("new_account_form", clear_on_submit=True):
        new_name = st.text_input("Name", placeholder="e.g. Brokerage, Roth IRA")
        new_desc = st.text_input("Description (optional)")
        submitted = st.form_submit_button("Create account")
        if submitted:
            try:
                storage.add_account(new_name, new_desc)
                st.success(f"Created account: {new_name}")
                st.rerun()
            except ValueError as e:
                st.error(str(e))


# ---------------------------------------------------------------------------
# Per-account section
# ---------------------------------------------------------------------------

if accounts_df.is_empty():
    st.stop()

selected_id = st.session_state["selected_account_id"]
selected_name = storage.get_account(selected_id)["name"]

st.divider()
st.header(f"{selected_name}")

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

tab_deposits, tab_snapshots = st.tabs(["Deposit entries", "Snapshot entries"])


# ===========================================================================
# Tab 1: Deposit entries
# ===========================================================================

with tab_deposits:

    # ----- Add single entry -----
    _last = st.session_state.get("last_single_entry", {})
    _def_type_idx = 0 if _last.get("entry_type", "Deposit") == "Deposit" else 1
    _def_amount = _last.get("amount", 100.0)
    _def_date = _last.get("entry_date", date.today())
    _def_note = _last.get("note", "")
    _def_save_snap = _last.get("save_snapshot", False)

    with st.expander("➕ Add single entry", expanded=True):
        with st.form("single_entry_form", clear_on_submit=True):
            c1, c2, c3 = st.columns([1, 1, 1])
            entry_type = c1.radio(
                "Type",
                ["Deposit", "Withdrawal"],
                index=_def_type_idx,
                horizontal=True,
            )
            amount_str = c2.text_input(
                "Amount ($)",
                value=f"{_def_amount:,.2f}",
                placeholder="e.g. 1,000.00",
                help="Always enter a positive number. Withdrawals are stored as negative internally.",
            )
            entry_date = c3.date_input(
                "Date",
                value=_def_date,
                max_value=date.today(),
            )
            st.divider()
            sc1, sc2 = st.columns([1, 2])
            save_snapshot = sc1.checkbox(
                "Also record snapshot entry",
                value=_def_save_snap,
                help="Save a snapshot entry on the same date. "
                     "Useful for TWRR on the View Performance page.",
            )
            snapshot_value_str = sc2.text_input(
                "Snapshot value ($)",
                value="",
                placeholder="e.g. 12,345.67",
                key="single_entry_snapshot_val",
                help="Enter the total portfolio value as shown in your brokerage account "
                     "after this transaction has settled. This already includes the deposit/withdrawal.",
            )

            note = st.text_input("Note (optional)", value=_def_note)

            submitted = st.form_submit_button("Add entry")
            if submitted:
                try:
                    amount = _parse_dollar(amount_str)
                    if amount <= 0:
                        raise ValueError("Amount must be greater than 0.")
                    signed = amount if entry_type == "Deposit" else -amount
                    storage.add_entry(selected_id, signed, entry_date, note)
                    st.session_state["last_single_entry"] = {
                        "entry_type": entry_type,
                        "amount": amount,
                        "entry_date": entry_date,
                        "note": note,
                        "save_snapshot": save_snapshot,
                    }
                    msg = f"Added {entry_type.lower()} of ${amount:,.2f} on {entry_date.isoformat()}"
                    if save_snapshot:
                        snapshot_value = _parse_dollar(snapshot_value_str) if snapshot_value_str.strip() else 0.0
                        storage.set_current_value(selected_id, snapshot_value, entry_date)
                        msg += f" · saved snapshot entry ${snapshot_value:,.2f}"
                    st.success(msg)
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
                    help="If the chosen day doesn't exist in a given month (e.g. 31 in February), the last day of the month is used.",
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
                    "account_name": selected_name,
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

    # ----- Entries table -----
    st.subheader(f"Entries")

    entries = storage.load_entries(selected_id).sort("date", descending=True)

    if entries.is_empty():
        st.caption("No entries yet for this account.")
    else:
        display = entries.with_columns(
            pl.when(pl.col("amount") >= 0).then(pl.lit("Deposit")).otherwise(pl.lit("Withdrawal")).alias("Type"),
            pl.col("amount").abs().map_elements(lambda x: f"${x:,.2f}", return_dtype=pl.Utf8).alias("Amount"),
            pl.lit(False).alias("__delete"),
        ).select(
            pl.col("__delete").alias("Delete?"),
            pl.col("date").alias("Date"),
            pl.col("Type"),
            pl.col("Amount"),
            pl.col("note").alias("Note"),
            pl.col("entry_id"),
        )

        edited = st.data_editor(
            display,
            column_config={
                "Delete?": st.column_config.CheckboxColumn(default=False),
                "Date": st.column_config.TextColumn(disabled=True),
                "Type": st.column_config.TextColumn(disabled=True),
                "Amount": st.column_config.TextColumn(disabled=True),
                "Note": st.column_config.TextColumn(disabled=True),
                "entry_id": None,
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
            storage.remove_entries(selected_rows["entry_id"].to_list())
            st.success(f"Deleted {n_selected} entries.")
            st.rerun()


# ===========================================================================
# Tab 2: Snapshot entries
# ===========================================================================

with tab_snapshots:
    st.caption(
        "Record the current market value of this account on a given date. "
        "Multiple snapshots over time enable TWRR on the View Performance page."
    )

    # ----- Add snapshot -----
    _last_snap = st.session_state.get(f"last_snapshot_{selected_id}", {})
    _def_snap_val = _last_snap.get("value", 0.0)
    _def_snap_date = _last_snap.get("as_of_date", date.today())

    with st.expander("➕ Add snapshot", expanded=True):
        with st.form(f"snapshot_form_{selected_id}", clear_on_submit=True):
            c1, c2, c3 = st.columns([2, 2, 1])
            new_val_str = c1.text_input(
                "Snapshot value ($)",
                value=f"{_def_snap_val:,.2f}" if _def_snap_val else "",
                placeholder="e.g. 12,345.67",
                help="Enter the total portfolio value as shown in your brokerage account. "
                     "This should reflect all settled transactions on this date.",
            )
            new_as_of = c2.date_input("As of date", value=_def_snap_date, max_value=date.today())
            if c3.form_submit_button("Save", type="primary"):
                try:
                    new_val = _parse_dollar(new_val_str)
                    if new_val < 0:
                        raise ValueError("Snapshot value cannot be negative.")
                    storage.set_current_value(selected_id, new_val, new_as_of)
                    st.session_state[f"last_snapshot_{selected_id}"] = {
                        "value": new_val,
                        "as_of_date": new_as_of,
                    }
                    st.success(f"Saved ${new_val:,.2f} as of {new_as_of.isoformat()}.")
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))

    # ----- Snapshot history -----
    st.subheader("Snapshot history")

    snaps = storage.load_current_values(selected_id).sort("as_of_date", descending=True)

    if snaps.is_empty():
        st.caption("No snapshots yet for this account.")
    else:
        n = snaps.height
        earliest = snaps["as_of_date"].min()
        latest = snaps["as_of_date"].max()
        st.caption(f"{n} snapshot{'s' if n != 1 else ''} · {earliest} → {latest}")

        snap_display = snaps.with_columns(
            pl.lit(False).alias("Delete?"),
            pl.col("value").map_elements(lambda x: f"${x:,.2f}", return_dtype=pl.Utf8).alias("Value"),
        ).select(
            pl.col("Delete?"),
            pl.col("as_of_date").alias("Date"),
            pl.col("Value"),
        )

        snap_edited = st.data_editor(
            snap_display,
            column_config={
                "Delete?": st.column_config.CheckboxColumn(default=False),
                "Date": st.column_config.TextColumn(disabled=True),
                "Value": st.column_config.TextColumn(disabled=True),
            },
            hide_index=True,
            use_container_width=True,
            num_rows="fixed",
            key=f"snaps_editor_{selected_id}",
        )

        if not isinstance(snap_edited, pl.DataFrame):
            snap_edited = pl.from_pandas(snap_edited)

        snaps_to_delete = snap_edited.filter(pl.col("Delete?") == True)
        n_snap_selected = snaps_to_delete.height

        if st.button(
            f"🗑️ Delete selected ({n_snap_selected})",
            disabled=n_snap_selected == 0,
            type="primary" if n_snap_selected > 0 else "secondary",
            key="del_snaps_btn",
        ):
            for row in snaps_to_delete.iter_rows(named=True):
                storage.remove_current_value(selected_id, row["Date"])
            st.success(f"Deleted {n_snap_selected} snapshot{'s' if n_snap_selected != 1 else ''}.")
            st.rerun()
