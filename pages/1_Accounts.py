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

    account_rows = list(accounts_df.iter_rows(named=True))

    # Group by description; named groups first (sorted), then ungrouped.
    groups: dict[str, list] = {}
    for row in account_rows:
        key = row["description"] or ""
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
                if col.button(
                    f"{'✓ ' if is_selected else ''}{row['name']}",
                    key=f"sel_{aid}",
                    type="primary" if is_selected else "secondary",
                    use_container_width=True,
                ):
                    st.session_state["selected_account_id"] = aid
                    st.session_state.pop("recurring_preview", None)
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

# ===========================================================================
# Add entry
# ===========================================================================

_last = st.session_state.get("last_entry", {})
_TYPES = ["Deposit", "Withdrawal", "Snapshot"]
_def_type_idx = _TYPES.index(_last.get("entry_type", "Deposit"))

# Type selector outside the form so switching it reruns and reshapes the form.
if "entry_type_sel" not in st.session_state:
    st.session_state["entry_type_sel"] = _last.get("entry_type", "Deposit")

with st.expander("➕ Add entry", expanded=True):
    entry_type = st.radio(
        "Type", _TYPES,
        key="entry_type_sel",
        horizontal=True,
    )

    with st.form("entry_form", clear_on_submit=True):
        if entry_type in ("Deposit", "Withdrawal"):
            _def_amount = _last.get("amount", 100.0) if _last.get("entry_type") in ("Deposit", "Withdrawal") else 100.0
            _def_date   = _last.get("entry_date", date.today()) if _last.get("entry_type") in ("Deposit", "Withdrawal") else date.today()
            _def_note   = _last.get("note", "") if _last.get("entry_type") in ("Deposit", "Withdrawal") else ""
            c1, c2 = st.columns(2)
            amount_str = c1.text_input(
                "Amount ($)",
                value=f"{_def_amount:,.2f}",
                placeholder="e.g. 1,000.00",
                help="Always enter a positive number.",
            )
            entry_date = c2.date_input("Date", value=_def_date, max_value=date.today())
            note = st.text_input("Note (optional)", value=_def_note)
            if st.form_submit_button("Add entry", type="primary"):
                try:
                    amount = _parse_dollar(amount_str)
                    if amount <= 0:
                        raise ValueError("Amount must be greater than 0.")
                    signed = amount if entry_type == "Deposit" else -amount
                    storage.add_entry(selected_id, signed, entry_date, note)
                    st.session_state["last_entry"] = {
                        "entry_type": entry_type,
                        "amount": amount,
                        "entry_date": entry_date,
                        "note": note,
                    }
                    st.success(f"Added {entry_type.lower()} of ${amount:,.2f} on {entry_date.isoformat()}.")
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))

        else:  # Snapshot
            _def_snap_val  = _last.get("snap_value", 0.0) if _last.get("entry_type") == "Snapshot" else 0.0
            _def_snap_date = _last.get("entry_date", date.today()) if _last.get("entry_type") == "Snapshot" else date.today()
            c1, c2 = st.columns(2)
            snap_val_str = c1.text_input(
                "Portfolio value ($)",
                value=f"{_def_snap_val:,.2f}" if _def_snap_val else "",
                placeholder="e.g. 12,345.67",
                help="Total market value of this account as shown in your brokerage.",
            )
            snap_date = c2.date_input("Date", value=_def_snap_date, max_value=date.today())
            if st.form_submit_button("Save snapshot", type="primary"):
                try:
                    snap_val = _parse_dollar(snap_val_str)
                    if snap_val < 0:
                        raise ValueError("Portfolio value cannot be negative.")
                    storage.set_snapshot(selected_id, snap_val, snap_date)
                    st.session_state["last_entry"] = {
                        "entry_type": "Snapshot",
                        "snap_value": snap_val,
                        "entry_date": snap_date,
                    }
                    st.success(f"Saved snapshot of ${snap_val:,.2f} on {snap_date.isoformat()}.")
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

# ===========================================================================
# Unified entries table
# ===========================================================================

st.subheader("Entries")

all_entries = storage.load_entries(selected_id).sort("entry_time", descending=True)

if all_entries.is_empty():
    st.caption("No entries yet for this account.")
else:
    display = all_entries.with_columns(
        pl.when(pl.col("amount") > 0).then(pl.lit("Deposit"))
          .when(pl.col("amount") < 0).then(pl.lit("Withdrawal"))
          .otherwise(pl.lit("Snapshot"))
          .alias("Type"),
        pl.when(pl.col("amount") != 0.0)
          .then(pl.col("amount").abs().map_elements(lambda x: f"${x:,.2f}", return_dtype=pl.Utf8))
          .otherwise(pl.lit(""))
          .alias("Amount"),
        pl.when(pl.col("amount") == 0.0)
          .then(pl.col("snapshot_value").map_elements(lambda x: f"${x:,.2f}" if x is not None else "", return_dtype=pl.Utf8))
          .otherwise(pl.lit(""))
          .alias("Total Value"),
        pl.lit(False).alias("__delete"),
    ).select(
        pl.col("__delete").alias("Delete?"),
        pl.col("entry_time").alias("Date"),
        pl.col("Type"),
        pl.col("Amount"),
        pl.col("Total Value"),
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
            "Total Value": st.column_config.TextColumn(disabled=True),
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
        st.success(f"Deleted {n_selected} {'entry' if n_selected == 1 else 'entries'}.")
        st.rerun()


# ---------------------------------------------------------------------------
# Delete account
# ---------------------------------------------------------------------------

st.divider()
if st.button(f"Delete {selected_name}", type="secondary"):
    st.session_state["confirm_del_account"] = True

if st.session_state.get("confirm_del_account"):
    warn_col, yes_col, no_col = st.columns([4, 1, 1])
    warn_col.warning(
        f"Delete **{selected_name}** and all its entries and snapshots? This cannot be undone."
    )
    if yes_col.button("Yes, delete", key="yes_del_account"):
        storage.remove_account(selected_id)
        st.session_state.pop("confirm_del_account", None)
        st.session_state.pop("selected_account_id", None)
        st.rerun()
    if no_col.button("Cancel", key="no_del_account"):
        st.session_state.pop("confirm_del_account", None)
        st.rerun()
