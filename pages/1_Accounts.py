"""
Accounts page: add/remove accounts, record entries (single and recurring),
view and bulk-delete entries.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import streamlit as st

from lib import recurring, storage


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
    # Show accounts with a delete button per row.
    for row in accounts_df.iter_rows(named=True):
        c1, c2, c3 = st.columns([3, 5, 1])
        c1.markdown(f"**{row['name']}**")
        c2.caption(row["description"] or "—")
        if c3.button("🗑️", key=f"del_acct_{row['account_id']}",
                     help="Delete account and ALL its entries (cannot be undone)"):
            st.session_state[f"confirm_del_{row['account_id']}"] = True

        if st.session_state.get(f"confirm_del_{row['account_id']}"):
            warn_col, yes_col, no_col = st.columns([4, 1, 1])
            warn_col.warning(
                f"Delete **{row['name']}** and all its entries? This cannot be undone."
            )
            if yes_col.button("Yes, delete", key=f"yes_del_{row['account_id']}"):
                storage.remove_account(row["account_id"])
                st.session_state.pop(f"confirm_del_{row['account_id']}", None)
                st.rerun()
            if no_col.button("Cancel", key=f"no_del_{row['account_id']}"):
                st.session_state.pop(f"confirm_del_{row['account_id']}", None)
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
# Per-account entries
# ---------------------------------------------------------------------------

if accounts_df.is_empty():
    st.stop()

st.divider()
st.header("Entries")

# Account selector
account_options = {row["name"]: row["account_id"] for row in accounts_df.iter_rows(named=True)}
selected_name = st.selectbox(
    "Select an account to view or add entries:",
    list(account_options.keys()),
)
selected_id = account_options[selected_name]


# ----- Tabs for adding entries -----

tab_single, tab_recurring = st.tabs(["Add single entry", "Add recurring deposits"])

with tab_single:
    with st.form("single_entry_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1, 1, 1])
        entry_type = c1.radio(
            "Type",
            ["Deposit", "Withdrawal"],
            horizontal=True,
        )
        amount = c2.number_input(
            "Amount ($)",
            min_value=0.01,
            step=100.0,
            value=100.0,
            help="Always enter a positive number. Withdrawals are stored as negative internally.",
        )
        entry_date = c3.date_input(
            "Date",
            value=date.today(),
            max_value=date.today(),
        )
        note = st.text_input("Note (optional)")
        submitted = st.form_submit_button("Add entry")
        if submitted:
            try:
                signed = amount if entry_type == "Deposit" else -amount
                storage.add_entry(selected_id, signed, entry_date, note)
                st.success(
                    f"Added {entry_type.lower()} of ${amount:,.2f} on {entry_date.isoformat()}"
                )
                st.rerun()
            except ValueError as e:
                st.error(str(e))


with tab_recurring:
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

        # Conditional inputs based on frequency
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
        else:  # Monthly
            freq_internal = "monthly"
            day_of_month = st.number_input(
                "Day of month",
                min_value=1, max_value=31, value=1, step=1,
                help="If the chosen day doesn't exist in a given month (e.g. 31 in February), the last day of the month is used.",
            )

        rec_note = st.text_input("Note for each entry (optional)", key="rec_note")
        preview_btn = st.form_submit_button("Preview")

    # Preview & confirm flow lives outside the form so we can show generated rows.
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

        # Account selection might have changed since last preview.
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


# ---------------------------------------------------------------------------
# Entries table with bulk delete
# ---------------------------------------------------------------------------

st.divider()
st.subheader(f"Entries for {selected_name}")

entries = storage.load_entries(selected_id).sort("date", descending=True)

if entries.is_empty():
    st.caption("No entries yet for this account.")
else:
    # Build a display frame with type, formatted amount, etc.
    display = entries.with_columns(
        pl.when(pl.col("amount") >= 0).then(pl.lit("Deposit")).otherwise(pl.lit("Withdrawal")).alias("Type"),
        pl.col("amount").abs().alias("AbsAmount"),
        pl.lit(False).alias("__delete"),
    ).select(
        pl.col("__delete").alias("Delete?"),
        pl.col("date").alias("Date"),
        pl.col("Type"),
        pl.col("AbsAmount").alias("Amount"),
        pl.col("note").alias("Note"),
        pl.col("entry_id"),     # hidden but needed for delete
    )

    # st.data_editor returns a pandas frame in older versions, polars in newer; coerce.
    edited = st.data_editor(
        display,
        column_config={
            "Delete?": st.column_config.CheckboxColumn(default=False),
            "Date": st.column_config.TextColumn(disabled=True),
            "Type": st.column_config.TextColumn(disabled=True),
            "Amount": st.column_config.NumberColumn(format="$%.2f", disabled=True),
            "Note": st.column_config.TextColumn(disabled=True),
            "entry_id": None,    # hide
        },
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
        key="entries_editor",
    )

    # Coerce edited result into a polars frame for selection
    if not isinstance(edited, pl.DataFrame):
        edited = pl.from_pandas(edited)

    selected_rows = edited.filter(pl.col("Delete?") == True)
    n_selected = selected_rows.height

    c1, c2 = st.columns([1, 5])
    if c1.button(
        f"🗑️ Delete selected ({n_selected})",
        disabled=n_selected == 0,
        type="primary" if n_selected > 0 else "secondary",
    ):
        ids_to_remove = selected_rows["entry_id"].to_list()
        storage.remove_entries(ids_to_remove)
        st.success(f"Deleted {n_selected} entries.")
        st.rerun()

    # Quick stats footer
    total_in = entries.filter(pl.col("amount") > 0)["amount"].sum() or 0
    total_out = -1 * (entries.filter(pl.col("amount") < 0)["amount"].sum() or 0)
    net = total_in - total_out
    st.caption(
        f"Total deposited: **${total_in:,.2f}** &nbsp;&nbsp; "
        f"Total withdrawn: **${total_out:,.2f}** &nbsp;&nbsp; "
        f"Net contributed: **${net:,.2f}**"
    )
