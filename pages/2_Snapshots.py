"""
Snapshots page: record and manage current market value snapshots per account.
Snapshots are the basis for TWRR computation on the View Performance page.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import streamlit as st

from lib import storage


st.set_page_config(page_title="Snapshots", page_icon="📸", layout="wide")
st.title("📸 Snapshots")

st.caption(
    "Record the current market value of each account on a given date. "
    "Multiple snapshots over time enable TWRR computation on the View Performance page. "
    "Adding a snapshot when you make a deposit or withdrawal gives the most accurate TWRR."
)

accounts_df = storage.load_accounts()

if accounts_df.is_empty():
    st.info("No accounts yet. Head to **Accounts** to create one.")
    st.stop()


# ---------------------------------------------------------------------------
# Account selector
# ---------------------------------------------------------------------------

account_options = {
    row["name"]: row["account_id"]
    for row in accounts_df.iter_rows(named=True)
}
selected_name = st.selectbox("Select an account:", list(account_options.keys()))
selected_id = account_options[selected_name]


# ---------------------------------------------------------------------------
# Add new snapshot
# ---------------------------------------------------------------------------

st.header("Add snapshot")

with st.form(f"snapshot_form_{selected_id}", clear_on_submit=True):
    c1, c2, c3 = st.columns([2, 2, 1])
    new_val = c1.number_input(
        "Current value ($)",
        min_value=0.0,
        step=100.0,
        value=0.0,
    )
    new_as_of = c2.date_input(
        "As of date",
        value=date.today(),
        max_value=date.today(),
    )
    if c3.form_submit_button("Save", type="primary"):
        try:
            storage.set_current_value(selected_id, new_val, new_as_of)
            st.success(f"Saved ${new_val:,.2f} as of {new_as_of.isoformat()}.")
            st.rerun()
        except ValueError as e:
            st.error(str(e))


# ---------------------------------------------------------------------------
# Snapshot history
# ---------------------------------------------------------------------------

st.divider()
st.subheader(f"Snapshot history for {selected_name}")

snaps = storage.load_current_values(selected_id).sort("as_of_date", descending=True)

if snaps.is_empty():
    st.caption("No snapshots yet for this account.")
else:
    n = snaps.height
    earliest = snaps["as_of_date"].min()
    latest = snaps["as_of_date"].max()
    st.caption(f"{n} snapshot{'s' if n != 1 else ''} · {earliest} → {latest}")

    display = snaps.with_columns(pl.lit(False).alias("Delete?")).select(
        pl.col("Delete?"),
        pl.col("as_of_date").alias("Date"),
        pl.col("value").alias("Value"),
    )

    edited = st.data_editor(
        display,
        column_config={
            "Delete?": st.column_config.CheckboxColumn(default=False),
            "Date": st.column_config.TextColumn(disabled=True),
            "Value": st.column_config.NumberColumn(format="$%.2f", disabled=True),
        },
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
        key="snaps_editor",
    )

    if not isinstance(edited, pl.DataFrame):
        edited = pl.from_pandas(edited)

    to_delete = edited.filter(pl.col("Delete?") == True)
    n_selected = to_delete.height

    if st.button(
        f"🗑️ Delete selected ({n_selected})",
        disabled=n_selected == 0,
        type="primary" if n_selected > 0 else "secondary",
    ):
        for row in to_delete.iter_rows(named=True):
            storage.remove_current_value(selected_id, row["Date"])
        st.success(f"Deleted {n_selected} snapshot{'s' if n_selected != 1 else ''}.")
        st.rerun()
