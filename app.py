"""
Investment Tracker - home page.

Run with: streamlit run app.py
"""

from __future__ import annotations

import streamlit as st

from lib import storage


st.set_page_config(
    page_title="Investment Tracker",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Investment Tracker")

st.write(
    """
    Track investment accounts, record cash flows, and compare your performance
    against market index tickers using the Money-Weighted Rate of Return (MWRR).
    """
)

# Summary stats
accounts = storage.load_accounts()
entries = storage.load_entries()
tickers = storage.list_active_tickers()

col1, col2, col3 = st.columns(3)
col1.metric("Accounts", accounts.height)
col2.metric("Entries", entries.height)
col3.metric("Cached tickers", len(tickers))

st.divider()

st.subheader("Pages")
st.write(
    """
    - **Accounts** — add/remove accounts, record deposits and withdrawals (including
      recurring), and manage portfolio value snapshots for TWRR.
    - **View Performance** — compare your MWRR and TWRR against one or more market
      index tickers.
    - **Ticker Data** — manage cached ticker price data. Add new tickers, refresh
      existing ones, and configure which price column (open/high/low/close) to use.
    """
)

if accounts.is_empty():
    st.info("👈 Start by adding an account on the **Accounts** page.")
elif not tickers:
    st.info("👈 Add a ticker on the **Ticker Data** page to enable performance comparisons.")
else:
    st.success("✅ You're set up. Head to **View Performance** to see your returns.")
