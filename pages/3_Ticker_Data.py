"""
Ticker Data page: manage cached price history for index tickers used in
performance comparisons.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import streamlit as st

from lib import storage, tickers


st.set_page_config(page_title="Ticker Data", page_icon="📊", layout="wide")
st.title("📊 Ticker Data")

st.write(
    """
    Cache market index price history locally. The first time you add a ticker,
    data is fetched from October 1, 2012 through today. Subsequent refreshes
    only fetch new data since the last refresh.
    """
)


# ---------------------------------------------------------------------------
# Add new ticker
# ---------------------------------------------------------------------------

with st.expander("➕ Add a new ticker", expanded=False):
    with st.form("add_ticker_form", clear_on_submit=True):
        new_symbol = st.text_input(
            "Ticker symbol",
            placeholder="e.g. VOO, FXAIX, SPY, QQQ",
        ).strip().upper()
        c1, c2 = st.columns(2)
        start_d = c1.date_input(
            "Fetch from",
            value=date(2012, 10, 1),
            max_value=date.today(),
            help="First date of price history to fetch. Default: 2012-10-01.",
        )
        end_d = c2.date_input(
            "Fetch through",
            value=date.today(),
            max_value=date.today(),
        )
        submitted = st.form_submit_button("Fetch and add")
        if submitted:
            if not new_symbol:
                st.error("Please enter a ticker symbol.")
            else:
                with st.spinner(f"Fetching {new_symbol} from {start_d} to {end_d}..."):
                    try:
                        meta = tickers.add_ticker(new_symbol, start=start_d, end=end_d)
                        st.success(
                            f"Added **{meta['ticker']}** "
                            f"({meta['earliest_date']} → {meta['latest_date']}) "
                            + ("· detected as close-only (mutual fund)"
                               if meta["close_only"] else "")
                        )
                        st.rerun()
                    except Exception as e:  # noqa: BLE001
                        st.error(f"Failed to add {new_symbol}: {e}")


# ---------------------------------------------------------------------------
# Cached tickers table
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Cached tickers")

cached = storage.list_active_tickers()
metadata = storage.load_ticker_metadata()

if not cached:
    st.info("No tickers cached yet. Add one above to get started.")
    st.stop()


# Global refresh
if st.button("🔄 Refresh all tickers"):
    progress = st.progress(0.0)
    status = st.empty()
    for i, t in enumerate(cached):
        status.write(f"Refreshing {t}...")
        try:
            tickers.refresh_ticker(t)
        except Exception as e:  # noqa: BLE001
            st.error(f"Failed to refresh {t}: {e}")
        progress.progress((i + 1) / len(cached))
    status.write("Done.")
    st.rerun()


# Per-ticker rows
for t in cached:
    meta = storage.get_ticker_metadata(t)
    if meta is None:
        continue   # stale list

    with st.container(border=True):
        c1, c2, c3, c4, c5 = st.columns([2, 3, 2, 2, 2])

        c1.markdown(f"### {t}")
        if meta["close_only"]:
            c1.caption("🔒 Close-only (mutual fund)")

        c2.caption("Date range")
        c2.write(f"{meta['earliest_date']} → {meta['latest_date']}")

        c3.caption("Last refreshed")
        c3.write((meta["last_refreshed"] or "—").split("T")[0])

        # Price-type selector
        c4.caption("Price type")
        if meta["close_only"]:
            c4.write("Close")
        else:
            options = ["open", "high", "low", "close"]
            current = meta["price_type"] if meta["price_type"] in options else "close"
            new_pt = c4.selectbox(
                "Price type",
                options,
                index=options.index(current),
                key=f"pt_{t}",
                label_visibility="collapsed",
            )
            if new_pt != current:
                storage.set_ticker_price_type(t, new_pt)
                st.rerun()

        # Action buttons
        c5.caption("Actions")
        ac1, ac2 = c5.columns(2)
        if ac1.button("🔄", key=f"refresh_{t}", help="Refresh this ticker"):
            with st.spinner(f"Refreshing {t}..."):
                try:
                    tickers.refresh_ticker(t)
                    st.success(f"Refreshed {t}.")
                    st.rerun()
                except Exception as e:  # noqa: BLE001
                    st.error(f"Failed: {e}")
        if ac2.button("🗑️", key=f"remove_{t}", help="Hide this ticker from the UI (price data is kept on disk)"):
            st.session_state[f"confirm_remove_{t}"] = True

        if st.session_state.get(f"confirm_remove_{t}"):
            wcol, ycol, ncol = st.columns([4, 1, 1])
            wcol.warning(f"Hide **{t}** from the UI? Price data is kept on disk — re-adding it will only fetch new prices.")
            if ycol.button("Yes", key=f"yes_remove_{t}"):
                storage.remove_ticker(t)
                st.session_state.pop(f"confirm_remove_{t}", None)
                st.rerun()
            if ncol.button("No", key=f"no_remove_{t}"):
                st.session_state.pop(f"confirm_remove_{t}", None)
                st.rerun()


# ---------------------------------------------------------------------------
# Optional: preview a few rows
# ---------------------------------------------------------------------------

st.divider()
with st.expander("Preview cached prices"):
    pick = st.selectbox("Ticker", cached, key="preview_pick")
    if pick:
        df = storage.load_ticker_prices(pick)
        st.caption(f"{df.height:,} rows")
        st.dataframe(df.tail(50), use_container_width=True, hide_index=True)
