"""Streamlit dashboard entry point."""

import streamlit as st

st.set_page_config(
    page_title="CSE Financial Analysis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("CSE Quarterly Financial Analysis")
st.caption("Dipped Products PLC (DIPD) & Richard Pieris Exports PLC (REXP)")

# Sidebar navigation
page = st.sidebar.radio(
    "Navigation",
    ["Overview", "Company Detail", "Comparison"],
    index=0,
)

st.sidebar.divider()
st.sidebar.markdown(
    "**Data Source:** Colombo Stock Exchange (CSE)\n\n"
    "**Metrics:** Revenue, COGS, Gross Profit, Operating Expenses, Operating Income, Net Income\n\n"
    "**Units:** LKR Thousands"
)

# Render selected page
if page == "Overview":
    from src.pages.overview import render
    render()
elif page == "Company Detail":
    from src.pages.company_detail import render
    render()
elif page == "Comparison":
    from src.pages.comparison import render
    render()
