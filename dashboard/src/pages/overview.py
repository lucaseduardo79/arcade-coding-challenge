"""Overview page - multi-company dashboard with key metrics."""

import streamlit as st

from src.db_reader import get_companies, get_quarterly_standalone, get_latest_quarter_data, get_annual_summary
from src.charts import quarterly_revenue_bar, profit_margin_trend, company_comparison_radar, yoy_growth_heatmap


def render():
    st.header("Financial Overview")

    companies = get_companies()
    if companies.empty:
        st.warning("No company data found. Please run the ETL pipeline first.")
        return

    # Key metrics cards for latest quarter
    latest = get_latest_quarter_data()
    if not latest.empty:
        st.subheader("Latest Quarter Snapshot")
        cols = st.columns(len(latest))
        for i, (_, row) in enumerate(latest.iterrows()):
            with cols[i]:
                st.metric(label=row["company_id"], value=f"Rev: {row['revenue']:,.0f}")
                st.metric(label="Gross Profit", value=f"{row['gross_profit']:,.0f}")
                st.metric(label="Net Income", value=f"{row['net_income']:,.0f}")
                period = f"{row['fiscal_year']} {row['fiscal_quarter']}"
                st.caption(f"Period: {period} (LKR '000)")

    st.divider()

    # Quarterly revenue comparison
    quarterly = get_quarterly_standalone()
    if not quarterly.empty:
        st.plotly_chart(quarterly_revenue_bar(quarterly), use_container_width=True)
        st.plotly_chart(profit_margin_trend(quarterly), use_container_width=True)

    # Company comparison radar
    if not latest.empty:
        st.plotly_chart(company_comparison_radar(latest), use_container_width=True)

    # YoY growth heatmap
    annual = get_annual_summary()
    if not annual.empty:
        st.plotly_chart(yoy_growth_heatmap(annual), use_container_width=True)
