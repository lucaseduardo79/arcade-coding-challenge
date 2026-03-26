"""Company detail page - deep dive into a single company's financials."""

import streamlit as st
import pandas as pd

from src.db_reader import get_companies, get_quarterly_standalone, get_income_statements
from src.charts import net_income_waterfall, profit_margin_trend


def render():
    st.header("Company Detail")

    companies = get_companies()
    if companies.empty:
        st.warning("No company data found.")
        return

    company_options = {
        row["company_id"]: f"{row['company_id']} - {row['full_name']}"
        for _, row in companies.iterrows()
    }
    selected = st.selectbox("Select Company", options=list(company_options.keys()),
                            format_func=lambda x: company_options[x])

    quarterly = get_quarterly_standalone(selected)
    if quarterly.empty:
        st.info(f"No quarterly data available for {selected}")
        return

    # Summary table
    st.subheader("Quarterly Standalone Data (LKR '000)")
    display_cols = [
        "fiscal_year", "fiscal_quarter", "period_end", "revenue",
        "cost_of_goods_sold", "gross_profit", "operating_expenses",
        "operating_income", "net_income", "derivation_method",
    ]
    available_cols = [c for c in display_cols if c in quarterly.columns]
    st.dataframe(
        quarterly[available_cols].sort_values("period_end", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    # Margin trends for this company
    st.subheader("Profit Margin Trends")
    st.plotly_chart(profit_margin_trend(quarterly), use_container_width=True)

    # Waterfall for selected quarter
    st.subheader("P&L Waterfall")
    quarterly_sorted = quarterly.sort_values("period_end", ascending=False)
    period_labels = (
        quarterly_sorted["fiscal_year"] + " " + quarterly_sorted["fiscal_quarter"]
    ).tolist()

    selected_period = st.selectbox("Select Period", period_labels)
    if selected_period:
        fy, fq = selected_period.split(" ", 1)
        row = quarterly_sorted[
            (quarterly_sorted["fiscal_year"] == fy) & (quarterly_sorted["fiscal_quarter"] == fq)
        ]
        if not row.empty:
            st.plotly_chart(
                net_income_waterfall(row.iloc[0]),
                use_container_width=True,
            )

    # Raw income statement data
    st.subheader("Raw Extracted Data (including cumulative periods)")
    raw = get_income_statements(selected)
    if not raw.empty:
        st.dataframe(raw.sort_values("period_end", ascending=False),
                      use_container_width=True, hide_index=True)
