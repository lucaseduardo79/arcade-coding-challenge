"""Comparison page - side-by-side analysis of two companies."""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from src.db_reader import get_quarterly_standalone, get_annual_summary


def render():
    st.header("Company Comparison")

    quarterly = get_quarterly_standalone()
    if quarterly.empty:
        st.warning("No data available for comparison.")
        return

    companies = quarterly["company_id"].unique().tolist()
    if len(companies) < 2:
        st.info("Need at least 2 companies for comparison.")
        return

    col1, col2 = st.columns(2)
    with col1:
        company_a = st.selectbox("Company A", companies, index=0)
    with col2:
        company_b = st.selectbox("Company B", companies, index=min(1, len(companies) - 1))

    metric = st.selectbox(
        "Metric to Compare",
        ["revenue", "gross_profit", "operating_income", "net_income", "cost_of_goods_sold", "operating_expenses"],
        format_func=lambda x: x.replace("_", " ").title(),
    )

    # Side-by-side quarterly trend
    st.subheader(f"{metric.replace('_', ' ').title()} - Quarterly Trend")

    df_a = quarterly[quarterly["company_id"] == company_a].sort_values("period_end")
    df_b = quarterly[quarterly["company_id"] == company_b].sort_values("period_end")

    fig = go.Figure()
    if not df_a.empty:
        labels_a = df_a["fiscal_year"] + " " + df_a["fiscal_quarter"]
        fig.add_trace(
            go.Bar(x=labels_a, y=df_a[metric], name=company_a, marker_color="#636EFA")
        )
    if not df_b.empty:
        labels_b = df_b["fiscal_year"] + " " + df_b["fiscal_quarter"]
        fig.add_trace(
            go.Bar(x=labels_b, y=df_b[metric], name=company_b, marker_color="#EF553B")
        )

    fig.update_layout(
        barmode="group",
        yaxis_title="LKR '000",
        xaxis_tickangle=-45,
        height=450,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Delta analysis table
    st.subheader("Quarter-by-Quarter Delta")
    merged = pd.merge(
        df_a[["fiscal_year", "fiscal_quarter", metric]],
        df_b[["fiscal_year", "fiscal_quarter", metric]],
        on=["fiscal_year", "fiscal_quarter"],
        suffixes=(f"_{company_a}", f"_{company_b}"),
        how="outer",
    ).sort_values(["fiscal_year", "fiscal_quarter"], ascending=False)

    col_a = f"{metric}_{company_a}"
    col_b = f"{metric}_{company_b}"
    if col_a in merged.columns and col_b in merged.columns:
        merged["delta"] = merged[col_a].fillna(0) - merged[col_b].fillna(0)
        merged["delta_pct"] = (
            merged["delta"] / merged[col_b].abs().replace(0, float("nan")) * 100
        ).round(1)

    st.dataframe(merged, use_container_width=True, hide_index=True)

    # Annual comparison
    st.subheader("Annual Totals Comparison")
    annual = get_annual_summary()
    if not annual.empty:
        annual_a = annual[annual["company_id"] == company_a][["fiscal_year", metric]].rename(
            columns={metric: company_a}
        )
        annual_b = annual[annual["company_id"] == company_b][["fiscal_year", metric]].rename(
            columns={metric: company_b}
        )
        annual_merged = pd.merge(annual_a, annual_b, on="fiscal_year", how="outer").sort_values(
            "fiscal_year", ascending=False
        )
        st.dataframe(annual_merged, use_container_width=True, hide_index=True)
