"""Plotly chart builders for the financial dashboard."""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


def quarterly_revenue_bar(df: pd.DataFrame) -> go.Figure:
    """Grouped bar chart of quarterly revenue by company."""
    df = df.sort_values("period_end")
    df["period_label"] = df["fiscal_year"] + " " + df["fiscal_quarter"]

    fig = px.bar(
        df,
        x="period_label",
        y="revenue",
        color="company_id",
        barmode="group",
        title="Quarterly Revenue (LKR '000)",
        labels={"revenue": "Revenue (LKR '000)", "period_label": "Period", "company_id": "Company"},
    )
    fig.update_layout(xaxis_tickangle=-45, height=450)
    return fig


def profit_margin_trend(df: pd.DataFrame) -> go.Figure:
    """Line chart of gross profit margin and net profit margin trends."""
    df = df.sort_values("period_end").copy()
    df["period_label"] = df["fiscal_year"] + " " + df["fiscal_quarter"]
    df["gross_margin_pct"] = (df["gross_profit"] / df["revenue"] * 100).round(1)
    df["net_margin_pct"] = (df["net_income"] / df["revenue"] * 100).round(1)

    fig = go.Figure()
    for company in df["company_id"].unique():
        cdf = df[df["company_id"] == company]
        fig.add_trace(
            go.Scatter(
                x=cdf["period_label"],
                y=cdf["gross_margin_pct"],
                mode="lines+markers",
                name=f"{company} Gross Margin %",
                line=dict(dash="solid"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=cdf["period_label"],
                y=cdf["net_margin_pct"],
                mode="lines+markers",
                name=f"{company} Net Margin %",
                line=dict(dash="dash"),
            )
        )

    fig.update_layout(
        title="Profit Margin Trends (%)",
        yaxis_title="Margin %",
        xaxis_tickangle=-45,
        height=450,
    )
    return fig


def net_income_waterfall(df_row: pd.Series) -> go.Figure:
    """Waterfall chart showing P&L breakdown for a single period."""
    labels = ["Revenue", "COGS", "Gross Profit", "OpEx", "Operating Income", "Net Income"]
    values = [
        df_row.get("revenue", 0),
        -df_row.get("cost_of_goods_sold", 0),
        df_row.get("gross_profit", 0),
        -df_row.get("operating_expenses", 0),
        df_row.get("operating_income", 0),
        df_row.get("net_income", 0),
    ]
    measures = ["absolute", "relative", "total", "relative", "total", "total"]

    fig = go.Figure(
        go.Waterfall(
            x=labels,
            y=values,
            measure=measures,
            textposition="outside",
            text=[f"{v:,.0f}" for v in values],
            connector_line_color="gray",
        )
    )
    company = df_row.get("company_id", "")
    period = f"{df_row.get('fiscal_year', '')} {df_row.get('fiscal_quarter', '')}"
    fig.update_layout(
        title=f"P&L Waterfall - {company} {period} (LKR '000)",
        yaxis_title="LKR '000",
        height=450,
    )
    return fig


def company_comparison_radar(latest_df: pd.DataFrame) -> go.Figure:
    """Radar chart comparing key metrics of companies for the latest quarter."""
    metrics = ["revenue", "gross_profit", "operating_income", "net_income"]
    metric_labels = ["Revenue", "Gross Profit", "Operating Income", "Net Income"]

    fig = go.Figure()
    for _, row in latest_df.iterrows():
        values = [row.get(m, 0) for m in metrics]
        # Normalize to max for radar visibility
        max_val = max(abs(v) for v in values) if values else 1
        normalized = [v / max_val * 100 for v in values]
        normalized.append(normalized[0])  # Close the radar

        fig.add_trace(
            go.Scatterpolar(
                r=normalized,
                theta=metric_labels + [metric_labels[0]],
                fill="toself",
                name=row["company_id"],
            )
        )

    fig.update_layout(
        title="Company Comparison - Latest Quarter (Normalized %)",
        polar=dict(radialaxis=dict(visible=True)),
        height=450,
    )
    return fig


def yoy_growth_heatmap(annual_df: pd.DataFrame) -> go.Figure:
    """Heatmap showing year-over-year growth rates."""
    metrics = ["revenue", "gross_profit", "net_income"]

    rows = []
    for company in annual_df["company_id"].unique():
        cdf = annual_df[annual_df["company_id"] == company].sort_values("fiscal_year")
        for metric in metrics:
            prev = None
            for _, row in cdf.iterrows():
                if prev is not None and prev != 0:
                    growth = ((row[metric] - prev) / abs(prev)) * 100
                    rows.append(
                        {
                            "company_metric": f"{company} - {metric.replace('_', ' ').title()}",
                            "fiscal_year": row["fiscal_year"],
                            "growth_pct": round(growth, 1),
                        }
                    )
                prev = row[metric]

    if not rows:
        fig = go.Figure()
        fig.update_layout(title="YoY Growth - Insufficient Data")
        return fig

    growth_df = pd.DataFrame(rows)
    pivot = growth_df.pivot(index="company_metric", columns="fiscal_year", values="growth_pct")

    fig = px.imshow(
        pivot,
        text_auto=".1f",
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        title="Year-over-Year Growth (%)",
        labels=dict(color="Growth %"),
    )
    fig.update_layout(height=400)
    return fig
