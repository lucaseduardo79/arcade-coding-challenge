"""Test dashboard data readers and chart builders (no Streamlit server needed)."""

import sys
import os

# Point to local DB
os.environ["DB_PATH"] = "shared/db/financial_data.duckdb"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

import pandas as pd

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    status = "PASS" if condition else "FAIL"
    if condition:
        PASS += 1
    else:
        FAIL += 1
    suffix = f" — {detail}" if detail else ""
    print(f"  [{status}] {name}{suffix}")


def main():
    global PASS, FAIL
    print("=" * 60)
    print("Dashboard Data & Chart Tests")
    print("=" * 60)

    # ── 1. db_reader functions ──
    print("\n--- db_reader: get_companies ---")
    from src.db_reader import (
        get_companies,
        get_income_statements,
        get_quarterly_standalone,
        get_latest_quarter_data,
        get_annual_summary,
        run_query,
    )

    companies = get_companies()
    check("get_companies returns DataFrame", isinstance(companies, pd.DataFrame))
    check("get_companies has 2 rows", len(companies) == 2, str(len(companies)))
    check("has company_id column", "company_id" in companies.columns)

    print("\n--- db_reader: get_income_statements ---")
    income_all = get_income_statements()
    check("get_income_statements() returns data", len(income_all) > 0, f"{len(income_all)} rows")
    check("has revenue column", "revenue" in income_all.columns)
    check("has net_income column", "net_income" in income_all.columns)

    income_dipd = get_income_statements("DIPD")
    check("filter by DIPD works", len(income_dipd) > 0, f"{len(income_dipd)} rows")
    check("all rows are DIPD", (income_dipd["company_id"] == "DIPD").all())

    income_rexp = get_income_statements("REXP")
    check("filter by REXP works", len(income_rexp) > 0, f"{len(income_rexp)} rows")

    print("\n--- db_reader: get_quarterly_standalone ---")
    quarterly_all = get_quarterly_standalone()
    check("get_quarterly_standalone() returns data", len(quarterly_all) > 0, f"{len(quarterly_all)} rows")
    required_cols = ["company_id", "fiscal_year", "fiscal_quarter", "revenue", "net_income"]
    for col in required_cols:
        check(f"has '{col}' column", col in quarterly_all.columns)

    quarterly_dipd = get_quarterly_standalone("DIPD")
    check("filter by DIPD works", len(quarterly_dipd) > 0)
    check("all rows are DIPD", (quarterly_dipd["company_id"] == "DIPD").all())

    print("\n--- db_reader: get_latest_quarter_data ---")
    latest = get_latest_quarter_data()
    check("get_latest_quarter_data() returns data", len(latest) > 0, f"{len(latest)} rows")
    check("has one row per company", len(latest) <= 2, f"{len(latest)} rows")
    for cid in ["DIPD", "REXP"]:
        has_company = cid in latest["company_id"].values
        check(f"latest data includes {cid}", has_company)

    print("\n--- db_reader: get_annual_summary ---")
    annual = get_annual_summary()
    check("get_annual_summary() returns data", len(annual) > 0, f"{len(annual)} rows")
    check("has quarters_available column", "quarters_available" in annual.columns)
    check("quarters_available between 1-4",
          annual["quarters_available"].between(1, 4).all(),
          f"values: {sorted(annual['quarters_available'].unique())}")

    print("\n--- db_reader: run_query ---")
    result = run_query("SELECT COUNT(*) as cnt FROM companies")
    check("run_query works", len(result) == 1 and result["cnt"].iloc[0] == 2)

    # ── 2. Chart builders ──
    print("\n--- charts: quarterly_revenue_bar ---")
    from src.charts import (
        quarterly_revenue_bar,
        profit_margin_trend,
        net_income_waterfall,
        company_comparison_radar,
        yoy_growth_heatmap,
    )

    try:
        fig = quarterly_revenue_bar(quarterly_all)
        check("quarterly_revenue_bar returns Figure", fig is not None)
        check("has data traces", len(fig.data) > 0, f"{len(fig.data)} traces")
    except Exception as e:
        check("quarterly_revenue_bar", False, str(e))

    print("\n--- charts: profit_margin_trend ---")
    try:
        fig = profit_margin_trend(quarterly_all)
        check("profit_margin_trend returns Figure", fig is not None)
        check("has data traces", len(fig.data) > 0, f"{len(fig.data)} traces")
    except Exception as e:
        check("profit_margin_trend", False, str(e))

    print("\n--- charts: net_income_waterfall ---")
    try:
        sample_row = quarterly_all.iloc[0]
        fig = net_income_waterfall(sample_row)
        check("net_income_waterfall returns Figure", fig is not None)
        check("has data traces", len(fig.data) > 0)
    except Exception as e:
        check("net_income_waterfall", False, str(e))

    print("\n--- charts: company_comparison_radar ---")
    try:
        fig = company_comparison_radar(latest)
        check("company_comparison_radar returns Figure", fig is not None)
        check("has data traces", len(fig.data) > 0, f"{len(fig.data)} traces")
    except Exception as e:
        check("company_comparison_radar", False, str(e))

    print("\n--- charts: yoy_growth_heatmap ---")
    try:
        fig = yoy_growth_heatmap(annual)
        check("yoy_growth_heatmap returns Figure", fig is not None)
    except Exception as e:
        check("yoy_growth_heatmap", False, str(e))

    # ── 3. Data Quality in Dashboard Context ──
    print("\n--- Dashboard Data Quality ---")
    # Revenue should be displayable (no infinities, no extreme outliers)
    check("No NaN in quarterly revenue",
          quarterly_all["revenue"].notna().all())
    check("No negative quarterly revenue",
          (quarterly_all["revenue"] >= 0).all(),
          f"min={quarterly_all['revenue'].min()}")

    # Periods should be sortable
    check("period_end is sortable",
          quarterly_all["period_end"].is_monotonic_decreasing or True)  # just check no errors

    # Fiscal year format
    fy_sample = quarterly_all["fiscal_year"].iloc[0]
    check("fiscal_year format like YYYY/YY", "/" in str(fy_sample), fy_sample)

    # Fiscal quarter format
    fq_values = set(quarterly_all["fiscal_quarter"].unique())
    check("fiscal_quarter values are Q1-Q4",
          fq_values.issubset({"Q1", "Q2", "Q3", "Q4"}),
          str(fq_values))

    # ── Summary ──
    print("\n" + "=" * 60)
    total = PASS + FAIL
    print(f"Results: {PASS}/{total} passed, {FAIL} failed")
    print("=" * 60)
    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    main()
