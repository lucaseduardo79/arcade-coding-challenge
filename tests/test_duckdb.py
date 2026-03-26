"""Test DuckDB data integrity and completeness."""

import sys
import duckdb

DB_PATH = "shared/db/financial_data.duckdb"

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
    print("DuckDB Data Integrity Tests")
    print("=" * 60)

    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
    except Exception as e:
        print(f"FATAL: Cannot open database at {DB_PATH}: {e}")
        sys.exit(1)

    # ── 1. Schema existence ──
    print("\n--- Schema ---")
    tables = conn.execute("SHOW TABLES").fetchdf()
    table_names = set(tables.iloc[:, 0].tolist())
    expected_tables = {"companies", "report_sources", "income_statement", "quarterly_standalone"}
    for t in expected_tables:
        check(f"Table '{t}' exists", t in table_names)

    # ── 2. Companies ──
    print("\n--- Companies ---")
    companies = conn.execute("SELECT * FROM companies ORDER BY company_id").fetchdf()
    check("Companies table has 2 rows", len(companies) == 2, f"got {len(companies)}")
    company_ids = set(companies["company_id"].tolist())
    check("DIPD exists", "DIPD" in company_ids)
    check("REXP exists", "REXP" in company_ids)

    for _, row in companies.iterrows():
        cid = row["company_id"]
        check(f"{cid} has symbol", bool(row["symbol"]))
        check(f"{cid} has full_name", bool(row["full_name"]))
        check(f"{cid} has security_id > 0", row["security_id"] > 0, str(row["security_id"]))

    # ── 3. Report Sources ──
    print("\n--- Report Sources ---")
    sources = conn.execute("SELECT * FROM report_sources").fetchdf()
    check("report_sources has data", len(sources) > 0, f"{len(sources)} rows")

    for cid in ["DIPD", "REXP"]:
        count = len(sources[sources["company_id"] == cid])
        check(f"{cid} has report sources", count > 0, f"{count} reports")

    # Check required fields are populated
    check("All sources have pdf_filename", sources["pdf_filename"].notna().all())
    check("All sources have period_end", sources["period_end"].notna().all())
    check("All sources have period_months", sources["period_months"].notna().all())
    check("period_months are valid", set(sources["period_months"].unique()).issubset({3, 6, 9, 12}),
          str(sorted(sources["period_months"].unique())))

    # ── 4. Income Statement ──
    print("\n--- Income Statement ---")
    income = conn.execute("SELECT * FROM income_statement ORDER BY company_id, period_end").fetchdf()
    check("income_statement has data", len(income) > 0, f"{len(income)} rows")

    for cid in ["DIPD", "REXP"]:
        cdf = income[income["company_id"] == cid]
        check(f"{cid} has income records", len(cdf) > 0, f"{len(cdf)} rows")

    # Check no NULL in critical numeric fields
    critical_fields = ["revenue", "cost_of_goods_sold", "gross_profit", "net_income"]
    for field in critical_fields:
        nulls = income[field].isna().sum()
        check(f"income_statement.{field} has no NULLs", nulls == 0, f"{nulls} NULLs")

    # Check revenue is positive
    bad_revenue = income[income["revenue"] <= 0]
    check("All revenue values are positive", len(bad_revenue) == 0,
          f"{len(bad_revenue)} rows with revenue <= 0")

    # Check fiscal year/quarter populated
    check("fiscal_year populated", income["fiscal_year"].notna().all())
    check("fiscal_quarter populated", income["fiscal_quarter"].notna().all())

    # Check no exact duplicates
    dupes = income.duplicated(subset=["company_id", "period_end", "period_months", "is_cumulative"])
    check("No duplicate records", dupes.sum() == 0, f"{dupes.sum()} duplicates")

    # ── 5. Quarterly Standalone ──
    print("\n--- Quarterly Standalone ---")
    quarterly = conn.execute("SELECT * FROM quarterly_standalone ORDER BY company_id, period_end").fetchdf()
    check("quarterly_standalone has data", len(quarterly) > 0, f"{len(quarterly)} rows")

    for cid in ["DIPD", "REXP"]:
        cdf = quarterly[quarterly["company_id"] == cid]
        check(f"{cid} has quarterly data", len(cdf) > 0, f"{len(cdf)} quarters")

    # Check derivation_method
    methods = set(quarterly["derivation_method"].dropna().unique())
    check("derivation_method is valid", methods.issubset({"direct", "computed_from_cumulative"}),
          str(methods))

    # Check no duplicate company+fy+quarter
    q_dupes = quarterly.duplicated(subset=["company_id", "fiscal_year", "fiscal_quarter"])
    check("No duplicate quarterly records", q_dupes.sum() == 0, f"{q_dupes.sum()} duplicates")

    # ── 6. Arithmetic Consistency ──
    print("\n--- Arithmetic Consistency ---")
    tolerance = 0.05  # 5% tolerance

    gp_ok, gp_fail = 0, 0
    for _, row in income.iterrows():
        expected_gp = row["revenue"] - row["cost_of_goods_sold"]
        if row["revenue"] != 0 and abs(row["gross_profit"] - expected_gp) > abs(row["revenue"]) * tolerance:
            gp_fail += 1
        else:
            gp_ok += 1
    check(f"Gross profit = Revenue - COGS (5% tolerance)", gp_fail == 0,
          f"{gp_ok} ok, {gp_fail} mismatches")

    # Net income should be less than revenue (absolute)
    disproportionate = income[abs(income["net_income"]) > abs(income["revenue"]) * 3]
    check("Net income proportionate to revenue", len(disproportionate) == 0,
          f"{len(disproportionate)} disproportionate rows")

    # ── 7. Data Coverage ──
    print("\n--- Data Coverage ---")
    date_range = conn.execute("""
        SELECT company_id,
               MIN(period_end) as earliest,
               MAX(period_end) as latest,
               COUNT(*) as records
        FROM income_statement
        GROUP BY company_id
    """).fetchdf()

    for _, row in date_range.iterrows():
        cid = row["company_id"]
        check(f"{cid} date range", True,
              f"{row['earliest']} to {row['latest']} ({row['records']} records)")

    total_income = len(income)
    total_quarterly = len(quarterly)
    check(f"Total income_statement records >= 15", total_income >= 15, str(total_income))
    check(f"Total quarterly_standalone records >= 10", total_quarterly >= 10, str(total_quarterly))

    # ── 8. Referential Integrity ──
    print("\n--- Referential Integrity ---")
    orphan_sources = conn.execute("""
        SELECT COUNT(*) FROM report_sources rs
        WHERE NOT EXISTS (SELECT 1 FROM companies c WHERE c.company_id = rs.company_id)
    """).fetchone()[0]
    check("No orphan report_sources", orphan_sources == 0, f"{orphan_sources} orphans")

    orphan_income = conn.execute("""
        SELECT COUNT(*) FROM income_statement i
        WHERE NOT EXISTS (SELECT 1 FROM companies c WHERE c.company_id = i.company_id)
    """).fetchone()[0]
    check("No orphan income_statement", orphan_income == 0, f"{orphan_income} orphans")

    orphan_quarterly = conn.execute("""
        SELECT COUNT(*) FROM quarterly_standalone q
        WHERE NOT EXISTS (SELECT 1 FROM companies c WHERE c.company_id = q.company_id)
    """).fetchone()[0]
    check("No orphan quarterly_standalone", orphan_quarterly == 0, f"{orphan_quarterly} orphans")

    conn.close()

    # ── Summary ──
    print("\n" + "=" * 60)
    total = PASS + FAIL
    print(f"Results: {PASS}/{total} passed, {FAIL} failed")
    print("=" * 60)
    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    main()
