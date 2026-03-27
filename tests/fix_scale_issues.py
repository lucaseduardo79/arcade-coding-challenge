"""Fix scale inconsistencies in DIPD income_statement records.

Some records were extracted in wrong units by the LLM:
- Record 20 (2025/26 Q3 9m): extracted in rupees instead of thousands -> /1000
- Record 5 (2021/22 Q4): extracted in millions instead of thousands -> x1000
- Record 4 (2021/22 Q3 9m): revenue misread (27M vs expected ~55M)
- Record 7 (2022/23 Q2 6m): revenue misread (2M vs expected ~40M)
- Record 16 (2024/25 Q3 9m): COGS/GP misread

This script fixes records that have clear scale issues (/1000 or x1000)
and flags others for manual review.
"""

import duckdb
import sys

DB_PATH = "shared/db/financial_data.duckdb"

NUMERIC_FIELDS = [
    "revenue", "cost_of_goods_sold", "gross_profit", "operating_expenses",
    "operating_income", "net_income", "other_income", "finance_income",
    "finance_costs", "profit_before_tax", "income_tax_expense",
]


def scale_record(conn, record_id, multiplier, reason):
    """Scale all numeric fields of a record by multiplier."""
    set_clauses = ", ".join(f"{f} = {f} * {multiplier}" for f in NUMERIC_FIELDS)
    conn.execute(f"UPDATE income_statement SET {set_clauses} WHERE record_id = ?", [record_id])
    print(f"  [FIXED] record_id={record_id}: x{multiplier} ({reason})")


def delete_record(conn, record_id, reason):
    """Delete a hopelessly wrong record."""
    conn.execute("DELETE FROM income_statement WHERE record_id = ?", [record_id])
    # Also remove the report_source
    conn.execute("DELETE FROM report_sources WHERE source_id = (SELECT source_id FROM income_statement WHERE record_id = ?)", [record_id])
    print(f"  [DELETED] record_id={record_id}: {reason}")


def main():
    conn = duckdb.connect(DB_PATH)

    print("=" * 60)
    print("Fixing DIPD scale inconsistencies")
    print("=" * 60)

    # Get all DIPD records for analysis
    records = conn.execute("""
        SELECT record_id, fiscal_year, fiscal_quarter, period_months,
               is_cumulative, revenue, gross_profit, net_income
        FROM income_statement
        WHERE company_id = 'DIPD'
        ORDER BY period_end
    """).fetchall()

    # Compute median revenue per period_months for reference
    quarterly_revs = [r[5] for r in records if r[3] == 3 and r[5] > 100000]
    half_year_revs = [r[5] for r in records if r[3] == 6 and r[5] > 100000]
    nine_month_revs = [r[5] for r in records if r[3] == 9 and r[5] > 100000 and r[5] < 1e9]

    median_q = sorted(quarterly_revs)[len(quarterly_revs) // 2] if quarterly_revs else 0
    median_h = sorted(half_year_revs)[len(half_year_revs) // 2] if half_year_revs else 0
    median_9m = sorted(nine_month_revs)[len(nine_month_revs) // 2] if nine_month_revs else 0

    print(f"\nReference medians (thousands):")
    print(f"  Quarterly:  {median_q:>15,.0f}")
    print(f"  Half-year:  {median_h:>15,.0f}")
    print(f"  9-month:    {median_9m:>15,.0f}")

    print(f"\n--- Analyzing {len(records)} records ---\n")

    fixes = []
    for rec in records:
        rid, fy, fq, pm, is_cum, rev, gp, ni = rec

        if pm == 3:
            ref = median_q
        elif pm == 6:
            ref = median_h
        else:
            ref = median_9m

        if ref == 0:
            continue

        ratio = rev / ref if ref else 0

        if ratio > 500:
            # ~1000x too large -> divide by 1000
            fixes.append((rid, 1/1000, f"{fy} {fq} ({pm}m): rev={rev:,.0f} is ~{ratio:.0f}x median -> /1000"))
        elif ratio < 0.002:
            # ~1000x too small -> multiply by 1000
            fixes.append((rid, 1000, f"{fy} {fq} ({pm}m): rev={rev:,.0f} is ~{ratio:.4f}x median -> x1000"))
        elif ratio < 0.1 and pm > 3:
            # Cumulative but much smaller than expected - bad extraction
            print(f"  [WARN] record_id={rid} {fy} {fq} ({pm}m): rev={rev:,.0f} is {ratio:.2f}x median - likely misread")

    if not fixes:
        print("  No scale fixes needed!")
    else:
        print(f"\nApplying {len(fixes)} fixes:\n")
        for rid, mult, reason in fixes:
            scale_record(conn, rid, mult, reason)

    # Also check for specific known issues
    print("\n--- Checking specific known issues ---\n")

    # Record with negative revenue in quarterly_standalone (2021/22 Q3)
    # This comes from cumulative H1 (row 3) having wrong values
    r4 = conn.execute("""
        SELECT record_id, revenue, cost_of_goods_sold, gross_profit
        FROM income_statement
        WHERE company_id = 'DIPD' AND fiscal_year = '2021/22' AND period_months = 9
    """).fetchone()
    if r4:
        rid, rev, cogs, gp = r4
        if gp < 0:
            print(f"  [WARN] record_id={rid} 2021/22 Q3 (9m): GP is negative ({gp:,.0f}). Revenue ({rev:,.0f}) may be wrong column.")

    # Record 2024/25 Q3 with suspiciously high GP
    r16 = conn.execute("""
        SELECT record_id, revenue, cost_of_goods_sold, gross_profit
        FROM income_statement
        WHERE company_id = 'DIPD' AND fiscal_year = '2024/25' AND period_months = 9
    """).fetchone()
    if r16:
        rid, rev, cogs, gp = r16
        margin = gp / rev * 100 if rev else 0
        if margin > 50:
            print(f"  [WARN] record_id={rid} 2024/25 Q3 (9m): GP margin {margin:.0f}% too high. COGS ({cogs:,.0f}) likely wrong.")

    # Recompute quarterly standalone
    print("\n--- Recomputing quarterly_standalone ---\n")

    # Clear existing DIPD quarterly data
    conn.execute("DELETE FROM quarterly_standalone WHERE company_id = 'DIPD'")
    print("  Cleared DIPD quarterly_standalone")

    conn.close()

    # Use the ETL function to recompute
    import sys, os
    sys.path.insert(0, 'etl')
    os.environ['DB_PATH'] = DB_PATH
    from src.db_writer import compute_quarterly_standalone
    compute_quarterly_standalone()

    # Verify
    conn = duckdb.connect(DB_PATH, read_only=True)
    print("\n--- Verification ---\n")
    print(conn.execute("""
        SELECT fiscal_year, fiscal_quarter, revenue, gross_profit, net_income
        FROM quarterly_standalone
        WHERE company_id = 'DIPD'
        ORDER BY period_end
    """).fetchdf().to_string())
    conn.close()

    print("\nDone!")


if __name__ == "__main__":
    main()
