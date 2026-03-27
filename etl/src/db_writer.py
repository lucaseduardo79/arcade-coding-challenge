"""DuckDB schema creation and data insertion."""

import logging
from pathlib import Path

import duckdb

from .config import DB_PATH, COMPANIES
from .schemas import PLExtraction

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS companies (
    company_id VARCHAR PRIMARY KEY,
    symbol VARCHAR NOT NULL,
    full_name VARCHAR NOT NULL,
    security_id INTEGER NOT NULL,
    sector VARCHAR,
    fiscal_year_end_month INTEGER
);

CREATE TABLE IF NOT EXISTS report_sources (
    source_id INTEGER PRIMARY KEY,
    company_id VARCHAR REFERENCES companies(company_id),
    pdf_filename VARCHAR NOT NULL,
    pdf_url VARCHAR,
    report_type VARCHAR NOT NULL,
    period_end DATE NOT NULL,
    period_months INTEGER NOT NULL,
    statement_type VARCHAR NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    confidence_score FLOAT
);

CREATE TABLE IF NOT EXISTS income_statement (
    record_id INTEGER PRIMARY KEY,
    source_id INTEGER REFERENCES report_sources(source_id),
    company_id VARCHAR REFERENCES companies(company_id),
    period_end DATE NOT NULL,
    period_months INTEGER NOT NULL,
    fiscal_year VARCHAR NOT NULL,
    fiscal_quarter VARCHAR,
    revenue DOUBLE,
    cost_of_goods_sold DOUBLE,
    gross_profit DOUBLE,
    operating_expenses DOUBLE,
    operating_income DOUBLE,
    net_income DOUBLE,
    other_income DOUBLE,
    finance_income DOUBLE,
    finance_costs DOUBLE,
    profit_before_tax DOUBLE,
    income_tax_expense DOUBLE,
    currency VARCHAR DEFAULT 'LKR',
    unit_scale VARCHAR DEFAULT 'thousands',
    is_cumulative BOOLEAN,
    UNIQUE(company_id, period_end, period_months, is_cumulative)
);

CREATE TABLE IF NOT EXISTS quarterly_standalone (
    record_id INTEGER PRIMARY KEY,
    company_id VARCHAR REFERENCES companies(company_id),
    period_end DATE NOT NULL,
    fiscal_year VARCHAR NOT NULL,
    fiscal_quarter VARCHAR NOT NULL,
    revenue DOUBLE,
    cost_of_goods_sold DOUBLE,
    gross_profit DOUBLE,
    operating_expenses DOUBLE,
    operating_income DOUBLE,
    net_income DOUBLE,
    currency VARCHAR DEFAULT 'LKR',
    unit_scale VARCHAR DEFAULT 'thousands',
    derivation_method VARCHAR,
    UNIQUE(company_id, fiscal_year, fiscal_quarter)
);

CREATE SEQUENCE IF NOT EXISTS seq_source_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_record_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_quarterly_id START 1;
"""


def get_connection() -> duckdb.DuckDBPyConnection:
    db_path = Path(DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def get_processed_pdfs() -> set[str]:
    """Return set of pdf_filename values already in report_sources."""
    conn = get_connection()
    try:
        result = conn.execute("SELECT pdf_filename FROM report_sources").fetchall()
        return {row[0] for row in result}
    except Exception:
        return set()
    finally:
        conn.close()


def initialize_db():
    """Create schema and seed company data."""
    conn = get_connection()
    try:
        conn.execute(SCHEMA_SQL)

        for cid, info in COMPANIES.items():
            conn.execute(
                """
                INSERT INTO companies
                (company_id, symbol, full_name, security_id, sector, fiscal_year_end_month)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (company_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    full_name = EXCLUDED.full_name,
                    security_id = EXCLUDED.security_id,
                    sector = EXCLUDED.sector,
                    fiscal_year_end_month = EXCLUDED.fiscal_year_end_month
                """,
                [
                    cid,
                    info["symbol"],
                    info["full_name"],
                    info["security_id"],
                    info["sector"],
                    info["fiscal_year_end_month"],
                ],
            )
        logger.info("Database initialized")
    finally:
        conn.close()


def compute_fiscal_year_quarter(period_end_date: str, fiscal_year_end_month: int = 3) -> tuple[str, str]:
    """Compute fiscal year string and quarter from period end date.

    For a March fiscal year end:
      Q1 = Apr-Jun, Q2 = Jul-Sep, Q3 = Oct-Dec, Q4 = Jan-Mar
    """
    from datetime import datetime

    dt = datetime.strptime(period_end_date, "%Y-%m-%d")
    month = dt.month

    # Determine fiscal year
    if month <= fiscal_year_end_month:
        fy_start = dt.year - 1
        fy_end = dt.year
    else:
        fy_start = dt.year
        fy_end = dt.year + 1

    fiscal_year = f"{fy_start}/{str(fy_end)[-2:]}"

    # Determine quarter based on month offset from fiscal year start
    fy_start_month = fiscal_year_end_month + 1  # April for March FY
    if fy_start_month > 12:
        fy_start_month -= 12

    month_offset = (month - fy_start_month) % 12
    quarter_num = (month_offset // 3) + 1
    fiscal_quarter = f"Q{quarter_num}"

    return fiscal_year, fiscal_quarter


def insert_extraction(
    company_id: str,
    pdf_filename: str,
    pdf_url: str,
    data: PLExtraction,
    confidence_score: float = 1.0,
) -> int:
    """Insert extracted P&L data into DuckDB. Returns source_id."""
    conn = get_connection()
    try:
        fiscal_year, fiscal_quarter = compute_fiscal_year_quarter(
            data.period_end_date,
            COMPANIES.get(company_id, {}).get("fiscal_year_end_month", 3),
        )

        # Determine report type
        if data.period_months == 3:
            report_type = "interim_quarterly"
        elif data.period_months == 12:
            report_type = "annual"
        else:
            report_type = f"interim_{data.period_months}m"

        # Insert source
        source_id = conn.execute("SELECT nextval('seq_source_id')").fetchone()[0]
        conn.execute(
            """
            INSERT INTO report_sources
            (source_id, company_id, pdf_filename, pdf_url, report_type,
             period_end, period_months, statement_type, confidence_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                source_id,
                company_id,
                pdf_filename,
                pdf_url,
                report_type,
                data.period_end_date,
                data.period_months,
                data.statement_type,
                confidence_score,
            ],
        )

        # Insert income statement
        record_id = conn.execute("SELECT nextval('seq_record_id')").fetchone()[0]
        conn.execute(
            """
            INSERT INTO income_statement
            (record_id, source_id, company_id, period_end, period_months,
             fiscal_year, fiscal_quarter, revenue, cost_of_goods_sold,
             gross_profit, operating_expenses, operating_income, net_income,
             other_income, finance_income, finance_costs, profit_before_tax,
             income_tax_expense, currency, unit_scale, is_cumulative)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (company_id, period_end, period_months, is_cumulative) DO UPDATE SET
                source_id = EXCLUDED.source_id,
                fiscal_year = EXCLUDED.fiscal_year,
                fiscal_quarter = EXCLUDED.fiscal_quarter,
                revenue = EXCLUDED.revenue,
                cost_of_goods_sold = EXCLUDED.cost_of_goods_sold,
                gross_profit = EXCLUDED.gross_profit,
                operating_expenses = EXCLUDED.operating_expenses,
                operating_income = EXCLUDED.operating_income,
                net_income = EXCLUDED.net_income,
                other_income = EXCLUDED.other_income,
                finance_income = EXCLUDED.finance_income,
                finance_costs = EXCLUDED.finance_costs,
                profit_before_tax = EXCLUDED.profit_before_tax,
                income_tax_expense = EXCLUDED.income_tax_expense,
                currency = EXCLUDED.currency,
                unit_scale = EXCLUDED.unit_scale
            """,
            [
                record_id,
                source_id,
                company_id,
                data.period_end_date,
                data.period_months,
                fiscal_year,
                fiscal_quarter,
                data.revenue,
                data.cost_of_goods_sold,
                data.gross_profit,
                data.operating_expenses,
                data.operating_income,
                data.net_income,
                data.other_income,
                data.finance_income,
                data.finance_costs,
                data.profit_before_tax,
                data.income_tax_expense,
                data.currency,
                "thousands",
                data.is_cumulative,
            ],
        )

        logger.info(
            f"Inserted: {company_id} {fiscal_year} {fiscal_quarter} "
            f"({data.period_months}m ending {data.period_end_date})"
        )
        return source_id
    finally:
        conn.close()


def _upsert_quarterly(conn, company_id, period_end, fiscal_year, fiscal_quarter,
                       values: dict, method: str):
    """Insert or update a quarterly_standalone record."""
    qid = conn.execute("SELECT nextval('seq_quarterly_id')").fetchone()[0]
    conn.execute(
        """
        INSERT INTO quarterly_standalone
        (record_id, company_id, period_end, fiscal_year, fiscal_quarter,
         revenue, cost_of_goods_sold, gross_profit, operating_expenses,
         operating_income, net_income, currency, unit_scale, derivation_method)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'LKR', 'thousands', ?)
        ON CONFLICT (company_id, fiscal_year, fiscal_quarter) DO UPDATE SET
            period_end = EXCLUDED.period_end,
            revenue = EXCLUDED.revenue,
            cost_of_goods_sold = EXCLUDED.cost_of_goods_sold,
            gross_profit = EXCLUDED.gross_profit,
            operating_expenses = EXCLUDED.operating_expenses,
            operating_income = EXCLUDED.operating_income,
            net_income = EXCLUDED.net_income,
            derivation_method = EXCLUDED.derivation_method
        """,
        [
            qid, company_id, period_end, fiscal_year, fiscal_quarter,
            values["revenue"], values["cost_of_goods_sold"],
            values["gross_profit"], values["operating_expenses"],
            values["operating_income"], values["net_income"],
            method,
        ],
    )


def compute_quarterly_standalone():
    """Derive single-quarter figures for the quarterly_standalone table.

    Two sources:
    1. Non-cumulative 3-month records → copied directly
    2. Cumulative records → Q2 standalone = H1 - Q1, etc.
    """
    conn = get_connection()
    try:
        columns = [
            "revenue", "cost_of_goods_sold", "gross_profit",
            "operating_expenses", "operating_income", "net_income",
        ]

        # ── Source 1: Direct quarterly records (is_cumulative=FALSE, period_months=3) ──
        direct_records = conn.execute(
            """
            SELECT company_id, period_end, period_months, fiscal_year, fiscal_quarter,
                   revenue, cost_of_goods_sold, gross_profit, operating_expenses,
                   operating_income, net_income
            FROM income_statement
            WHERE is_cumulative = FALSE AND period_months = 3
            ORDER BY company_id, period_end
            """
        ).fetchall()

        for rec in direct_records:
            values = {columns[i]: rec[5 + i] or 0.0 for i in range(len(columns))}
            _upsert_quarterly(conn, rec[0], rec[1], rec[3], rec[4], values, "direct")

        logger.info(f"Inserted {len(direct_records)} direct quarterly records")

        # ── Source 2: Derived from cumulative records ──
        cumulative_records = conn.execute(
            """
            SELECT company_id, period_end, period_months, fiscal_year, fiscal_quarter,
                   revenue, cost_of_goods_sold, gross_profit, operating_expenses,
                   operating_income, net_income
            FROM income_statement
            WHERE is_cumulative = TRUE
            ORDER BY company_id, fiscal_year, period_months
            """
        ).fetchall()

        by_company_fy: dict[tuple[str, str], list] = {}
        for row in cumulative_records:
            key = (row[0], row[3])  # company_id, fiscal_year
            by_company_fy.setdefault(key, []).append(row)

        derived_count = 0
        for (company_id, fiscal_year), fy_records in by_company_fy.items():
            fy_records.sort(key=lambda r: r[2])

            prev_values = {c: 0.0 for c in columns}
            for rec in fy_records:
                period_months = rec[2]
                quarter_num = period_months // 3
                fiscal_quarter = f"Q{quarter_num}"
                period_end = rec[1]

                current_values = {
                    columns[i]: rec[5 + i] or 0.0 for i in range(len(columns))
                }

                standalone = {
                    c: current_values[c] - prev_values[c] for c in columns
                }

                method = "direct" if period_months == 3 else "computed_from_cumulative"
                _upsert_quarterly(conn, company_id, period_end, fiscal_year,
                                  fiscal_quarter, standalone, method)
                derived_count += 1
                prev_values = current_values

        logger.info(f"Inserted {derived_count} cumulative-derived quarterly records")
        logger.info("Computed quarterly standalone figures")
    finally:
        conn.close()
