"""DuckDB query helper for the dashboard."""

import os
from contextlib import contextmanager

import duckdb
import pandas as pd

DB_PATH = os.environ.get("DB_PATH", "shared/db/financial_data.duckdb")


@contextmanager
def get_connection():
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        yield conn
    finally:
        conn.close()


def get_companies() -> pd.DataFrame:
    with get_connection() as conn:
        return conn.execute("SELECT * FROM companies ORDER BY company_id").fetchdf()


def get_income_statements(company_id: str | None = None) -> pd.DataFrame:
    query = """
        SELECT * FROM income_statement
        WHERE 1=1
    """
    params = []
    if company_id:
        query += " AND company_id = ?"
        params.append(company_id)
    query += " ORDER BY period_end DESC"

    with get_connection() as conn:
        return conn.execute(query, params).fetchdf()


def get_quarterly_standalone(company_id: str | None = None) -> pd.DataFrame:
    query = """
        SELECT * FROM quarterly_standalone
        WHERE 1=1
    """
    params = []
    if company_id:
        query += " AND company_id = ?"
        params.append(company_id)
    query += " ORDER BY period_end DESC"

    with get_connection() as conn:
        return conn.execute(query, params).fetchdf()


def get_latest_quarter_data() -> pd.DataFrame:
    """Get the most recent quarter data for each company."""
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT qs.*
            FROM quarterly_standalone qs
            INNER JOIN (
                SELECT company_id, MAX(period_end) as max_date
                FROM quarterly_standalone
                GROUP BY company_id
            ) latest ON qs.company_id = latest.company_id
                     AND qs.period_end = latest.max_date
            """
        ).fetchdf()


def get_annual_summary() -> pd.DataFrame:
    """Aggregate quarterly standalone into annual totals."""
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT
                company_id,
                fiscal_year,
                SUM(revenue) as revenue,
                SUM(cost_of_goods_sold) as cost_of_goods_sold,
                SUM(gross_profit) as gross_profit,
                SUM(operating_expenses) as operating_expenses,
                SUM(operating_income) as operating_income,
                SUM(net_income) as net_income,
                COUNT(*) as quarters_available
            FROM quarterly_standalone
            GROUP BY company_id, fiscal_year
            ORDER BY company_id, fiscal_year
            """
        ).fetchdf()


def run_query(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return conn.execute(sql).fetchdf()
