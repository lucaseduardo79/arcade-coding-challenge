"""Pydantic models for financial data extraction."""

from typing import Optional

from pydantic import BaseModel, Field


class PLExtraction(BaseModel):
    """Structured extraction of a Profit & Loss statement."""

    period_end_date: str = Field(
        description="End date of the reporting period in YYYY-MM-DD format"
    )
    period_months: int = Field(
        description="Number of months covered: 3 (quarter), 6 (half-year), 9 (nine months), or 12 (full year)"
    )
    is_cumulative: bool = Field(
        description="True if figures are cumulative from fiscal year start (e.g. 9 months ended), False if single quarter"
    )
    currency: str = Field(default="LKR", description="Currency code")
    unit_description: str = Field(
        description="Unit as stated in the document, e.g. 'Rs. 000', 'In Rupees Thousands', 'Rs. Mn'"
    )

    revenue: float = Field(description="Total Revenue / Turnover")
    cost_of_goods_sold: float = Field(
        description="Cost of Sales / COGS (as a positive number)"
    )
    gross_profit: float = Field(description="Gross Profit = Revenue - COGS")
    other_income: Optional[float] = Field(
        default=None, description="Other operating income"
    )
    operating_expenses: float = Field(
        description="Total operating expenses (distribution + admin + other opex)"
    )
    operating_income: float = Field(
        description="Operating profit / EBIT = Gross Profit + Other Income - Operating Expenses"
    )
    finance_income: Optional[float] = Field(
        default=None, description="Interest / finance income"
    )
    finance_costs: Optional[float] = Field(
        default=None, description="Interest / finance costs (as positive number)"
    )
    profit_before_tax: Optional[float] = Field(
        default=None, description="Profit before income tax"
    )
    income_tax_expense: Optional[float] = Field(
        default=None, description="Income tax expense (as positive number)"
    )
    net_income: float = Field(description="Profit for the period / Net income")

    statement_type: str = Field(
        description="'group' if Group/Consolidated statement, 'company' if Company-only"
    )
    confidence_notes: str = Field(
        description="Notes on extraction confidence, any ambiguities or assumptions made"
    )


class PageIdentification(BaseModel):
    """Result of identifying P&L pages in a PDF."""

    pl_page_numbers: list[int] = Field(
        description="List of 1-based page numbers containing the P&L / Income Statement"
    )
    statement_title: str = Field(
        description="Exact title of the statement as found in the document"
    )
    has_group_statement: bool = Field(
        description="Whether a Group/Consolidated statement was found"
    )
    has_company_statement: bool = Field(
        description="Whether a Company-only statement was found"
    )
