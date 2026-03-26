"""LLM-based financial data extraction using raw Groq SDK with fallback and LangSmith tracing."""

import json
import logging
import os
import time

from groq import Groq
from openai import OpenAI
from langsmith import traceable

from .config import GROQ_MODEL, GROQ_TEMPERATURE
from .schemas import PLExtraction, PageIdentification

logger = logging.getLogger(__name__)

# ── Primary: Groq client ──
groq_client = Groq()

# ── Fallback: OpenAI-compatible client (OpenRouter, Together, OpenAI, etc.) ──
FALLBACK_API_KEY = os.environ.get("FALLBACK_API_KEY", "")
FALLBACK_BASE_URL = os.environ.get("FALLBACK_BASE_URL", "https://openrouter.ai/api/v1")
FALLBACK_MODEL = os.environ.get("FALLBACK_MODEL", "meta-llama/llama-3.3-70b-instruct")

fallback_client = OpenAI(
    api_key=FALLBACK_API_KEY,
    base_url=FALLBACK_BASE_URL,
) if FALLBACK_API_KEY else None

if fallback_client:
    logger.info(f"Fallback configured: {FALLBACK_BASE_URL} key={FALLBACK_API_KEY[:8]}... model={FALLBACK_MODEL}")
else:
    logger.warning("No fallback API configured (FALLBACK_API_KEY is empty)")


class DailyLimitExhausted(Exception):
    """Raised when both Groq and fallback are exhausted."""
    pass


def _call_groq(kwargs: dict) -> str:
    """Try Groq API."""
    completion = groq_client.chat.completions.create(**kwargs)
    return completion.choices[0].message.content or ""


def _call_fallback(kwargs: dict) -> str:
    """Try fallback OpenAI-compatible API."""
    if not fallback_client:
        raise RuntimeError("No fallback API configured (set FALLBACK_API_KEY)")
    # Swap model to fallback model
    fb_kwargs = {**kwargs, "model": FALLBACK_MODEL}
    logger.info(f"Using fallback: {FALLBACK_BASE_URL} model={FALLBACK_MODEL}")
    completion = fallback_client.chat.completions.create(**fb_kwargs)
    return completion.choices[0].message.content or ""


@traceable(run_type="llm", name="groq_chat")
def groq_chat(messages, model=GROQ_MODEL, temperature=GROQ_TEMPERATURE,
              max_completion_tokens=2000, response_format=None) -> str:
    """Call Groq API with fallback (traced by LangSmith via @traceable)."""
    kwargs = dict(
        model=model,
        messages=messages,
        temperature=temperature,
        max_completion_tokens=max_completion_tokens,
        stream=False,
    )
    if response_format:
        kwargs["response_format"] = response_format

    # Rate limit protection
    time.sleep(3)

    try:
        return _call_groq(kwargs)
    except Exception as e:
        error_str = str(e)
        is_rate_limit = "429" in error_str or "rate_limit" in error_str
        is_daily_limit = "tokens per day" in error_str or "TPD" in error_str

        if is_rate_limit or is_daily_limit:
            logger.warning(f"Groq rate limited: {error_str[:120]}. Trying fallback...")
            if fallback_client:
                try:
                    return _call_fallback(kwargs)
                except Exception as fb_err:
                    logger.error(f"Fallback also failed: {fb_err}")
                    # Both Groq and fallback failed - stop processing
                    raise DailyLimitExhausted(
                        f"Groq: {error_str[:200]}. Fallback: {fb_err}"
                    ) from e
            else:
                if is_daily_limit:
                    raise DailyLimitExhausted(error_str) from e
                raise
        raise


def identify_pl_pages(page_summaries: str) -> PageIdentification:
    """Use LLM to identify which pages contain the P&L / Income Statement."""
    messages = [
        {
            "role": "system",
            "content": (
                "You are a financial document analyst. Given page summaries from a financial PDF report, "
                "identify which pages contain the Statement of Profit or Loss (also called Income Statement, "
                "Statement of Comprehensive Income, or P&L Statement). "
                "Look for keywords like: Revenue, Turnover, Cost of sales, Gross profit, "
                "Operating profit, Profit for the period, Net income. "
                "Also determine if there is a Group/Consolidated statement and/or a Company-only statement.\n\n"
                "Respond with ONLY valid JSON matching this schema:\n"
                '{"pl_page_numbers": [int], "statement_title": "str", '
                '"has_group_statement": bool, "has_company_statement": bool}'
            ),
        },
        {"role": "user", "content": page_summaries},
    ]

    # Use smaller/faster model for page identification (saves tokens)
    text = groq_chat(messages, model="llama-3.1-8b-instant", response_format={"type": "json_object"})
    data = json.loads(text)
    return PageIdentification(**data)


def extract_pl_data(
    pl_text: str,
    company_id: str,
    feedback: str | None = None,
) -> PLExtraction:
    """Use LLM to extract structured P&L data from the identified pages."""
    system_prompt = (
        "You are a financial data extraction specialist. Extract the Profit & Loss (Income Statement) "
        "data from the provided financial report text.\n\n"
        "IMPORTANT RULES:\n"
        "1. Prefer the GROUP / CONSOLIDATED column over Company-only column.\n"
        "2. Extract data for the LATEST / CURRENT quarter/period (not prior year comparatives).\n"
        "3. COGS (cost_of_goods_sold) should be a POSITIVE number even if shown as negative in the statement.\n"
        "4. operating_expenses = you must compute the sum yourself and write ONLY the final number. "
        "For example if distribution=100, admin=200, other=50, write 350.\n"
        "5. operating_income = gross_profit + other_income - operating_expenses. Write ONLY the final number.\n"
        "6. Identify the unit of measurement (thousands, millions, etc.) from the document header.\n"
        "7. period_end_date must be in YYYY-MM-DD format.\n"
        "8. is_cumulative = true if the period is described as '6 months ended', '9 months ended', etc.\n"
        "   is_cumulative = false only if it's clearly a single quarter figure.\n"
        "9. finance_costs and income_tax_expense should be POSITIVE numbers.\n"
        "10. CRITICAL: Every value MUST be a single number like 12345.0. "
        "NEVER write arithmetic expressions like 100+200+300 or (100-200). "
        "Compute the result yourself and write only the final number. "
        "JSON with expressions like 'a + b' is INVALID and will cause errors.\n"
        "11. All numeric fields MUST be numbers (int or float), NOT strings.\n"
        "12. Boolean fields MUST be true/false, NOT strings.\n\n"
        "Respond with ONLY valid JSON matching this exact schema:\n"
        "{\n"
        '  "period_end_date": "YYYY-MM-DD",\n'
        '  "period_months": 3|6|9|12,\n'
        '  "is_cumulative": true|false,\n'
        '  "currency": "LKR",\n'
        '  "unit_description": "str",\n'
        '  "revenue": number,\n'
        '  "cost_of_goods_sold": number,\n'
        '  "gross_profit": number,\n'
        '  "other_income": number|null,\n'
        '  "operating_expenses": number,\n'
        '  "operating_income": number,\n'
        '  "finance_income": number|null,\n'
        '  "finance_costs": number|null,\n'
        '  "profit_before_tax": number|null,\n'
        '  "income_tax_expense": number|null,\n'
        '  "net_income": number,\n'
        '  "statement_type": "group"|"company",\n'
        '  "confidence_notes": "str"\n'
        "}"
    )

    if feedback:
        system_prompt += (
            f"\n\nPREVIOUS EXTRACTION HAD ERRORS. Please fix:\n{feedback}\n"
            "Re-extract carefully, paying attention to the specific errors mentioned."
        )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Company: {company_id}\n\nFinancial Statement Text:\n{pl_text}"},
    ]

    text = groq_chat(messages, response_format={"type": "json_object"})
    data = json.loads(text)
    return PLExtraction(**data)


def validate_extraction(data: PLExtraction) -> list[str]:
    """Programmatic validation of extracted data - checks arithmetic consistency."""
    errors = []

    # Check gross_profit = revenue - COGS (2% tolerance)
    expected_gp = data.revenue - data.cost_of_goods_sold
    if data.revenue != 0 and abs(data.gross_profit - expected_gp) > abs(data.revenue * 0.02):
        errors.append(
            f"Gross profit mismatch: extracted {data.gross_profit}, "
            f"expected {expected_gp} (revenue {data.revenue} - COGS {data.cost_of_goods_sold})"
        )

    # Check net_income is reasonable relative to revenue
    if data.revenue != 0 and abs(data.net_income) > abs(data.revenue) * 2:
        errors.append(
            f"Net income ({data.net_income}) seems disproportionate to revenue ({data.revenue})"
        )

    # Required fields should not be zero for an operating company
    for field_name in ["revenue", "cost_of_goods_sold", "gross_profit"]:
        val = getattr(data, field_name)
        if val == 0:
            errors.append(f"{field_name} is zero, unlikely for an operating company")

    # period_months should be valid
    if data.period_months not in (3, 6, 9, 12):
        errors.append(f"period_months={data.period_months} is not valid (expected 3, 6, 9, or 12)")

    # COGS should be positive
    if data.cost_of_goods_sold < 0:
        errors.append(f"cost_of_goods_sold ({data.cost_of_goods_sold}) should be positive")

    return errors


def normalize_to_thousands(data: PLExtraction) -> PLExtraction:
    """Normalize all values to LKR thousands based on unit_description."""
    unit = data.unit_description.lower()

    if any(k in unit for k in ["million", "mn", "m"]):
        multiplier = 1000.0
    elif any(k in unit for k in ["thousand", "000", "'000"]):
        multiplier = 1.0
    elif any(k in unit for k in ["billion", "bn"]):
        multiplier = 1_000_000.0
    else:
        multiplier = 1.0

    if multiplier == 1.0:
        return data

    logger.info(f"Normalizing from '{data.unit_description}' with multiplier {multiplier}")
    fields = [
        "revenue", "cost_of_goods_sold", "gross_profit", "other_income",
        "operating_expenses", "operating_income", "finance_income",
        "finance_costs", "profit_before_tax", "income_tax_expense", "net_income",
    ]

    update = {}
    for f in fields:
        val = getattr(data, f)
        if val is not None:
            update[f] = val * multiplier

    update["unit_description"] = "In Rupees Thousands (normalized)"
    return data.model_copy(update=update)
