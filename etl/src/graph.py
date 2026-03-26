"""LangGraph extraction graph definition."""

import logging
from typing import Annotated, Optional

from langgraph.graph import StateGraph, START, END
from typing_extensions import TypedDict

from .llm_extractor import (
    identify_pl_pages,
    extract_pl_data,
    validate_extraction,
    normalize_to_thousands,
    DailyLimitExhausted,
)
from .pdf_parser import (
    PageContent,
    format_page_summary,
    format_pages_for_extraction,
)
from .schemas import PLExtraction, PageIdentification

logger = logging.getLogger(__name__)


class ExtractionState(TypedDict):
    pdf_path: str
    company_id: str
    pages: list[PageContent]
    page_identification: Optional[PageIdentification]
    pl_text: str
    extraction: Optional[PLExtraction]
    validation_errors: list[str]
    retry_count: int
    is_complete: bool
    error_message: str


def node_identify_pages(state: ExtractionState) -> dict:
    """Identify which pages contain the P&L statement."""
    pages = state["pages"]

    # For large PDFs (annual reports), only scan first 30 pages where P&L usually is
    scan_pages = pages[:30] if len(pages) > 30 else pages

    # Send concise summaries (first 150 chars each to stay within token limits)
    summaries = []
    for p in scan_pages:
        summary = f"PAGE {p.page_num}: {p.text[:150]}"
        if p.tables:
            summary += f" [{len(p.tables)} table(s)]"
        summaries.append(summary)

    all_summaries = "\n\n".join(summaries)

    try:
        identification = identify_pl_pages(all_summaries)
        logger.info(
            f"Identified P&L on pages: {identification.pl_page_numbers}, "
            f"Group: {identification.has_group_statement}"
        )

        # Extract full text for identified pages
        pl_text = format_pages_for_extraction(pages, identification.pl_page_numbers)

        return {
            "page_identification": identification,
            "pl_text": pl_text,
        }
    except DailyLimitExhausted:
        raise  # Propagate to main loop for graceful stop
    except Exception as e:
        logger.error(f"Page identification failed: {e}")
        return {
            "is_complete": True,
            "error_message": f"Page identification failed: {e}",
        }


def node_extract_data(state: ExtractionState) -> dict:
    """Extract structured P&L data from identified pages."""
    pl_text = state["pl_text"]
    company_id = state["company_id"]
    retry_count = state.get("retry_count", 0)

    # On retry, include validation errors as feedback
    feedback = None
    if retry_count > 0 and state.get("validation_errors"):
        feedback = "\n".join(state["validation_errors"])

    try:
        extraction = extract_pl_data(pl_text, company_id, feedback)
        logger.info(
            f"Extracted: revenue={extraction.revenue}, "
            f"net_income={extraction.net_income}, "
            f"period={extraction.period_end_date} ({extraction.period_months}m)"
        )
        return {"extraction": extraction}
    except DailyLimitExhausted:
        raise  # Propagate to main loop for graceful stop
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        # On retry failure, keep previous extraction if available (accept with warnings)
        if retry_count > 0 and state.get("extraction") is not None:
            logger.warning("Retry failed but keeping previous extraction with validation warnings")
            return {
                "is_complete": False,  # Continue to normalize with previous data
                "validation_errors": state.get("validation_errors", []) + [f"Retry failed: {str(e)[:100]}"],
                "retry_count": 3,  # Force accept on next validate
            }
        return {
            "is_complete": True,
            "error_message": f"Extraction failed: {e}",
        }


def node_validate(state: ExtractionState) -> dict:
    """Validate extracted data arithmetically."""
    extraction = state["extraction"]
    if extraction is None:
        return {"is_complete": True, "error_message": "No extraction to validate"}

    errors = validate_extraction(extraction)
    retry_count = state.get("retry_count", 0)

    if errors:
        logger.warning(f"Validation errors (attempt {retry_count + 1}): {errors}")
        if retry_count >= 2:
            # Accept with warnings after max retries
            logger.warning("Max retries reached, accepting with validation warnings")
            return {
                "validation_errors": errors,
                "is_complete": False,  # Continue to normalize
            }
        return {
            "validation_errors": errors,
            "retry_count": retry_count + 1,
        }

    logger.info("Validation passed")
    return {"validation_errors": []}


def node_normalize(state: ExtractionState) -> dict:
    """Normalize units to LKR thousands."""
    extraction = state["extraction"]
    if extraction is None:
        return {"is_complete": True, "error_message": "No extraction to normalize"}

    normalized = normalize_to_thousands(extraction)
    return {"extraction": normalized, "is_complete": True}


def should_retry(state: ExtractionState) -> str:
    """Decide whether to retry extraction or proceed to normalization."""
    if state.get("is_complete"):
        return "end"
    if state.get("validation_errors") and state.get("retry_count", 0) > 0 and state.get("retry_count", 0) <= 2:
        return "retry"
    return "normalize"


def build_extraction_graph() -> StateGraph:
    """Build the LangGraph extraction pipeline."""
    graph = StateGraph(ExtractionState)

    graph.add_node("identify_pages", node_identify_pages)
    graph.add_node("extract_data", node_extract_data)
    graph.add_node("validate", node_validate)
    graph.add_node("normalize", node_normalize)

    graph.add_edge(START, "identify_pages")
    graph.add_edge("identify_pages", "extract_data")
    graph.add_edge("extract_data", "validate")

    graph.add_conditional_edges(
        "validate",
        should_retry,
        {
            "retry": "extract_data",
            "normalize": "normalize",
            "end": END,
        },
    )
    graph.add_edge("normalize", END)

    return graph.compile()
