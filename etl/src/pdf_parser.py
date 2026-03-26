"""PDF table and text extraction using pdfplumber with PyMuPDF fallback."""

import logging
from dataclasses import dataclass
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)


@dataclass
class PageContent:
    page_num: int  # 1-based
    text: str
    tables: list[list[list[str]]]


def extract_pdf_content(pdf_path: str | Path) -> list[PageContent]:
    """Extract text and tables from all pages of a PDF using pdfplumber."""
    pdf_path = Path(pdf_path)
    pages: list[PageContent] = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                page_num = i + 1

                # Extract text
                text = page.extract_text() or ""

                # Try strict line-based table extraction first
                tables = page.extract_tables(
                    table_settings={
                        "vertical_strategy": "lines_strict",
                        "horizontal_strategy": "lines_strict",
                    }
                )

                # Fallback to text-based strategy if no tables found
                if not tables:
                    tables = page.extract_tables(
                        table_settings={
                            "vertical_strategy": "text",
                            "horizontal_strategy": "text",
                            "snap_y_tolerance": 5,
                            "intersection_y_tolerance": 10,
                        }
                    )

                # Clean table cells
                cleaned_tables = []
                for table in tables:
                    cleaned = []
                    for row in table:
                        cleaned_row = [
                            (cell.strip() if cell else "") for cell in row
                        ]
                        cleaned.append(cleaned_row)
                    cleaned_tables.append(cleaned)

                pages.append(
                    PageContent(
                        page_num=page_num,
                        text=text,
                        tables=cleaned_tables,
                    )
                )

    except Exception as e:
        logger.warning(f"pdfplumber failed for {pdf_path}: {e}, trying PyMuPDF fallback")
        pages = _pymupdf_fallback(pdf_path)

    logger.info(
        f"Extracted {len(pages)} pages from {pdf_path.name}, "
        f"tables found on pages: {[p.page_num for p in pages if p.tables]}"
    )
    return pages


def _pymupdf_fallback(pdf_path: Path) -> list[PageContent]:
    """Fallback extraction using PyMuPDF when pdfplumber fails."""
    import fitz

    pages = []
    try:
        doc = fitz.open(str(pdf_path))
        for i, page in enumerate(doc):
            text = page.get_text()
            pages.append(
                PageContent(page_num=i + 1, text=text, tables=[])
            )
        doc.close()
    except Exception as e:
        logger.error(f"PyMuPDF also failed for {pdf_path}: {e}")

    return pages


def format_page_summary(page: PageContent) -> str:
    """Format a page's content into a concise summary for LLM processing."""
    parts = [f"=== PAGE {page.page_num} ==="]
    if page.text:
        # Truncate very long pages
        text = page.text[:3000] if len(page.text) > 3000 else page.text
        parts.append(text)

    if page.tables:
        for t_idx, table in enumerate(page.tables):
            parts.append(f"\n--- Table {t_idx + 1} ---")
            for row in table:
                parts.append(" | ".join(row))

    return "\n".join(parts)


def format_pages_for_extraction(pages: list[PageContent], page_numbers: list[int]) -> str:
    """Format specific pages for detailed extraction by the LLM."""
    selected = [p for p in pages if p.page_num in page_numbers]
    return "\n\n".join(format_page_summary(p) for p in selected)
