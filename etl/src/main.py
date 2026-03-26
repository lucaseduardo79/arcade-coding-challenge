"""Main entry point for the ETL pipeline."""

import json
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from .config import MANIFEST_PATH, PDF_INPUT_DIR
from .db_writer import initialize_db, insert_extraction, compute_quarterly_standalone, get_processed_pdfs
from .graph import build_extraction_graph, ExtractionState
from .llm_extractor import DailyLimitExhausted
from .pdf_parser import extract_pdf_content

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def load_manifest() -> list[dict]:
    manifest_path = Path(MANIFEST_PATH)
    if not manifest_path.exists():
        logger.warning(f"Manifest not found at {manifest_path}, scanning PDF directory...")
        return scan_pdf_directory()

    with open(manifest_path, encoding="utf-8") as f:
        return json.load(f)


def scan_pdf_directory() -> list[dict]:
    """Fallback: scan PDF directory if no manifest exists."""
    entries = []
    pdf_dir = Path(PDF_INPUT_DIR)
    if not pdf_dir.exists():
        logger.warning(f"PDF directory {pdf_dir} does not exist")
        return entries
    for company_dir in pdf_dir.iterdir():
        if not company_dir.is_dir():
            continue
        company_id = company_dir.name
        for pdf_file in company_dir.glob("*.pdf"):
            entries.append(
                {
                    "company_id": company_id,
                    "pdf_filename": pdf_file.name,
                    "pdf_url": "",
                    "report_title": pdf_file.stem,
                    "period_description": pdf_file.stem,
                }
            )
    return entries


def process_pdf(entry: dict, graph) -> bool:
    """Process a single PDF through the extraction pipeline."""
    company_id = entry["company_id"]
    pdf_filename = entry["pdf_filename"]
    pdf_path = Path(PDF_INPUT_DIR) / company_id / pdf_filename

    if not pdf_path.exists():
        logger.error(f"PDF not found: {pdf_path}")
        return False

    # Skip annual reports - too large and quarterly data is what we need
    lower_name = pdf_filename.lower()
    if "annual_report" in lower_name or "annual report" in lower_name:
        logger.info(f"Skipping annual report: {pdf_filename}")
        return False

    # Skip non-financial PDFs (press releases, etc.)
    if "press_release" in lower_name or "press release" in lower_name:
        logger.info(f"Skipping non-financial PDF: {pdf_filename}")
        return False

    logger.info(f"Processing: {pdf_path}")

    # Parse PDF
    pages = extract_pdf_content(pdf_path)
    if not pages:
        logger.error(f"No pages extracted from {pdf_path}")
        return False

    # Run extraction graph
    initial_state: ExtractionState = {
        "pdf_path": str(pdf_path),
        "company_id": company_id,
        "pages": pages,
        "page_identification": None,
        "pl_text": "",
        "extraction": None,
        "validation_errors": [],
        "retry_count": 0,
        "is_complete": False,
        "error_message": "",
    }

    try:
        result = graph.invoke(initial_state)
    except DailyLimitExhausted:
        raise  # Propagate to main loop for graceful stop
    except Exception as e:
        logger.error(f"Graph execution failed for {pdf_path}: {e}")
        return False

    if result.get("error_message"):
        logger.error(f"Extraction error for {pdf_path}: {result['error_message']}")
        return False

    extraction = result.get("extraction")
    if extraction is None:
        logger.error(f"No extraction result for {pdf_path}")
        return False

    # Calculate confidence based on validation errors
    validation_errors = result.get("validation_errors", [])
    confidence = 1.0 if not validation_errors else max(0.5, 1.0 - 0.15 * len(validation_errors))

    # Insert into DuckDB
    try:
        insert_extraction(
            company_id=company_id,
            pdf_filename=pdf_filename,
            pdf_url=entry.get("pdf_url", ""),
            data=extraction,
            confidence_score=confidence,
        )
        return True
    except Exception as e:
        logger.error(f"DB insertion failed for {pdf_path}: {e}")
        return False


def main():
    load_dotenv()

    logger.info("Starting ETL Pipeline")

    # Initialize database
    initialize_db()

    # Load manifest
    manifest = load_manifest()
    logger.info(f"Found {len(manifest)} PDFs in manifest")

    # Check which PDFs are already processed (resumable ETL)
    already_processed = get_processed_pdfs()
    if already_processed:
        logger.info(f"Already processed {len(already_processed)} PDFs, skipping those")

    # Build extraction graph
    graph = build_extraction_graph()

    # Process each PDF
    success_count = 0
    fail_count = 0
    skipped_count = 0

    for i, entry in enumerate(manifest):
        pdf_filename = entry.get("pdf_filename", "")

        # Skip already processed PDFs
        if pdf_filename in already_processed:
            skipped_count += 1
            continue

        try:
            ok = process_pdf(entry, graph)
            if ok:
                success_count += 1
            else:
                fail_count += 1
        except DailyLimitExhausted:
            logger.warning(
                f"Daily token limit reached after {success_count} new extractions. "
                f"Run ETL again later to process remaining PDFs."
            )
            break

        # Delay between PDFs to respect Groq rate limits
        if i < len(manifest) - 1:
            logger.info("Waiting 5s before next PDF (rate limit protection)...")
            time.sleep(5)

    logger.info(
        f"ETL complete: {success_count} new, {skipped_count} skipped (already done), "
        f"{fail_count} failed, out of {len(manifest)} total"
    )

    # Compute derived quarterly standalone figures
    if success_count > 0 or skipped_count > 0:
        compute_quarterly_standalone()
        logger.info("Quarterly standalone computation complete")


if __name__ == "__main__":
    main()
