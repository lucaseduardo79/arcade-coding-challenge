"""Main entry point for the CSE scraper."""

import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

from .config import COMPANIES
from .cse_client import fetch_financial_reports
from .manifest import ScrapeManifest
from .pdf_downloader import download_pdf, sanitize_filename

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def scrape_company(company, manifest: ScrapeManifest):
    """Discover and download all quarterly reports for a company."""
    logger.info(f"=== Scraping {company.full_name} ({company.symbol}) ===")

    reports = await fetch_financial_reports(company)
    logger.info(f"Found {len(reports)} reports for {company.company_id}")

    downloaded = 0
    for report in reports:
        if manifest.is_downloaded(report.pdf_url):
            logger.info(f"Already downloaded: {report.title}")
            continue

        try:
            filename = sanitize_filename(
                f"{company.company_id}_{report.title}"
            )
            path, size = await download_pdf(
                url=report.pdf_url,
                company_id=company.company_id,
                filename=filename,
            )
            manifest.add(
                company_id=company.company_id,
                symbol=company.symbol,
                pdf_filename=path.name,
                pdf_url=report.pdf_url,
                report_title=report.title,
                period_description=report.period_description,
                file_size_bytes=size,
            )
            downloaded += 1
            logger.info(f"Downloaded: {report.title} ({size} bytes)")
        except Exception as e:
            logger.error(f"Failed to download {report.title}: {e}")
            continue

    logger.info(
        f"Finished {company.company_id}: {downloaded} new, "
        f"{len(manifest.get_entries_for_company(company.company_id))} total"
    )


async def main():
    load_dotenv()

    manifest_path = os.environ.get("MANIFEST_PATH")
    manifest = ScrapeManifest(manifest_path)
    # Ensure manifest file exists even if empty, so ETL doesn't crash
    manifest.save()

    logger.info("Starting CSE Financial Report Scraper")
    logger.info(f"Target companies: {[c.company_id for c in COMPANIES]}")

    for company in COMPANIES:
        await scrape_company(company, manifest)

    total = len(manifest.entries)
    logger.info(f"=== Scraping complete: {total} total PDFs in manifest ===")


if __name__ == "__main__":
    asyncio.run(main())
