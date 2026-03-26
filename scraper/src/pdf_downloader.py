"""Downloads PDFs from CSE and saves them organized by company."""

import logging
import os
import re
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    name = re.sub(r"\s+", "_", name)
    return name[:200]


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
async def download_pdf(url: str, company_id: str, filename: str) -> tuple[Path, int]:
    output_dir = Path(os.environ.get("PDF_OUTPUT_DIR", "shared/pdfs")) / company_id
    output_dir.mkdir(parents=True, exist_ok=True)

    safe_name = sanitize_filename(filename)
    if not safe_name.endswith(".pdf"):
        safe_name += ".pdf"

    output_path = output_dir / safe_name

    if output_path.exists():
        logger.info(f"Already exists: {output_path}")
        return output_path, output_path.stat().st_size

    logger.info(f"Downloading: {url} -> {output_path}")
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()

        output_path.write_bytes(response.content)
        file_size = len(response.content)
        logger.info(f"Downloaded {file_size} bytes -> {output_path}")
        return output_path, file_size
