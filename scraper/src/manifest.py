"""Manages the scrape manifest - tracks downloaded PDFs and their metadata."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel


class ManifestEntry(BaseModel):
    company_id: str
    symbol: str
    pdf_filename: str
    pdf_url: str
    report_title: str
    period_description: str
    downloaded_at: str
    file_size_bytes: int


class ScrapeManifest:
    def __init__(self, manifest_path: Optional[str] = None):
        self.path = Path(
            manifest_path
            or os.environ.get("MANIFEST_PATH", "shared/metadata/scrape_manifest.json")
        )
        self.entries: list[ManifestEntry] = []
        self._load()

    def _load(self):
        if self.path.exists():
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.entries = [ManifestEntry(**e) for e in data]

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = [e.model_dump() for e in self.entries]
        self.path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def add(
        self,
        company_id: str,
        symbol: str,
        pdf_filename: str,
        pdf_url: str,
        report_title: str,
        period_description: str,
        file_size_bytes: int,
    ):
        entry = ManifestEntry(
            company_id=company_id,
            symbol=symbol,
            pdf_filename=pdf_filename,
            pdf_url=pdf_url,
            report_title=report_title,
            period_description=period_description,
            downloaded_at=datetime.utcnow().isoformat(),
            file_size_bytes=file_size_bytes,
        )
        self.entries.append(entry)
        self.save()

    def is_downloaded(self, pdf_url: str) -> bool:
        return any(e.pdf_url == pdf_url for e in self.entries)

    def get_entries_for_company(self, company_id: str) -> list[ManifestEntry]:
        return [e for e in self.entries if e.company_id == company_id]
