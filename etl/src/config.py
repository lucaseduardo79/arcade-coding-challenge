"""Configuration for the ETL pipeline."""

import os

GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_TEMPERATURE = 0

DB_PATH = os.environ.get("DB_PATH", "shared/db/financial_data.duckdb")
PDF_INPUT_DIR = os.environ.get("PDF_INPUT_DIR", "shared/pdfs")
MANIFEST_PATH = os.environ.get("MANIFEST_PATH", "shared/metadata/scrape_manifest.json")

COMPANIES = {
    "DIPD": {
        "symbol": "DIPD.N0000",
        "full_name": "Dipped Products PLC",
        "security_id": 670,
        "sector": "Materials",
        "fiscal_year_end_month": 3,
    },
    "REXP": {
        "symbol": "REXP.N0000",
        "full_name": "Richard Pieris Exports PLC",
        "security_id": 771,
        "sector": "Materials",
        "fiscal_year_end_month": 3,
    },
}
