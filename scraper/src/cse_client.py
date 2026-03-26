"""CSE API client - fetches quarterly report PDF links via the discovered API endpoint."""

import logging
from dataclasses import dataclass
from datetime import datetime

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import CompanyConfig, CSE_BASE_URL

logger = logging.getLogger(__name__)

CDN_BASE = "https://cdn.cse.lk"
FINANCIAL_ANNOUNCEMENT_URL = f"{CSE_BASE_URL}/api/getFinancialAnnouncement"


@dataclass
class ReportInfo:
    title: str
    pdf_url: str
    period_description: str


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
async def fetch_financial_reports(
    company: CompanyConfig,
    from_date: str = "2021-01-01",
    to_date: str | None = None,
) -> list[ReportInfo]:
    """Fetch financial report listing from CSE API.

    Uses POST https://www.cse.lk/api/getFinancialAnnouncement
    with application/x-www-form-urlencoded body.
    Response key is 'reqFinancialAnnouncemnets' (CSE typo).
    """
    if to_date is None:
        to_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(
        f"Fetching reports for {company.company_id} "
        f"(securityId={company.security_id}) from {from_date} to {to_date}"
    )

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        response = await client.post(
            FINANCIAL_ANNOUNCEMENT_URL,
            data={
                "companyIds": str(company.security_id),
                "fromDate": from_date,
                "toDate": to_date,
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": f"{CSE_BASE_URL}/pages/financial-reports/financial-reports.component.html",
            },
        )
        response.raise_for_status()
        data = response.json()

    # Note: CSE API has a typo in the response key
    announcements = data.get("reqFinancialAnnouncemnets", [])
    logger.info(f"API returned {len(announcements)} announcements for {company.company_id}")

    reports: list[ReportInfo] = []
    for item in announcements:
        path = item.get("path", "")
        if not path:
            continue

        pdf_url = f"{CDN_BASE}/{path}"
        title = item.get("fileText", "") or item.get("name", "Report")

        reports.append(
            ReportInfo(
                title=title,
                pdf_url=pdf_url,
                period_description=title,
            )
        )
        logger.info(f"  Found: {title} -> {pdf_url}")

    return reports
