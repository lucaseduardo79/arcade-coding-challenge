"""Configuration for CSE scraper."""

from dataclasses import dataclass


@dataclass
class CompanyConfig:
    company_id: str
    symbol: str
    full_name: str
    security_id: int


COMPANIES = [
    CompanyConfig(
        company_id="DIPD",
        symbol="DIPD.N0000",
        full_name="Dipped Products PLC",
        security_id=670,
    ),
    CompanyConfig(
        company_id="REXP",
        symbol="REXP.N0000",
        full_name="Richard Pieris Exports PLC",
        security_id=771,
    ),
]

CSE_BASE_URL = "https://www.cse.lk"
CSE_COMPANY_PROFILE_URL = (
    f"{CSE_BASE_URL}/pages/company-profile/company-profile.component.html"
)
CSE_API_BASE = f"{CSE_BASE_URL}/api"

MIN_YEARS = 3
