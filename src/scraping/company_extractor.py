"""
Company page extractor: fetches LinkedIn company pages and extracts
raw metadata about the company (about section, industry, size, etc.).

This module handles the HTTP/HTML layer only — no NLP, normalization, or inference.
All values are stored exactly as they appear on the page.
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import logging
from typing import Optional, Dict, Any, List, Tuple

logger = logging.getLogger(__name__)

# Selectors for LinkedIn company about section (public/guest view)
_ABOUT_SELECTORS = [
    ("p", {"class": "break-words white-space-pre-wrap mb5 text-body-small t-black--light"}),
    ("section", {"class": "core-section-container"}),
    ("p", {"class": "break-words"}),
    ("div", {"class": "core-section-container__content"}),
    ("section", {"data-test-id": "about-us"}),
]

# Selectors for company metadata fields on the about page
# These are often in a dl/dt/dd structure or specific data-test-id fields
_COMPANY_META_SELECTORS = {
    "industry": [
        ("div", {"data-test-id": "about-us__industry"}),
        ("dd", {"class": "basic-info-item__content"}),
    ],
    "company_size": [
        ("div", {"data-test-id": "about-us__size"}),
        ("dd", {"class": "basic-info-item__content"}),
    ],
    "headquarters": [
        ("div", {"data-test-id": "about-us__headquarters"}),
        ("dd", {"class": "basic-info-item__content"}),
    ],
    "type": [
        ("div", {"data-test-id": "about-us__organizationType"}),
        ("dd", {"class": "basic-info-item__content"}),
    ],
    "specialties": [
        ("div", {"data-test-id": "about-us__specialties"}),
        ("dd", {"class": "basic-info-item__content"}),
    ],
}

# Generic dt/dd patterns used on LinkedIn company pages
_COMPANY_FIELDS_MAP = {
    "industry": "company_industry",
    "company size": "company_size",
    "headquarters": "company_headquarters",
    "type": "company_type",
    "specialties": "company_specialties",
    "founded": "company_founded",
    "website": "company_website",
}


class CompanyExtractor:
    """
    Fetches a LinkedIn company page (guest/public view) and extracts
    raw metadata about the company.

    This is a pure extraction layer — no inference, normalization, or transformation.
    Company pages are deduplicated at the caller level (one fetch per unique company).
    """

    def __init__(
        self,
        min_delay: float = 1.5,
        max_delay: float = 4.0,
        timeout: int = 15,
        max_retries: int = 3,
        session: Optional[requests.Session] = None,
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        if session is None:
            self.session = requests.Session()
            self.session.headers.update(self.headers)
        else:
            self.session = session

    def _polite_delay(self):
        """Respectful crawling delay."""
        time.sleep(random.uniform(self.min_delay, self.max_delay))

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch a company page with exponential backoff retries.

        Returns BeautifulSoup or None on failure.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code == 200:
                    return BeautifulSoup(response.text, "html.parser")
                elif response.status_code == 429:
                    # Exponential backoff: 2, 4, 8 seconds...
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        f"Rate limited (429) on attempt {attempt} for {url}. "
                        f"Backing off for {wait_time:.1f}s"
                    )
                    time.sleep(wait_time)
                else:
                    logger.warning(
                        f"HTTP {response.status_code} on attempt {attempt} for {url}"
                    )
            except requests.RequestException as e:
                logger.warning(f"Request error on attempt {attempt} for {url}: {e}")

            if attempt < self.max_retries:
                self._polite_delay()

        logger.error(f"Failed to fetch {url} after {self.max_retries} attempts")
        return None

    def extract_about_section(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract the company about/description section as raw plaintext and HTML.
        Includes metadata about which selector worked.
        """
        for i, (tag, attrs) in enumerate(_ABOUT_SELECTORS):
            element = soup.find(tag, attrs)
            if element:
                text = element.get_text(separator="\n", strip=True)
                if len(text) > 10:  # Minimal validation
                    return {
                        "company_about_raw_text": text,
                        "company_about_raw_html": str(element),
                        "company_about_method": i
                    }
        
        return {
            "company_about_raw_text": None,
            "company_about_raw_html": None,
            "company_about_method": None
        }

    def extract_company_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract structured company metadata from the about page.
        
        Tries to map generic label-value pairs (dt/dd) to known fields.
        Returns a dict with raw values and 'method' metadata.
        """
        result = {}
        
        # Initialize all fields to None
        for field in _COMPANY_FIELDS_MAP.values():
            result[f"{field}_raw"] = None
            result[f"{field}_method"] = None

        # Strategy 1: Look for dl list (Description List) which is common for metadata
        # We scan all dt/dd pairs on the page
        dt_elements = soup.find_all("dt")
        
        for dt in dt_elements:
            raw_label = dt.get_text(strip=True).lower()
            
            # Check if this label matches any of our known fields
            matched_field = None
            for keyword, field_name in _COMPANY_FIELDS_MAP.items():
                if keyword in raw_label:
                    matched_field = field_name
                    break
            
            if matched_field:
                # Found a known field label, look for the value
                dd = dt.find_next_sibling("dd")
                if dd:
                    value = dd.get_text(strip=True)
                    if value:
                        result[f"{matched_field}_raw"] = value
                        result[f"{matched_field}_method"] = "dt_dd_scan"

        # Strategy 2: Fallback to specific classes/ids if dt/dd didn't work
        # (This is less robust as classes change, but good as backup)
        if not result.get("company_industry_raw"):
             ind_elem = soup.find("div", {"data-test-id": "about-us__industry"})
             if ind_elem:
                 val = ind_elem.get_text(strip=True)
                 if val:
                     result["company_industry_raw"] = val
                     result["company_industry_method"] = "data_test_id"

        return result

    def extract(self, company_url: str) -> Dict[str, Any]:
        """
        Full extraction pipeline for a single company URL.

        Returns dict with:
            - company_about_raw_text
            - company_about_raw_html
            - company_industry_raw
            - company_size_raw
            - company_headquarters_raw
            - company_type_raw
            - company_specialties_raw
            - company_founded_raw
            - company_website_raw
            - success: bool
            - error: str or None
        """
        self._polite_delay()

        # Initialize result with global success/error
        result = {
            "success": False,
            "error": None,
        }

        # Initialize data fields
        result.update({
             "company_about_raw_text": None,
             "company_about_raw_html": None,
             "company_about_method": None,
        })
        for field in _COMPANY_FIELDS_MAP.values():
            result[f"{field}_raw"] = None
            result[f"{field}_method"] = None

        soup = self.fetch_page(company_url)
        if soup is None:
            result["error"] = "Failed to fetch company page"
            return result

        # 1. About Section
        about_res = self.extract_about_section(soup)
        result.update(about_res)

        # 2. Metadata Section (Industry, Size, HQ, etc.)
        metadata_res = self.extract_company_metadata(soup)
        result.update(metadata_res)

        result["success"] = True
        return result
