"""
Description extractor: fetches individual LinkedIn job pages and extracts
the full job description text.

This module handles the HTTP/HTML layer only — no NLP or normalization.
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Selectors for LinkedIn guest job view pages (public, no login)
# LinkedIn serves different HTML for guest vs authenticated users.
_JD_SELECTORS = [
    # Primary: guest job view
    ("div", {"class": "show-more-less-html__markup"}),
    ("div", {"class": "description__text"}),
    ("section", {"class": "show-more-less-html"}),
    # Fallback: article body
    ("article", {"class": "jobs-description"}),
    ("div", {"class": "jobs-description__content"}),
    # Generic fallback
    ("div", {"class": "core-section-container__content"}),
]

# Selectors for structured metadata on the detail page
_DETAIL_SELECTORS = {
    "employment_type": [
        ("span", {"class": "description__job-criteria-text"}),
    ],
    "seniority_level": [
        ("span", {"class": "description__job-criteria-text"}),
    ],
}


class DescriptionExtractor:
    """
    Fetches a LinkedIn job detail page (guest/public view) and extracts
    the full job description plus any structured metadata on the page.
    """

    def __init__(
        self,
        min_delay: float = 1.5,
        max_delay: float = 4.0,
        timeout: int = 15,
        max_retries: int = 2,
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
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _polite_delay(self):
        """Respectful crawling delay."""
        time.sleep(random.uniform(self.min_delay, self.max_delay))

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch a job detail page with retries.
        
        Returns BeautifulSoup or None on failure.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code == 200:
                    return BeautifulSoup(response.text, "html.parser")
                elif response.status_code == 429:
                    logger.warning(f"Rate limited (429) on attempt {attempt} for {url}")
                    time.sleep(random.uniform(5, 10))
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

    def extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract the full job description text from the parsed page.
        
        Tries multiple CSS selectors in priority order.
        Returns cleaned plaintext or None.
        """
        for tag, attrs in _JD_SELECTORS:
            element = soup.find(tag, attrs)
            if element:
                # Get text, preserving paragraph breaks
                raw_text = element.get_text(separator="\n", strip=True)
                if len(raw_text) > 50:  # Minimum viable description
                    return raw_text

        # Last resort: look for the largest text block on the page
        all_divs = soup.find_all("div")
        best = ""
        for div in all_divs:
            text = div.get_text(separator="\n", strip=True)
            if len(text) > len(best):
                best = text
        
        if len(best) > 200:
            logger.info("Used fallback: largest div text extraction")
            return best

        return None

    def extract_page_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract structured metadata from the job detail page
        (employment type, seniority section, etc.)
        """
        metadata = {}

        # LinkedIn detail pages have a criteria section with items like:
        # Seniority level, Employment type, Job function, Industries
        criteria_items = soup.find_all("li", class_="description__job-criteria-item")
        for item in criteria_items:
            header = item.find("h3", class_="description__job-criteria-subheader")
            value = item.find("span", class_="description__job-criteria-text")
            if header and value:
                key = header.get_text(strip=True).lower().replace(" ", "_")
                val = value.get_text(strip=True)
                metadata[key] = val

        return metadata

    def extract(self, url: str) -> Dict[str, Any]:
        """
        Full extraction pipeline for a single job URL.
        
        Returns dict with:
            - description: str or None
            - page_metadata: dict of structured fields from the page
            - success: bool
            - error: str or None
        """
        self._polite_delay()

        result = {
            "description": None,
            "page_metadata": {},
            "success": False,
            "error": None,
        }

        soup = self.fetch_page(url)
        if soup is None:
            result["error"] = "Failed to fetch page"
            return result

        description = self.extract_description(soup)
        if description is None:
            result["error"] = "Could not extract job description"
            return result

        result["description"] = description
        result["page_metadata"] = self.extract_page_metadata(soup)
        result["success"] = True
        return result
