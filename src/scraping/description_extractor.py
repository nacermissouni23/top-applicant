"""
Description extractor: fetches individual LinkedIn job pages and extracts
the full job description text plus all available raw metadata.

This module handles the HTTP/HTML layer only — no NLP, normalization, or inference.
All values are stored exactly as they appear on the page.
"""

import json
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
from typing import Optional, Dict, Any, List

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

# Selectors for the job insight / top-card metadata panel
_INSIGHT_SELECTORS = [
    ("div", {"class": "job-details-jobs-unified-top-card__job-insight"}),
    ("div", {"class": "jobs-unified-top-card__job-insight"}),
    ("ul", {"class": "job-details-jobs-unified-top-card__job-insight"}),
    ("div", {"class": "top-card-layout__entity-info"}),
    ("section", {"class": "top-card-layout"}),
]

# Selectors for salary section
_SALARY_SELECTORS = [
    ("div", {"class": "salary-main-rail__data-body"}),
    ("span", {"class": "top-card-layout__salary-info"}),
    ("div", {"class": "compensation__salary"}),
    ("div", {"class": "job-details-jobs-unified-top-card__job-insight--highlight"}),
]

# Selectors for applicant count
_APPLICANT_SELECTORS = [
    ("span", {"class": "num-applicants__caption"}),
    ("span", {"class": "topcard__flavor--metadata"}),
    ("figcaption", {"class": "num-applicants__caption"}),
]

# Selectors for Easy Apply badge
_EASY_APPLY_SELECTORS = [
    ("span", {"class": "easy-apply-badge"}),
    ("button", {"class": "jobs-apply-button--top-card"}),
    ("span", {"class": "topcard__flavor--easy-apply"}),
]

# Selectors for posted-by / recruiter info
_POSTED_BY_SELECTORS = [
    ("div", {"class": "message-the-recruiter"}),
    ("div", {"class": "hirer-card__hirer-information"}),
    ("a", {"class": "message-the-recruiter__cta"}),
]

# Selectors for location in the top card / insight panel
_LOCATION_SELECTORS = [
    ("span", {"class": "job-details-jobs-unified-top-card__primary-description-container"}),
    ("span", {"class": "topcard__flavor--bullet"}),
    ("span", {"class": "job-search-card__location"}),
]

# Selectors for remote/on-site label
_REMOTE_LABEL_SELECTORS = [
    ("span", {"class": "job-details-jobs-unified-top-card__workplace-type"}),
    ("span", {"class": "topcard__flavor--workplace-type"}),
    ("span", {"class": "job-search-card__workplace-type"}),
]


class DescriptionExtractor:
    """
    Fetches a LinkedIn job detail page (guest/public view) and extracts
    the full job description plus all available raw metadata.

    This is a pure extraction layer — no inference, normalization, or transformation.
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
        Fetch a job detail page with exponential backoff retries.
        
        Returns BeautifulSoup or None on failure.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code == 200:
                    return BeautifulSoup(response.text, "html.parser")
                elif response.status_code == 429:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Rate limited (429) on attempt {attempt}. Waiting {wait_time:.1f}s for {url}")
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

    # ── Description Extraction ───────────────────────────────

    def extract_description_text(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract the full job description as raw plaintext.

        Returns { 'value': str|None, 'method': int|None }
        """
        for i, (tag, attrs) in enumerate(_JD_SELECTORS):
            element = soup.find(tag, attrs)
            if element:
                raw_text = element.get_text(separator="\n", strip=True)
                if len(raw_text) > 50:
                    return {"value": raw_text, "method": i}

        # Last resort: largest text block on page
        all_divs = soup.find_all("div")
        best = ""
        for div in all_divs:
            text = div.get_text(separator="\n", strip=True)
            if len(text) > len(best):
                best = text

        if len(best) > 200:
            logger.info("Used fallback: largest div text extraction")
            return {"value": best, "method": -1} # -1 indicates fallback

        return {"value": None, "method": None}

    def extract_description_html(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract the full job description as raw HTML string.
        """
        for tag, attrs in _JD_SELECTORS:
            element = soup.find(tag, attrs)
            if element:
                html = str(element)
                if len(html) > 50:
                    return html
        return None

    # ── Job Insight / Metadata Panel ─────────────────────────

    def extract_location_from_panel(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract job location from the top card / insight panel.

        Important fallback if listing card had empty/incomplete location (e.g. worldwide search).
        """
        for i, (tag, attrs) in enumerate(_LOCATION_SELECTORS):
            element = soup.find(tag, attrs)
            if element:
                text = element.get_text(strip=True)
                if text:
                    return {"value": text, "method": i}
        return {"value": None, "method": None}

    def extract_insight_section(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        """
        Extract the job insight panel (the metadata section above the description).

        Returns raw text and raw HTML of the insight section.
        """
        for tag, attrs in _INSIGHT_SELECTORS:
            element = soup.find(tag, attrs)
            if element:
                return {
                    "job_insight_section_raw_text": element.get_text(separator="\n", strip=True),
                    "job_insight_section_raw_html": str(element),
                }
        return {
            "job_insight_section_raw_text": None,
            "job_insight_section_raw_html": None,
        }

    def extract_fields_with_status(
        self, soup: BeautifulSoup, selectors: List, name: str
    ) -> Dict[str, Any]:
        """Generic extractor that returns value + metadata status."""
        for i, (tag, attrs) in enumerate(selectors):
            element = soup.find(tag, attrs)
            if element:
                text = element.get_text(strip=True)
                if text:
                    return {
                        f"{name}_raw": text,
                        f"{name}_status": "success",
                        f"{name}_method": i,
                    }
        return {
            f"{name}_raw": None,
            f"{name}_status": "not_found",
            f"{name}_method": None,
        }

    # ── Criteria Section (structured metadata) ───────────────

    def extract_page_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract structured metadata from the job criteria section.

        Returns raw key-value pairs exactly as displayed on the page.
        Keys are lowercased with spaces replaced by underscores.
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

    def extract_criteria_fields(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        """
        Extract individual criteria fields into named raw columns.

        Maps the criteria section into:
          employment_type_raw, seniority_raw, industry_raw, job_function_raw
        """
        raw_metadata = self.extract_page_metadata(soup)

        return {
            "employment_type_raw": raw_metadata.get("employment_type", None),
            "seniority_raw": raw_metadata.get("seniority_level", None),
            "industry_raw": raw_metadata.get("industries", None),
            "job_function_raw": raw_metadata.get("job_function", None),
        }

    # ── Embedded Structured Data ─────────────────────────────

    def extract_embedded_json_ld(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract JSON-LD structured data from <script type="application/ld+json"> tags.

        Returns the raw JSON string exactly as embedded. Does not parse or transform.
        """
        scripts = soup.find_all("script", {"type": "application/ld+json"})
        for script in scripts:
            content = script.string
            if content:
                content = content.strip()
                try:
                    json.loads(content)
                    return content
                except (json.JSONDecodeError, ValueError):
                    logger.debug("Found ld+json script but content is not valid JSON")
                    continue
        return None

    def extract_embedded_job_json(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract any job-related JSON objects embedded in <script> tags.

        Looks for script tags containing job data patterns (jobPosting,
        hiringOrganization, etc.). Returns the raw JSON string.
        """
        job_patterns = [
            "jobPosting", "hiringOrganization", "employmentType",
            "jobLocation", "baseSalary", "validThrough",
        ]

        scripts = soup.find_all("script")
        for script in scripts:
            content = script.string
            if not content:
                continue

            content_stripped = content.strip()

            # Skip if it's ld+json (already captured separately)
            if script.get("type") == "application/ld+json":
                continue

            # Check if content has job-related patterns
            has_job_data = any(pat in content_stripped for pat in job_patterns)
            if not has_job_data:
                continue

            # Try to find a JSON object within the script content
            start_brace = content_stripped.find("{")
            start_bracket = content_stripped.find("[")

            if start_brace == -1 and start_bracket == -1:
                continue

            # Pick whichever comes first
            if start_brace != -1 and (start_bracket == -1 or start_brace < start_bracket):
                start = start_brace
            else:
                start = start_bracket

            # Find matching end via depth tracking
            depth = 0
            for i in range(start, len(content_stripped)):
                if content_stripped[i] in "{[":
                    depth += 1
                elif content_stripped[i] in "}]":
                    depth -= 1
                    if depth == 0:
                        candidate = content_stripped[start : i + 1]
                        try:
                            json.loads(candidate)
                            return candidate
                        except (json.JSONDecodeError, ValueError):
                            break

        return None

    # ── Full Extraction Pipeline ─────────────────────────────

    def extract(self, url: str) -> Dict[str, Any]:
        """
        Full extraction pipeline for a single job URL.

        Returns dict with all raw fields + extraction metadata.
        Tracks which selectors succeeded/failed for debugging.
        """
        self._polite_delay()

        # Initialize result with defaults
        result = {
            "success": False,
            "error": None,
            "description": None,
            "description_html": None,
            "description_extract_method": None,
            "embedded_json_ld": None,
            "embedded_job_json": None,
            "_selector_hits": 0,  # Track how many selectors found content
        }

        soup = self.fetch_page(url)
        if soup is None:
            result["error"] = "Failed to fetch page"
            return result

        # Description (text + html) — CRITICAL FIELD
        desc_res = self.extract_description_text(soup)
        if desc_res["value"] is None:
            # Log which selector methods were tried
            logger.error(
                f"CRITICAL: Description extraction failed for {url}. "
                f"All {len(_JD_SELECTORS)} selectors returned None. "
                f"LinkedIn HTML structure may have changed."
            )
            result["error"] = "Could not extract job description (selectors failed)"
            return result

        result["description"] = desc_res["value"]
        result["description_extract_method"] = desc_res["method"]
        result["_selector_hits"] += 1
        
        # If using fallback selector (-1), warn that selectors may be broken
        if desc_res.get("method") == -1:
            logger.warning(
                f"Description extraction used fallback method (largest div) for {url}. "
                f"Primary selectors may not match current LinkedIn HTML."
            )

        result["description_html"] = self.extract_description_html(soup)

        # Location fallback (if listing card missed it)
        loc_res = self.extract_location_from_panel(soup)
        result["location_from_panel_raw"] = loc_res["value"]
        if loc_res["value"] is not None:
            result["_selector_hits"] += 1

        # Insight / metadata panel
        insight = self.extract_insight_section(soup)
        result.update(insight)
        if insight.get("job_insight_section_raw_text") is not None:
            result["_selector_hits"] += 1

        # Individual raw metadata fields with status tracking
        salary_res = self.extract_fields_with_status(soup, _SALARY_SELECTORS, "salary")
        result.update(salary_res)
        if salary_res.get("salary_status") == "success":
            result["_selector_hits"] += 1

        applicant_res = self.extract_fields_with_status(soup, _APPLICANT_SELECTORS, "applicant_count")
        result.update(applicant_res)
        if applicant_res.get("applicant_count_status") == "success":
            result["_selector_hits"] += 1

        easy_apply_res = self.extract_fields_with_status(soup, _EASY_APPLY_SELECTORS, "easy_apply_flag")
        result.update(easy_apply_res)
        
        remote_res = self.extract_fields_with_status(soup, _REMOTE_LABEL_SELECTORS, "remote_label")
        result.update(remote_res)
        
        posted_by_res = self.extract_fields_with_status(soup, _POSTED_BY_SELECTORS, "posted_by")
        result.update(posted_by_res)

        # Criteria section fields
        criteria = self.extract_criteria_fields(soup)
        result.update(criteria)
        if any(v is not None for v in criteria.values()):
            result["_selector_hits"] += 1

        # Backward compat: page_metadata dict
        result["page_metadata"] = self.extract_page_metadata(soup)
        if result["page_metadata"]:
            result["_selector_hits"] += 1

        # Embedded structured data
        result["embedded_json_ld"] = self.extract_embedded_json_ld(soup)
        if result["embedded_json_ld"] is not None:
            result["_selector_hits"] += 1

        result["embedded_job_json"] = self.extract_embedded_job_json(soup)
        if result["embedded_job_json"] is not None:
            result["_selector_hits"] += 1

        result["success"] = True
        return result
