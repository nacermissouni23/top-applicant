"""
Enhanced LinkedIn scraper: extends the simple scraper to visit each job link,
extract full descriptions, and produce structured records.

Preserves the original listing scraper logic and adds a detail-page pass.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from src.scraping.description_extractor import DescriptionExtractor
from src.preprocessing.job_parser import parse_job

logger = logging.getLogger(__name__)


class LinkedInScraper:
    """
    Two-phase LinkedIn scraper:
      Phase 1: Scrape job listing cards (title, company, location, link)
      Phase 2: Visit each link → extract full description → parse into schema
    """

    def __init__(
        self,
        min_delay: float = 1.5,
        max_delay: float = 4.0,
        timeout: int = 15,
    ):
        self.base_url = (
            "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
        )
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.timeout = timeout
        self.extractor = DescriptionExtractor(
            min_delay=min_delay, max_delay=max_delay, timeout=timeout
        )

        # Scrape run metadata
        self.scrape_log: List[Dict[str, Any]] = []

    # ── Phase 1: Listing Scrape ──────────────────────────────

    def scrape_listings(
        self, keywords: str, location: str, limit: int = 25
    ) -> List[Dict[str, str]]:
        """
        Scrape job listing cards from LinkedIn's public guest API.
        
        Returns a list of raw listing dicts (title, company, location, date, link).
        """
        logger.info(f"Phase 1: Scraping up to {limit} listings for '{keywords}' in '{location}'")
        jobs = []
        start = 0

        while len(jobs) < limit:
            params = {
                "keywords": keywords,
                "location": location,
                "start": start,
            }

            try:
                response = requests.get(
                    self.base_url,
                    params=params,
                    headers=self.headers,
                    timeout=self.timeout,
                )

                if response.status_code != 200:
                    logger.warning(f"HTTP {response.status_code} at offset {start}")
                    break

                soup = BeautifulSoup(response.text, "html.parser")
                cards = soup.find_all("li")

                if not cards:
                    logger.info("No more listing cards found.")
                    break

                for card in cards:
                    if len(jobs) >= limit:
                        break

                    try:
                        title_tag = card.find("h3", class_="base-search-card__title")
                        company_tag = card.find("h4", class_="base-search-card__subtitle")
                        location_tag = card.find("span", class_="job-search-card__location")
                        link_tag = card.find("a", class_="base-card__full-link")
                        date_tag = card.find("time", class_="job-search-card__listdate")

                        title = title_tag.get_text(strip=True) if title_tag else "N/A"
                        company = company_tag.get_text(strip=True) if company_tag else "N/A"
                        loc = location_tag.get_text(strip=True) if location_tag else "N/A"
                        link = link_tag["href"] if link_tag else "N/A"
                        date_posted = date_tag.get_text(strip=True) if date_tag else "N/A"

                        # Strip query params for clean URL
                        if link != "N/A" and "?" in link:
                            link = link.split("?")[0]

                        jobs.append({
                            "raw_title": title,
                            "company": company,
                            "raw_location": loc,
                            "date_posted_raw": date_posted,
                            "job_url": link,
                        })
                        logger.info(f"  [{len(jobs)}] {title} @ {company}")

                    except Exception as e:
                        logger.warning(f"Error parsing card: {e}")
                        continue

                start += 25
                time.sleep(random.uniform(self.min_delay, self.max_delay))

            except Exception as e:
                logger.error(f"Request failed at offset {start}: {e}")
                break

        logger.info(f"Phase 1 complete: {len(jobs)} listings scraped.")
        return jobs

    # ── Phase 2: Detail Extraction & Parsing ─────────────────

    def enrich_listings(
        self, listings: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """
        For each listing, visit the job URL, extract the full description,
        and parse into the structured output schema.
        
        Records that fail quality checks are logged and discarded.
        
        Returns:
            List of fully parsed job records.
        """
        logger.info(f"Phase 2: Enriching {len(listings)} listings with full descriptions")
        scrape_timestamp = datetime.now().isoformat()
        parsed_records = []
        
        for i, listing in enumerate(listings, 1):
            job_url = listing.get("job_url", "")
            if not job_url or job_url == "N/A":
                self._log_failure(listing, "Missing job URL")
                continue

            logger.info(f"  [{i}/{len(listings)}] Fetching: {job_url}")

            # Extract description from detail page
            extraction = self.extractor.extract(job_url)

            if not extraction["success"]:
                self._log_failure(listing, extraction.get("error", "Unknown error"))
                continue

            description = extraction["description"]
            page_metadata = extraction.get("page_metadata", {})

            # Parse into structured record
            record = parse_job(
                raw_title=listing["raw_title"],
                company=listing["company"],
                raw_location=listing["raw_location"],
                date_posted_raw=listing["date_posted_raw"],
                job_url=job_url,
                job_description_raw=description,
                page_metadata=page_metadata,
                scrape_timestamp=scrape_timestamp,
            )

            if record is not None:
                parsed_records.append(record)
                logger.info(
                    f"    ✓ Parsed: {record['normalized_title']} | "
                    f"Seniority: {record['seniority_level']} | "
                    f"Skills: {len(record['skills_required'])} required"
                )
            else:
                self._log_failure(listing, "Failed quality checks (no desc or no skills)")

        # Summary
        success_rate = len(parsed_records) / len(listings) * 100 if listings else 0
        logger.info(
            f"Phase 2 complete: {len(parsed_records)}/{len(listings)} "
            f"records parsed ({success_rate:.0f}% success rate)"
        )

        return parsed_records

    def _log_failure(self, listing: Dict, reason: str):
        """Log a scrape/parse failure."""
        entry = {
            "job_url": listing.get("job_url", "N/A"),
            "raw_title": listing.get("raw_title", "N/A"),
            "company": listing.get("company", "N/A"),
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
        }
        self.scrape_log.append(entry)
        logger.warning(f"    ✗ DISCARDED: {entry['raw_title']} @ {entry['company']} — {reason}")

    # ── Full Pipeline ────────────────────────────────────────

    def scrape(
        self,
        keywords: str = "Data Scientist",
        location: str = "Remote",
        limit: int = 25,
    ) -> pd.DataFrame:
        """
        Full scrape pipeline: listing → detail → parse → DataFrame.
        
        Args:
            keywords: Search keywords
            location: Location filter
            limit: Max number of listings to scrape (keep at 20-30 for dev)
        
        Returns:
            DataFrame with the full output schema, or empty DataFrame.
        """
        # Phase 1
        listings = self.scrape_listings(keywords, location, limit)
        if not listings:
            logger.error("No listings found. Aborting.")
            return pd.DataFrame()

        # Phase 2
        records = self.enrich_listings(listings)
        if not records:
            logger.error("No records survived parsing. Aborting.")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        return df

    def get_scrape_report(self) -> Dict[str, Any]:
        """Get a summary report of the scrape run."""
        return {
            "total_failures": len(self.scrape_log),
            "failure_reasons": pd.Series(
                [log["reason"] for log in self.scrape_log]
            ).value_counts().to_dict() if self.scrape_log else {},
            "failures": self.scrape_log,
        }
