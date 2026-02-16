"""
LinkedIn Job Scraper (v1.0.0)

Pure raw data collection from LinkedIn job listings.
No parsing, normalization, inference, or analytics.

All fields captured exactly as they appear on the page.
Missing field = None. All HTML/text is preserved.

This is a final, frozen scraper. Raw schema version 1.0.0 will not change.

Phases:
  1. Scrape job listing cards
  2. Visit each job detail page, extract all raw fields
  3. (Optional) Visit company about pages, extract raw company info
  4. Save to Parquet in data/raw/jobs/ and data/raw/companies/
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import json
import logging
import glob
from datetime import datetime
from typing import List, Dict, Any, Optional, Set

from src.scraping.description_extractor import DescriptionExtractor
from src.scraping.company_extractor import CompanyExtractor
from src.scraping.raw_schema_v1 import (
    SCRAPER_VERSION,
    RAW_SCHEMA_VERSION,
    create_job_record_template,
    create_company_record_template,
)
from src.utils.hashing_v1 import hash_job_url, hash_company_url, hash_job_description, hash_company_content

logger = logging.getLogger(__name__)


class LinkedInScraper:
    """
    Robust LinkedIn scraper with exponential backoff, detailed raw extraction,
    and optional parsing.
    """

    def __init__(
        self,
        min_delay: float = 0.5,
        max_delay: float = 1.0,
        timeout: int = 15,
        scrape_company_pages: bool = True,
        max_retries: int = 3,
    ):
        """
        Initialize LinkedIn scraper.
        
        Args:
            min_delay: Minimum delay (seconds) between requests across all phases
            max_delay: Maximum delay (seconds) between requests across all phases
            timeout: HTTP request timeout (seconds)
            scrape_company_pages: Whether to visit company about pages
            max_retries: Max retries on HTTP failures
        
        Rate limiting strategy:
        - Phase 1 (Listings): 0.5-1.0s delays + exponential backoff on 429s
        - Phase 2 (Detail pages): 1.5-4.0s delays + exponential backoff on 429s
        - Phase 3 (Company pages): 1.5-4.0s delays + exponential backoff on 429s
        
        All extractors use shared session for connection pooling and share
        the same delay parameters for consistency. Exponential backoff adds
        2^attempt seconds when 429 (rate limit) errors occur.
        """
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
        self.max_retries = max_retries
        self.scrape_company_pages = scrape_company_pages
        
        # Create single session for connection pooling across all phases
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Initialize extractors with shared session and delays
        # Phase 2: Detail extraction uses slightly higher delays to avoid 429s
        self.extractor = DescriptionExtractor(
            min_delay=max(min_delay, 1.0),  # At least 1s between detail page requests
            max_delay=max(max_delay, 2.0),  # At least 2s max delay
            timeout=timeout,
            max_retries=max_retries,
            session=self.session,
        )
        # Phase 3: Company pages use same conservative delays as details
        self.company_extractor = CompanyExtractor(
            min_delay=max(min_delay, 1.0),
            max_delay=max(max_delay, 2.0),
            timeout=timeout,
            max_retries=max_retries,
            session=self.session,
        )

        # Scrape state
        self.scrape_log: List[Dict[str, Any]] = []
        self._company_cache: Dict[str, Dict[str, Any]] = {}
        self._seen_job_ids: Set[str] = set()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: cleanup resources."""
        self.close()
        return False

    def close(self):
        """Close session and cleanup resources."""
        if hasattr(self, 'session') and self.session:
            try:
                self.session.close()
                logger.info("Session closed successfully")
            except Exception as e:
                logger.warning(f"Error closing session: {e}")

    def _load_existing_ids(self, data_dir: str):
        """Load job IDs from existing JSON files to avoid re-scraping."""
        if not os.path.exists(data_dir):
            return
            
        pattern = os.path.join(data_dir, "*.json")
        for filepath in glob.glob(pattern):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for item in data:
                            jid = item.get("job_id_raw")
                            if jid:
                                self._seen_job_ids.add(jid)
            except Exception as e:
                logger.warning(f"Could not load existing IDs from {filepath}: {e}")
        
        logger.info(f"Loaded {len(self._seen_job_ids)} existing job IDs to skip.")

    # ── Phase 1: Listing Scrape ──────────────────────────────

    def scrape_listings(
        self, keywords: str, location: str, limit: int = 25
    ) -> List[Dict[str, str]]:
        """
        Scrape job listing cards from LinkedIn's public guest API.
        Includes exponential backoff for 429s.
        """
        logger.info(f"Phase 1: Scraping up to {limit} listings for '{keywords}' in '{location}'")
        jobs = []
        start = 0
        consecutive_failures = 0
        
        while len(jobs) < limit:
            params = {
                "keywords": keywords,
                "location": location,
                "start": start,
            }

            try:
                # Retry logic for the listing page itself
                response = None
                for attempt in range(1, self.max_retries + 1):
                    try:
                        resp = requests.get(
                            self.base_url,
                            params=params,
                            headers=self.headers,
                            timeout=self.timeout,
                        )
                        if resp.status_code == 200:
                            response = resp
                            break
                        elif resp.status_code == 429:
                            wait = (2 ** attempt) + random.uniform(0, 1)
                            logger.warning(f"[Phase 1] Rate limit 429 at offset {start}. Waiting {wait:.1f}s")
                            time.sleep(wait)
                        else:
                            logger.warning(f"[Phase 1] HTTP {resp.status_code} at offset {start}")
                    except requests.RequestException:
                        pass
                
                if not response:
                    logger.error(f"[Phase 1] Failed to fetch listings at offset {start} after retries.")
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        break
                    start += 25
                    continue

                consecutive_failures = 0
                soup = BeautifulSoup(response.text, "html.parser")
                cards = soup.find_all("li")

                if not cards:
                    logger.info("No more listing cards found.")
                    break

                for card in cards:
                    if len(jobs) >= limit:
                        break

                    try:
                        # Extract basic fields
                        title_tag = card.find("h3", class_="base-search-card__title")
                        company_tag = card.find("h4", class_="base-search-card__subtitle")
                        location_tag = card.find("span", class_="job-search-card__location")
                        link_tag = card.find("a", class_="base-card__full-link")
                        date_tag = card.find("time", class_="job-search-card__listdate")

                        # Company URL
                        company_link_tag = card.find("a", class_="hidden-nested-link")
                        company_url = None
                        if company_link_tag and company_link_tag.get("href"):
                            company_url = company_link_tag["href"].split("?")[0]

                        # Job URL
                        link = link_tag["href"] if link_tag else None
                        if link:
                            link = link.split("?")[0]
                        
                        # Generate stable job ID via hashing
                        if link:
                            job_id = hash_job_url(link)
                            if job_id in self._seen_job_ids:
                                logger.debug(f"  Skipping seen job: {job_id}")
                                continue
                            self._seen_job_ids.add(job_id)

                        jobs.append({
                            "raw_title": title_tag.get_text(strip=True) if title_tag else None,
                            "company": company_tag.get_text(strip=True) if company_tag else None,
                            "raw_location": location_tag.get_text(strip=True) if location_tag else None,
                            "date_posted_raw": date_tag.get_text(strip=True) if date_tag else None,
                            "date_posted_attr": date_tag["datetime"] if date_tag and date_tag.get("datetime") else None,
                            "job_url": link,
                            "company_url": company_url,
                        })
                        logger.info(f"  [{len(jobs)}] {jobs[-1]['raw_title']} @ {jobs[-1]['company']}")

                    except Exception as e:
                        logger.warning(f"Error parsing card: {e}")
                        continue

                start += 25
                time.sleep(random.uniform(self.min_delay, self.max_delay))

            except Exception as e:
                logger.error(f"Request failed at offset {start}: {e}")
                consecutive_failures += 1
                if consecutive_failures >= 3:
                     break

        logger.info(f"Phase 1 complete: {len(jobs)} listings scraped.")
        return jobs

    # ── Phase 2 & 3: Details & Company ───────────────────────

    def enrich_listings_raw(
        self,
        listings: List[Dict[str, str]],
        search_keyword: str = "",
        search_location: str = "",
        output_dir: str = "data/raw",
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Visit each listing to extract full details and (optionally) company info.
        
        Returns: (job_records, company_records)
        Both are in raw v1.0.0 format with all hashing applied.
        """
        logger.info(f"Phase 2: Extracting {len(listings)} job detail pages")
        scrape_timestamp = datetime.now().isoformat()
        job_records = []
        company_records_dict = {}  # Deduplicated by company_id_hash
        
        # Quality tracking
        quality_stats = {"high": 0, "medium": 0, "low": 0}
        
        # Ensure output dir exists
        os.makedirs(output_dir, exist_ok=True)
        interim_file = os.path.join(output_dir, "interim_jobs.json")

        for i, listing in enumerate(listings, 1):
            job_url = listing.get("job_url")
            if not job_url:
                continue

            logger.info(f"  [{i}/{len(listings)}] {job_url}")
            
            # Extract job details (raw)
            extraction = self.extractor.extract(job_url)
            
            if not extraction["success"]:
                self._log_failure(listing, extraction.get("error", "Unknown error"))
                continue

            # Assess extraction quality
            critical_fields = ["description", "salary_raw", "applicant_count_raw"]
            found_count = sum(1 for f in critical_fields if extraction.get(f) is not None)
            
            if found_count >= 3:
                quality_level = "high"
                quality_stats["high"] += 1
            elif found_count >= 1:
                quality_level = "medium"
                quality_stats["medium"] += 1
            else:
                quality_level = "low"
                quality_stats["low"] += 1

            # Build raw job record (v1.0.0 schema)
            job_record = create_job_record_template()
            job_record["scraper_version"] = SCRAPER_VERSION
            job_record["raw_schema_version"] = RAW_SCHEMA_VERSION
            
            # Scrape metadata
            job_record["scrape_metadata"]["search_keyword"] = search_keyword
            job_record["scrape_metadata"]["search_location"] = search_location
            job_record["scrape_metadata"]["scrape_timestamp"] = scrape_timestamp
            job_record["scrape_metadata"]["user_agent_used"] = self.headers.get("User-Agent")
            
            # Job identity
            job_id_hash = hash_job_url(job_url)
            job_record["job_identity"]["job_id_raw"] = job_id_hash
            job_record["job_identity"]["job_url"] = job_url
            
            # Job card (from listing)
            job_record["job_card_raw"]["title_raw"] = listing.get("raw_title")
            job_record["job_card_raw"]["company_raw"] = listing.get("company")
            job_record["job_card_raw"]["location_raw"] = listing.get("raw_location")
            job_record["job_card_raw"]["date_posted_raw"] = listing.get("date_posted_raw")
            job_record["job_card_raw"]["date_posted_attr"] = listing.get("date_posted_attr")
            
            # Job page (from detail extraction)
            job_record["job_page_raw"]["job_description_raw_text"] = extraction.get("description")
            job_record["job_page_raw"]["job_description_raw_html"] = extraction.get("description_html")
            job_record["job_page_raw"]["description_extract_method"] = extraction.get("description_extract_method")
            job_record["job_page_raw"]["job_insight_section_raw_text"] = extraction.get("job_insight_section_raw_text")
            job_record["job_page_raw"]["job_insight_section_raw_html"] = extraction.get("job_insight_section_raw_html")
            job_record["job_page_raw"]["salary_raw_text"] = extraction.get("salary_raw")
            job_record["job_page_raw"]["salary_status"] = extraction.get("salary_status")
            job_record["job_page_raw"]["applicant_count_raw"] = extraction.get("applicant_count_raw")
            job_record["job_page_raw"]["applicant_count_status"] = extraction.get("applicant_count_status")
            job_record["job_page_raw"]["easy_apply_flag_raw"] = extraction.get("easy_apply_flag_raw")
            job_record["job_page_raw"]["easy_apply_flag_status"] = extraction.get("easy_apply_flag_status")
            job_record["job_page_raw"]["remote_label_raw"] = extraction.get("remote_label_raw")
            job_record["job_page_raw"]["remote_label_status"] = extraction.get("remote_label_status")
            job_record["job_page_raw"]["posted_by_raw"] = extraction.get("posted_by_raw")
            job_record["job_page_raw"]["posted_by_status"] = extraction.get("posted_by_status")
            job_record["job_page_raw"]["location_from_panel_raw"] = extraction.get("location_from_panel_raw")
            job_record["job_page_raw"]["employment_type_raw"] = extraction.get("employment_type_raw")
            job_record["job_page_raw"]["seniority_raw"] = extraction.get("seniority_raw")
            job_record["job_page_raw"]["industry_raw"] = extraction.get("industry_raw")
            job_record["job_page_raw"]["job_function_raw"] = extraction.get("job_function_raw")
            job_record["job_page_raw"]["embedded_json_ld"] = extraction.get("embedded_json_ld")
            job_record["job_page_raw"]["embedded_job_json"] = extraction.get("embedded_job_json")
            
            # Company reference
            company_url = listing.get("company_url")
            if company_url:
                job_record["company_info"]["company_url"] = company_url
                job_record["company_info"]["company_id_hash"] = hash_company_url(company_url)
            
            # Quality & tracking
            job_record["quality_tracking"]["extraction_quality"] = quality_level
            job_record["quality_tracking"]["selector_hits"] = extraction.get("_selector_hits", 0)
            job_record["quality_tracking"]["retry_count"] = extraction.get("_retry_count", 0)
            
            # Hashing
            desc_text = extraction.get("description")
            job_record["hashing"]["job_description_content_hash"] = hash_job_description(desc_text)
            job_record["hashing"]["job_post_id_hash"] = job_id_hash
            
            job_records.append(job_record)
            
            # Fetch company (if enabled)
            if self.scrape_company_pages and company_url:
                company_record = self._fetch_company_data_raw(company_url, scrape_timestamp)
                if company_record:
                    company_id = company_record["company_identity"]["company_id_hash"]
                    # Deduplicate: keep latest timestamp
                    if company_id not in company_records_dict:
                        company_records_dict[company_id] = company_record
                    else:
                        # Update last_seen
                        company_records_dict[company_id]["timestamps"]["last_seen"] = scrape_timestamp
            
            # Checkpoint every 10 records
            if i % 10 == 0:
                self._save_interim(job_records, interim_file)

        # Final save
        self._save_interim(job_records, interim_file)
        
        # Cleanup
        try:
            if os.path.exists(interim_file):
                os.remove(interim_file)
        except Exception as e:
            logger.warning(f"Could not cleanup interim file: {e}")
        
        # Log summary
        total = sum(quality_stats.values())
        if total > 0:
            logger.info(
                f"Phase 2 complete: {len(job_records)}/{len(listings)} jobs. "
                f"Quality: {quality_stats['high']:.0f}% high, {quality_stats['medium']:.0f}% med, {quality_stats['low']:.0f}% low"
            )
        
        company_list = list(company_records_dict.values())
        logger.info(f"Phase 3 complete: {len(company_list)} unique companies extracted")
        
        return job_records, company_list

    def _fetch_company_data_raw(self, company_url: str, scrape_timestamp: str) -> Optional[Dict[str, Any]]:
        """Fetch and extract company data into raw v1.0.0 schema."""
        cached = self._company_cache.get(company_url)
        if cached is not None:
            return cached

        target_url = self._normalize_company_url(company_url)
        
        # Retry with exponential backoff
        for attempt in range(1, self.max_retries + 1):
            logger.info(f"    Fetching company: {target_url} (attempt {attempt}/{self.max_retries})")

            result = self.company_extractor.extract(target_url)
            
            if result.get("success"):
                # Build raw company record (v1.0.0 schema)
                company_record = create_company_record_template()
                company_record["scraper_version"] = SCRAPER_VERSION
                company_record["raw_schema_version"] = RAW_SCHEMA_VERSION
                
                # Company identity
                company_id_hash = hash_company_url(company_url)
                company_record["company_identity"]["company_id_hash"] = company_id_hash
                company_record["company_identity"]["company_url"] = company_url
                
                # Company page (from extraction)
                company_record["company_page_raw"]["company_about_raw_text"] = result.get("company_about")
                company_record["company_page_raw"]["company_about_raw_html"] = result.get("company_about_html")
                company_record["company_page_raw"]["company_industry_raw"] = result.get("company_industry")
                company_record["company_page_raw"]["company_size_raw"] = result.get("company_size")
                company_record["company_page_raw"]["company_headquarters_raw"] = result.get("company_headquarters")
                company_record["company_page_raw"]["company_type_raw"] = result.get("company_type")
                company_record["company_page_raw"]["company_specialties_raw"] = result.get("company_specialties")
                company_record["company_page_raw"]["company_founded_year"] = result.get("company_founded_year")
                company_record["company_page_raw"]["company_website_raw"] = result.get("company_website")
                company_record["company_page_raw"]["company_employees_count_raw"] = result.get("company_employees_count")
                
                # Hashing
                about_text = result.get("company_about")
                company_record["hashing"]["company_about_content_hash"] = hash_company_content(about_text)
                company_record["hashing"]["company_id_hash"] = company_id_hash
                
                # Timestamps
                company_record["timestamps"]["first_seen"] = scrape_timestamp
                company_record["timestamps"]["last_seen"] = scrape_timestamp
                
                # Quality
                company_record["quality_tracking"]["selector_hits"] = result.get("_selector_hits", 0)
                company_record["quality_tracking"]["retry_count"] = attempt - 1
                
                # Cache successful result
                self._company_cache[company_url] = company_record
                return company_record
            else:
                error_msg = result.get("error", "Unknown error")
                logger.warning(f"      Company fetch failed: {error_msg}")
                
                if attempt < self.max_retries:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.info(f"      Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"    ✗ Company fetch permanently failed after {self.max_retries} attempts: {target_url}")
        
        # Cache the failure to avoid repeated attempts
        self._company_cache[company_url] = None
        return None

    def _normalize_company_url(self, company_url: str) -> str:
        """Normalize company URL to LinkedIn about page format."""
        if not company_url:
            return ""
        
        base = company_url.split("?")[0]  # Remove query params
        if "/about" not in base:
            base = base.rstrip("/") + "/about/"
        return base

    def _save_interim(self, records: List[Dict], filepath: str):
        """
        Save current progress to a JSON file (overwrites previous version).
        
        This is used for checkpointing during enrichment to prevent data loss.
        We overwrite rather than append to keep only one interim file.
        Fails loudly on serialization errors instead of silently converting.
        """
        if not records:
            return
        try:
            # Don't use default=str; let serialization errors bubble up
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)
            logger.debug(f"Interim checkpoint saved: {len(records)} records")
        except TypeError as e:
            # Debug: find the problematic record
            logger.error(f"JSON serialization error in {filepath}: {e}")
            for i, rec in enumerate(records):
                try:
                    json.dumps(rec)
                except TypeError as rec_err:
                    logger.error(
                        f"  Record {i} is not JSON-serializable: "
                        f"{list(rec.keys())} — {rec_err}"
                    )
                    # Print a sample of the problematic value
                    for k, v in rec.items():
                        try:
                            json.dumps(v)
                        except TypeError:
                            logger.error(f"    Non-serializable field '{k}': {type(v)} = {str(v)[:100]}")
            raise
        except Exception as e:
            logger.error(f"Failed to save interim file {filepath}: {e}")
            raise

    def _log_failure(self, listing: Dict, reason: str, extraction_metadata: Optional[Dict[str, Any]] = None):
        """
        Log extraction failure with detailed metadata for debugging.
        
        Args:
            listing: The listing card info (title, company, url)
            reason: Why the extraction failed
            extraction_metadata: Optional dict with field-level failure info
        """
        failure_record = {
            "job_url": listing.get("job_url"),
            "raw_title": listing.get("raw_title"),
            "company": listing.get("company"),
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
        }
        
        if extraction_metadata:
            failure_record["extraction_details"] = extraction_metadata
        
        self.scrape_log.append(failure_record)
        
        logger.warning(
            f"    ✗ Extraction failed for '{listing.get('raw_title')}' "
            f"@ {listing.get('company')}: {reason}"
        )


    # ── Entry Points ─────────────────────────────────────────

    def _scrape_internal(
        self,
        keywords: str,
        location: str,
        limit: int,
        output_dir: str,
        use_existing_ids: bool = True,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Internal method: executes the full scraping pipeline and returns raw records.
        Returns: (job_records, company_records) in v1.0.0 raw schema format.
        Does not parse or save to disk (caller decides).
        """
        if use_existing_ids:
            self._load_existing_ids(output_dir)

        listings = self.scrape_listings(keywords, location, limit)
        if not listings:
            return [], []

        job_records, company_records = self.enrich_listings_raw(listings, keywords, location, output_dir)
        return job_records, company_records

    def scrape_raw(
        self,
        keywords: str = "Data Scientist",
        location: str = "",
        limit: int = 25,
        output_dir: str = "data/raw",
        use_existing_ids: bool = True
    ) -> pd.DataFrame:
        """
        Execute raw scrape pipeline and save to JSON.
        Returns DataFrame with raw job records (no parsing/normalization).
        Includes keyword in filename for easy identification.
        """
        job_records, company_records = self._scrape_internal(keywords, location, limit, output_dir, use_existing_ids)
        
        # Save raw results
        if job_records:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Sanitize keyword for filename (lowercase, replace spaces with underscores)
            keyword_slug = keywords.lower().replace(" ", "_").replace("/", "_")
            
            # Save jobs
            jobs_path = os.path.join(output_dir, f"linkedin_raw_jobs_{keyword_slug}_{timestamp}.json")
            try:
                with open(jobs_path, "w", encoding="utf-8") as f:
                    json.dump(job_records, f, indent=2, ensure_ascii=False)
                logger.info(f"Raw job records saved: {jobs_path}")
            except TypeError as e:
                logger.error(f"JSON serialization error when saving {jobs_path}: {e}")
                raise
            
            # Save companies if available
            if company_records:
                companies_path = os.path.join(output_dir, f"linkedin_raw_companies_{keyword_slug}_{timestamp}.json")
                try:
                    with open(companies_path, "w", encoding="utf-8") as f:
                        json.dump(company_records, f, indent=2, ensure_ascii=False)
                    logger.info(f"Raw company records saved: {companies_path}")
                except TypeError as e:
                    logger.error(f"JSON serialization error when saving {companies_path}: {e}")
                    raise
            
            return pd.DataFrame(job_records)
        
        return pd.DataFrame()

    def scrape(
        self,
        keywords: str = "Data Scientist",
        location: str = "",
        limit: int = 25,
        output_dir: str = "data/raw",
    ) -> pd.DataFrame:
        """
        Execute full scrape and parse pipeline.
        Returns DataFrame with parsed/normalized fields.
        
        Note: This method still uses the old parse_job function.
        For raw-only mode, use scrape_raw() instead.
        """
        logger.info("Starting full scrape + parse pipeline...")
        job_records, company_records = self._scrape_internal(keywords, location, limit, output_dir, use_existing_ids=True)
        
        if not job_records:
            logger.warning("No records scraped, aborting parse pipeline")
            return pd.DataFrame()
            
        # Parse raw records into structured schema
        logger.info(f"Parsing {len(job_records)} raw records...")
        parsed_records = []
        
        for i, rec in enumerate(job_records, 1):
            try:
                # Extract fields from nested raw schema
                raw_title = rec.get("job_card_raw", {}).get("title_raw")
                company = rec.get("job_card_raw", {}).get("company_raw")
                raw_location = rec.get("job_card_raw", {}).get("location_raw")
                date_posted_raw = rec.get("job_card_raw", {}).get("date_posted_raw")
                job_url = rec.get("job_identity", {}).get("job_url")
                job_description_raw = rec.get("job_page_raw", {}).get("job_description_raw_text")
                
                parsed = parse_job(
                    raw_title=raw_title,
                    company=company,
                    raw_location=raw_location,
                    date_posted_raw=date_posted_raw,
                    job_url=job_url,
                    job_description_raw=job_description_raw,
                    page_metadata={},
                    scrape_timestamp=rec.get("scrape_metadata", {}).get("scrape_timestamp")
                )
                if parsed:
                    # Preserve extraction metadata for quality tracking
                    parsed["_extraction_quality"] = rec.get("quality_tracking", {}).get("extraction_quality")
                    parsed_records.append(parsed)
            except Exception as e:
                logger.warning(f"Parse failed for {rec.get('job_identity', {}).get('job_url')}: {e}")
                continue

        # Save parsed results
        if parsed_records:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            keyword_slug = keywords.lower().replace(" ", "_").replace("/", "_")
            processed_dir = "data/processed"
            os.makedirs(processed_dir, exist_ok=True)
            parsed_path = os.path.join(processed_dir, f"linkedin_parsed_{keyword_slug}_{timestamp}.json")
            try:
                with open(parsed_path, "w", encoding="utf-8") as f:
                    json.dump(parsed_records, f, indent=2, ensure_ascii=False)
                logger.info(f"Parsed results saved: {parsed_path}")
            except TypeError as e:
                logger.error(f"JSON serialization error when saving {parsed_path}: {e}")
                raise
        
        logger.info(f"Parse complete: {len(parsed_records)}/{len(job_records)} records parsed")
        return pd.DataFrame(parsed_records)

    def get_scrape_report(self) -> Dict[str, Any]:
        return {
            "total_failures": len(self.scrape_log),
            "top_reasons": pd.Series([x["reason"] for x in self.scrape_log]).value_counts().to_dict() if self.scrape_log else {}
        }
