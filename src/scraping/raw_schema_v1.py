"""
Raw Schema Definitions (v1.0.0)

Defines the immutable, raw-only schemas for:
  - Job postings (all fields captured from HTML, no transformation)
  - Company info (all fields from about pages, no inference)

These schemas are frozen and will not change. All future preprocessing
will work with these raw fields.

Versions:
  - scraper_version: "1.0.0" (final frozen scraper)
  - raw_schema_version: "1.0.0" (final frozen raw schema)
"""

from typing import Dict, Any, Optional, List

SCRAPER_VERSION = "1.0.0"
RAW_SCHEMA_VERSION = "1.0.0"


def create_job_record_template() -> Dict[str, Any]:
    """
    Template for a raw job record.
    
    All values are captured exactly as they appear on the page.
    No transformation, cleaning, inference, or normalization.
    Missing values = None.
    """
    return {
        # ─── Versioning ──────────────────────────────
        "scraper_version": SCRAPER_VERSION,
        "raw_schema_version": RAW_SCHEMA_VERSION,

        # ─── Scrape Metadata ─────────────────────────
        "scrape_metadata": {
            "search_keyword": None,
            "search_location": None,
            "scrape_timestamp": None,  # ISO 8601
            "user_agent_used": None,
        },

        # ─── Job Identity ────────────────────────────
        "job_identity": {
            "job_id_raw": None,  # Hash of job_url
            "job_url": None,
        },

        # ─── Job Card Raw (from listing page) ────────
        "job_card_raw": {
            "title_raw": None,
            "company_raw": None,
            "location_raw": None,
            "date_posted_raw": None,  # e.g. "2 months ago"
            "date_posted_attr": None,  # e.g. "2025-12-16" from datetime attr
        },

        # ─── Job Page Raw (from detail page) ────────
        "job_page_raw": {
            # Description (primary fields)
            "job_description_raw_text": None,
            "job_description_raw_html": None,
            "description_extract_method": None,  # Which selector worked

            # Insight panel (metadata summary)
            "job_insight_section_raw_text": None,
            "job_insight_section_raw_html": None,

            # Salary, applicants, etc. (from top card)
            "salary_raw_text": None,
            "salary_status": None,  # "success" or "not_found"
            "applicant_count_raw": None,
            "applicant_count_status": None,
            "easy_apply_flag_raw": None,
            "easy_apply_flag_status": None,
            "remote_label_raw": None,
            "remote_label_status": None,

            # Posted-by recruiter info
            "posted_by_raw": None,
            "posted_by_status": None,

            # Location fallback (if listing missed it)
            "location_from_panel_raw": None,

            # Criteria section (key-value pairs)
            "employment_type_raw": None,
            "seniority_raw": None,
            "industry_raw": None,
            "job_function_raw": None,

            # Structured data embedded in page
            "embedded_json_ld": None,  # Full JSON-LD as string
            "embedded_job_json": None,  # Any job-related JSON found
        },

        # ─── Company URL & Page ──────────────────────
        "company_info": {
            "company_url": None,
            "company_id_hash": None,  # Hash of company_url
        },

        # ─── Quality & Tracking ──────────────────────
        "quality_tracking": {
            "extraction_quality": None,  # "high" | "medium" | "low"
            "selector_hits": None,  # Count of successful CSS selectors
            "status_code_history": [],  # List of HTTP codes (for 429 retries)
            "retry_count": 0,
        },

        # ─── Content Hashing ────────────────────────
        "hashing": {
            "job_description_content_hash": None,  # SHA-256 of description_raw_text
            "job_post_id_hash": None,  # SHA-256 of job_url
        },
    }


def create_company_record_template() -> Dict[str, Any]:
    """
    Template for a raw company record.
    
    Separate from job records. Deduplicated by company_id_hash.
    All values captured exactly as on the page.
    """
    return {
        # ─── Versioning ──────────────────────────────
        "scraper_version": SCRAPER_VERSION,
        "raw_schema_version": RAW_SCHEMA_VERSION,

        # ─── Company Identity ────────────────────────
        "company_identity": {
            "company_id_hash": None,  # Hash of company_url
            "company_name_raw": None,
            "company_url": None,
        },

        # ─── Company Page Raw ────────────────────────
        "company_page_raw": {
            "company_about_raw_text": None,
            "company_about_raw_html": None,
            "company_industry_raw": None,
            "company_size_raw": None,
            "company_headquarters_raw": None,
            "company_type_raw": None,
            "company_specialties_raw": None,
        },

        # ─── Content Hashing ────────────────────────
        "hashing": {
            "company_content_hash": None,  # SHA-256 of about_raw_text
        },

        # ─── Timestamps ──────────────────────────────
        "timestamps": {
            "first_seen": None,  # ISO 8601
            "last_seen": None,  # ISO 8601
        },

        # ─── Quality ─────────────────────────────────
        "quality_tracking": {
            "extraction_quality": None,
            "retry_count": 0,
        },
    }


def get_job_schema_fields() -> List[str]:
    """Get flattened list of all job record field paths."""
    template = create_job_record_template()
    fields = []
    
    def flatten(obj, prefix=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                full_key = f"{prefix}.{k}" if prefix else k
                if isinstance(v, dict):
                    flatten(v, full_key)
                else:
                    fields.append(full_key)
        return fields
    
    return flatten(template)


def get_company_schema_fields() -> List[str]:
    """Get flattened list of all company record field paths."""
    template = create_company_record_template()
    fields = []
    
    def flatten(obj, prefix=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                full_key = f"{prefix}.{k}" if prefix else k
                if isinstance(v, dict):
                    flatten(v, full_key)
                else:
                    fields.append(full_key)
        return fields
    
    return flatten(template)
