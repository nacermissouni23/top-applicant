"""
End-to-end pipeline orchestrator for Top Applicant data collection.

Ties together scraping, extraction, parsing, and output with:
  - Versioned outputs (Parquet + CSV/JSON)
  - Scrape logs
  - Quality reports
  - Raw schema v1.0.0 support
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any, List

from src.scraping.linkedin_scraper import LinkedInScraper
from src.scraping.raw_schema_v1 import SCRAPER_VERSION, RAW_SCHEMA_VERSION
from src.preprocessing.vocabularies import (
    SCHEMA_VERSION,
    DATASET_VERSION,
    SCHEMA_FIELDS,
)

logger = logging.getLogger(__name__)


def setup_logging(log_dir: str = "outputs/logs") -> str:
    """Configure logging to both console and file. Returns log path."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"scrape_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )
    return log_path


def validate_schema(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate that the DataFrame conforms to the expected schema.
    
    Returns a report dict with missing/extra columns and type checks.
    """
    expected = set(SCHEMA_FIELDS)
    actual = set(df.columns)
    
    missing = expected - actual
    extra = actual - expected
    
    # Check list columns are actually lists (not strings)
    list_columns = [
        "title_keywords", "skills_required", "skills_optional", "tools_frameworks"
    ]
    list_issues = {}
    for col in list_columns:
        if col in df.columns:
            non_list = df[col].apply(lambda x: not isinstance(x, list)).sum()
            if non_list > 0:
                list_issues[col] = f"{non_list} rows are not lists"

    return {
        "valid": len(missing) == 0 and len(list_issues) == 0,
        "missing_columns": list(missing),
        "extra_columns": list(extra),
        "list_format_issues": list_issues,
        "row_count": len(df),
        "schema_version": SCHEMA_VERSION,
    }


def _df_to_parquet(df: pd.DataFrame, path: str) -> None:
    """
    Save DataFrame to Parquet, handling list columns for PyArrow compatibility.
    
    PyArrow 23+ can fail on Python object columns. We explicitly convert
    list columns to PyArrow list(string) arrays to avoid ArrowKeyError.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    list_cols = ["title_keywords", "skills_required", "skills_optional", "tools_frameworks"]

    # Build a PyArrow Table with explicit types for list columns
    arrays = {}
    for col in df.columns:
        if col in list_cols:
            # Convert Python lists → PyArrow list<string>
            arrays[col] = pa.array(
                df[col].tolist(), type=pa.list_(pa.string())
            )
        else:
            arrays[col] = pa.array(df[col].tolist(), from_pandas=True)

    table = pa.table(arrays)
    pq.write_table(table, path)


def save_dataset(
    df: pd.DataFrame,
    output_dir: str = "data/processed",
    also_save_csv: bool = True,
) -> Dict[str, str]:
    """
    Save dataset as versioned Parquet (+ optional CSV).
    
    Parquet preserves list types; CSV drops list columns (cannot be properly serialized).
    
    Returns dict of output file paths.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    paths = {}

    # Parquet (primary — preserves types)
    parquet_path = os.path.join(
        output_dir, f"jobs_{DATASET_VERSION}_{timestamp}.parquet"
    )
    _df_to_parquet(df, parquet_path)
    paths["parquet"] = parquet_path
    logger.info(f"Saved Parquet: {parquet_path}")

    # Also save a "latest" copy
    latest_parquet = os.path.join(output_dir, f"jobs_latest.parquet")
    _df_to_parquet(df, latest_parquet)
    paths["parquet_latest"] = latest_parquet

    if also_save_csv:
        # CSV (secondary — drops list columns since CSV cannot preserve them)
        csv_path = os.path.join(
            output_dir, f"jobs_{DATASET_VERSION}_{timestamp}.csv"
        )
        df_csv = df.copy()
        list_cols = ["title_keywords", "skills_required", "skills_optional", "tools_frameworks"]
        
        # Drop list columns from CSV
        cols_to_drop = [col for col in list_cols if col in df_csv.columns]
        if cols_to_drop:
            logger.warning(
                f"CSV export: dropping list columns {cols_to_drop} "
                "(use Parquet for complete data)"
            )
            df_csv = df_csv.drop(columns=cols_to_drop)
        
        df_csv.to_csv(csv_path, index=False)
        paths["csv"] = csv_path
        logger.info(f"Saved CSV: {csv_path}")

    return paths


def save_raw_listings(
    listings: List[Dict[str, str]],
    output_dir: str = "data/raw",
) -> str:
    """Save raw listing data (before enrichment) for reprocessing."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"raw_listings_{timestamp}.json")
    
    with open(path, "w", encoding="utf-8") as f:
        json.dump(listings, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Saved raw listings: {path}")
    return path


def save_raw_jobs(
    job_records: List[Dict[str, Any]],
    output_dir: str = "data/raw",
    keywords: str = "",
) -> str:
    """
    Save raw job records in v1.0.0 schema format.
    Uses JSON for now (Parquet support can be added later).
    Includes keyword in filename if provided.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Sanitize keyword for filename if provided
    if keywords:
        keyword_slug = keywords.lower().replace(" ", "_").replace("/", "_")
        path = os.path.join(output_dir, f"linkedin_raw_jobs_{keyword_slug}_{timestamp}.json")
    else:
        path = os.path.join(output_dir, f"linkedin_raw_jobs_{timestamp}.json")
    
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(job_records, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(job_records)} raw job records: {path}")
    except TypeError as e:
        logger.error(f"JSON serialization error in save_raw_jobs: {e}")
        raise
    
    return path


def save_raw_companies(
    company_records: List[Dict[str, Any]],
    output_dir: str = "data/raw",
    keywords: str = "",
) -> str:
    """
    Save raw company records in v1.0.0 schema format.
    Uses JSON for now (Parquet support can be added later).
    Includes keyword in filename if provided.
    """
    if not company_records:
        logger.info("No company records to save")
        return ""
    
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Sanitize keyword for filename if provided
    if keywords:
        keyword_slug = keywords.lower().replace(" ", "_").replace("/", "_")
        path = os.path.join(output_dir, f"linkedin_raw_companies_{keyword_slug}_{timestamp}.json")
    else:
        path = os.path.join(output_dir, f"linkedin_raw_companies_{timestamp}.json")
    
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(company_records, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(company_records)} raw company records: {path}")
    except TypeError as e:
        logger.error(f"JSON serialization error in save_raw_companies: {e}")
        raise
    
    return path


def save_scrape_report(
    report: Dict[str, Any],
    schema_report: Dict[str, Any],
    output_dir: str = "outputs/tables",
) -> str:
    """Save scrape report (failures, quality metrics)."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"scrape_report_{timestamp}.json")

    full_report = {
        "timestamp": datetime.now().isoformat(),
        "schema_version": SCHEMA_VERSION,
        "dataset_version": DATASET_VERSION,
        "scrape_report": {
            "total_failures": report["total_failures"],
            "failure_reasons": report.get("failure_reasons", report.get("top_reasons", {})),
        },
        "schema_validation": schema_report,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(full_report, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved scrape report: {path}")
    return path


def run_pipeline(
    keywords: str = "Data Scientist",
    location: str = "",
    limit: int = 25,
    raw_output_dir: str = "data/raw",
    scrape_company_pages: bool = True,
) -> Optional[pd.DataFrame]:
    """
    Run the raw data collection pipeline (v1.0.0 - Raw Only).
    
    Scrapes listings, enriches with details, optionally fetches company pages,
    and saves raw data in v1.0.0 schema to data/raw.
    
    No parsing or normalization is performed by the scraper.
    
    Args:
        keywords: Job search query
        location: Location filter (empty string = worldwide)
        limit: Max jobs to scrape
        raw_output_dir: Directory for raw data archive
        scrape_company_pages: Whether to visit company about pages
    
    Returns:
        DataFrame with raw job records in v1.0.0 schema, or None on failure.
    """
    log_path = setup_logging()
    logger.info("=" * 60)
    logger.info("TOP APPLICANT — Data Collection Pipeline v1.0.0")
    logger.info(f"Raw Schema Version: {RAW_SCHEMA_VERSION}")
    logger.info(f"Scraper Version: {SCRAPER_VERSION}")
    logger.info(f"Search: '{keywords}' | Location: '{location}' | Limit: {limit}")
    logger.info(f"Mode: Raw Data Collection Only (No Parsing)")
    logger.info("=" * 60)

    scraper = LinkedInScraper(scrape_company_pages=scrape_company_pages)

    # Raw pipeline: Scrape only (no parsing)
    df = scraper.scrape_raw(
        keywords=keywords,
        location=location,
        limit=limit,
        output_dir=raw_output_dir
    )

    if df is None or df.empty:
        logger.error("Pipeline produced no data. Aborting.")
        return None

    # Summary
    schema_report = {
        "valid": True, 
        "note": "Raw mode - no schema validation enforced",
        "scraper_version": SCRAPER_VERSION,
        "raw_schema_version": RAW_SCHEMA_VERSION,
    }

    # Save reports
    scrape_report = scraper.get_scrape_report()
    save_scrape_report(scrape_report, schema_report)

    # Summary logging
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Records: {len(df)}")
    logger.info(f"Failures: {scrape_report['total_failures']}")
    logger.info(f"Output directory: {raw_output_dir}")
    logger.info(f"Log: {log_path}")
    logger.info("=" * 60)

    return df

