"""
End-to-end pipeline orchestrator for Top Applicant data collection.

Ties together scraping, extraction, parsing, and output with:
  - Versioned outputs (Parquet + CSV)
  - Scrape logs
  - Quality reports
  - Schema validation
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any, List

from src.scraping.linkedin_scraper import LinkedInScraper
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
    
    Parquet preserves list types; CSV flattens them to JSON strings.
    
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
        # CSV (secondary — lists become JSON strings)
        csv_path = os.path.join(
            output_dir, f"jobs_{DATASET_VERSION}_{timestamp}.csv"
        )
        df_csv = df.copy()
        list_cols = ["title_keywords", "skills_required", "skills_optional", "tools_frameworks"]
        for col in list_cols:
            if col in df_csv.columns:
                df_csv[col] = df_csv[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, list) else x
                )
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
            "failure_reasons": report["failure_reasons"],
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
    output_dir: str = "data/processed",
    raw_output_dir: str = "data/raw",
    save_csv: bool = True,
) -> Optional[pd.DataFrame]:
    """
    Run the complete scraping + parsing pipeline.
    
    Args:
        keywords: Job search query
        location: Location filter (empty string = worldwide)
        limit: Max jobs to scrape (keep 20-30 for dev)
        output_dir: Directory for processed output
        raw_output_dir: Directory for raw data preservation
        save_csv: Also save CSV alongside Parquet
    
    Returns:
        Parsed DataFrame, or None on failure.
    """
    log_path = setup_logging()
    logger.info("=" * 60)
    logger.info("TOP APPLICANT — Data Collection Pipeline")
    logger.info(f"Schema version: {SCHEMA_VERSION}")
    logger.info(f"Dataset version: {DATASET_VERSION}")
    logger.info(f"Search: '{keywords}' | Location: '{location}' | Limit: {limit}")
    logger.info("=" * 60)

    scraper = LinkedInScraper()

    # Phase 1: Scrape listings
    listings = scraper.scrape_listings(keywords, location, limit)
    if not listings:
        logger.error("No listings found. Pipeline aborted.")
        return None

    # Save raw listings for reprocessing
    save_raw_listings(listings, raw_output_dir)

    # Phase 2: Enrich with descriptions + parse
    records = scraper.enrich_listings(listings)
    if not records:
        logger.error("No records survived parsing. Pipeline aborted.")
        return None

    df = pd.DataFrame(records)

    # Validate schema
    schema_report = validate_schema(df)
    if not schema_report["valid"]:
        logger.warning(f"Schema validation issues: {schema_report}")
    else:
        logger.info("Schema validation: PASSED")

    # Save outputs
    paths = save_dataset(df, output_dir, save_csv)
    
    # Save reports
    scrape_report = scraper.get_scrape_report()
    save_scrape_report(scrape_report, schema_report)

    # Summary
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Records: {len(df)}")
    logger.info(f"Failures: {scrape_report['total_failures']}")
    logger.info(f"Output: {paths}")
    logger.info(f"Log: {log_path}")
    logger.info("=" * 60)

    return df
