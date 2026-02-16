"""
Hashing utilities for raw data records.

Used for:
  - Deduplication (same job posted multiple times)
  - Change detection (job description updated)
  - Content integrity (verify data preserved correctly)
"""

import hashlib
from typing import Optional


def hash_content(content: Optional[str], algorithm: str = "sha256") -> Optional[str]:
    """
    Hash raw content (text or HTML).
    
    Args:
        content: Text to hash (None returns None)
        algorithm: "sha256" (default) or "md5"
    
    Returns:
        Hex digest string or None
    """
    if content is None:
        return None
    
    # Normalize: strip whitespace but preserve content structure
    normalized = content.strip()
    if not normalized:
        return None
    
    if algorithm == "sha256":
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    elif algorithm == "md5":
        return hashlib.md5(normalized.encode("utf-8")).hexdigest()
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}")


def hash_job_description(description_text: Optional[str]) -> Optional[str]:
    """Hash a job description for change detection."""
    return hash_content(description_text, algorithm="sha256")


def hash_company_content(about_text: Optional[str]) -> Optional[str]:
    """Hash company about section for change detection."""
    return hash_content(about_text, algorithm="sha256")


def hash_job_url(job_url: Optional[str]) -> Optional[str]:
    """
    Hash a job URL to create stable, unique job ID.
    
    Multiple scrapers over time might see the same job at same URL.
    This hash serves as the canonical job ID.
    """
    return hash_content(job_url, algorithm="sha256")


def hash_company_url(company_url: Optional[str]) -> Optional[str]:
    """
    Hash a company URL to create stable, unique company ID.
    
    Used for deduplication across scrape runs.
    """
    return hash_content(company_url, algorithm="sha256")
