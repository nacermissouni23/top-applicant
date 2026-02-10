"""
Stable hashing utilities for generating deterministic job IDs.
"""

import hashlib


def generate_job_id(url: str) -> str:
    """
    Generate a stable, deterministic job_id from a URL.
    
    Uses SHA-256 truncated to 16 hex chars. This gives 64 bits of entropy —
    effectively collision-free for datasets under ~1 billion records.
    
    Args:
        url: The canonical job URL (query params should be stripped first).
    
    Returns:
        A 16-character hex string.
    """
    # Normalize: strip whitespace, lowercase
    normalized = url.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
