"""
NLP-based job parser: transforms raw scraped data into structured, normalized fields.

All parsing is deterministic (regex / vocabulary-based). No LLM calls.
This module is the core intelligence layer between raw scrape data and the output schema.

⚠️  DEPRECATION WARNING (v1.0.0):
This module is deprecated and no longer used by the core scraper pipeline.
The scraper now focuses exclusively on raw data collection with frozen v1.0.0 schema.
For parsing and preprocessing, please use a separate preprocessing pipeline.
Last update: February 2025 — Frozen for backward compatibility only.
"""

import re
import logging
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

from src.preprocessing.vocabularies import (
    TITLE_NORMALIZATION_RULES,
    TITLE_KEYWORD_PATTERNS,
    SKILL_SYNONYMS,
    TOOLS_AND_FRAMEWORKS,
    ROLE_TYPE_SIGNALS,
    SENIORITY_TITLE_SIGNALS,
    SENIORITY_VERB_SIGNALS,
    EXPERIENCE_SENIORITY_MAP,
    EMPLOYMENT_TYPE_SIGNALS,
    WORK_MODE_SIGNALS,
    RoleType,
    SeniorityLevel,
    EmploymentType,
    WorkMode,
)
from src.preprocessing.location_parser import parse_location
from src.utils.hashing import generate_job_id

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# TITLE NORMALIZATION
# ─────────────────────────────────────────────

def normalize_title(raw_title: str) -> str:
    """
    Map a noisy job title to a controlled canonical title.
    
    Examples:
        "ML/AI Fraud Data Scientist" → "Data Scientist"
        "Machine Learning Engineer (L4)" → "Machine Learning Engineer"
        "Senior Staff Data Scientist - NLP" → "Data Scientist"
    """
    if not raw_title:
        return "Unknown"

    lower = raw_title.lower()
    # Remove level tags, parentheticals, dashes-suffixes for matching
    cleaned = re.sub(r"\(.*?\)", "", lower)  # Remove (L4), (Remote), etc.
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    for pattern, canonical in TITLE_NORMALIZATION_RULES:
        if pattern in cleaned:
            return canonical

    return raw_title.strip()


def extract_title_keywords(raw_title: str) -> List[str]:
    """
    Extract meaningful keywords from a raw job title.
    
    Returns a deduplicated list of keywords found.
    """
    if not raw_title:
        return []

    keywords = []
    title_upper = raw_title  # Preserve case for matching
    title_lower = raw_title.lower()

    for kw in TITLE_KEYWORD_PATTERNS:
        # Use word boundary matching for short keywords to avoid false positives
        if len(kw) <= 3:
            pattern = r"\b" + re.escape(kw) + r"\b"
            if re.search(pattern, title_upper, re.IGNORECASE):
                keywords.append(kw)
        else:
            if kw.lower() in title_lower:
                keywords.append(kw)

    return list(dict.fromkeys(keywords))  # Deduplicate, preserve order


# ─────────────────────────────────────────────
# SKILLS EXTRACTION
# ─────────────────────────────────────────────

# Patterns that indicate "required" vs "nice-to-have" sections
_REQUIRED_SECTION_RE = re.compile(
    r"(requirement|required|must have|what you.ll need|qualifications|"
    r"what we.re looking for|minimum|basic qualifications|"
    r"what you bring|essential)",
    re.IGNORECASE,
)
_OPTIONAL_SECTION_RE = re.compile(
    r"(nice.to.have|preferred|bonus|plus|ideal|desirable|"
    r"additional|not required but|good to have|extra credit)",
    re.IGNORECASE,
)


def _find_skills_in_text(text: str) -> List[str]:
    """Find all recognized skills/tools in a text block."""
    found = set()
    text_lower = text.lower()

    for raw_variant, canonical in SKILL_SYNONYMS.items():
        # Word-boundary match for short tokens to avoid false positives
        if len(raw_variant) <= 2:
            # For very short tokens (R, Go), require exact word boundary
            pattern = r"(?<![a-zA-Z])" + re.escape(raw_variant) + r"(?![a-zA-Z])"
            if re.search(pattern, text_lower):
                found.add(canonical)
        else:
            if raw_variant in text_lower:
                found.add(canonical)

    return sorted(found)


def extract_skills(
    description: str,
) -> Tuple[List[str], List[str], List[str]]:
    """
    Extract skills_required, skills_optional, and tools_frameworks from
    a job description.
    
    Strategy:
    1. Split description into sections by headings / bullet patterns.
    2. Classify sections as "required" or "optional".
    3. Extract skills from each section.
    4. Skills appearing in both default to required.
    5. tools_frameworks = subset of all skills that are in TOOLS_AND_FRAMEWORKS.
    
    Returns:
        (skills_required, skills_optional, tools_frameworks)
    """
    if not description:
        return [], [], []

    # Split the description into paragraphs/sections
    paragraphs = re.split(r"\n{2,}|\n(?=[A-Z•●■-])", description)

    required_skills: set = set()
    optional_skills: set = set()
    all_skills: set = set()

    current_section = "required"  # Default assumption

    for para in paragraphs:
        # Detect section type
        if _REQUIRED_SECTION_RE.search(para[:150]):
            current_section = "required"
        elif _OPTIONAL_SECTION_RE.search(para[:150]):
            current_section = "optional"

        skills = _find_skills_in_text(para)
        all_skills.update(skills)

        if current_section == "optional":
            optional_skills.update(skills)
        else:
            required_skills.update(skills)

    # Skills in both → promote to required
    optional_skills -= required_skills

    # If we didn't find section structure, all go to required
    if not required_skills and optional_skills:
        required_skills = optional_skills
        optional_skills = set()
    
    # If we found no section structure at all, put all in required
    if not required_skills and not optional_skills and all_skills:
        required_skills = all_skills

    # Tools/frameworks = intersection with known tools
    tools = sorted(
        (required_skills | optional_skills) & TOOLS_AND_FRAMEWORKS
    )

    return sorted(required_skills), sorted(optional_skills), tools


# ─────────────────────────────────────────────
# ROLE TYPE INFERENCE
# ─────────────────────────────────────────────

def infer_role_type(
    raw_title: str, description: str, tools: List[str]
) -> str:
    """
    Infer the role type from title + description + tools.
    
    Scoring with weighted sections:
      - Title keywords: 3x weight (strongest signal)
      - Required/Responsibilities section: 2x weight
      - Nice-to-have section: 0.5x weight (shouldn't dominate)
      - Tool matches: 1x weight
    """
    title_lower = raw_title.lower() if raw_title else ""
    desc_lower = description.lower() if description else ""

    # Split description into core vs nice-to-have
    nice_to_have_start = len(desc_lower)
    for marker in ["nice to have", "nice-to-have", "preferred", "bonus", "plus"]:
        idx = desc_lower.find(marker)
        if idx != -1 and idx < nice_to_have_start:
            nice_to_have_start = idx

    core_desc = desc_lower[:nice_to_have_start]
    optional_desc = desc_lower[nice_to_have_start:]

    scores: Dict[str, float] = {}
    for role_type, keywords in ROLE_TYPE_SIGNALS.items():
        score = 0.0
        for kw in keywords:
            if kw in title_lower:
                score += 3.0  # Title is strongest signal
            if kw in core_desc:
                score += 2.0  # Core description
            if kw in optional_desc:
                score += 0.5  # Nice-to-have (weak signal)

        # Tool-based boost
        if role_type == RoleType.GENAI_LLM:
            genai_tools = {"LangChain", "LlamaIndex", "OpenAI API", "Hugging Face Transformers"}
            score += len(set(tools) & genai_tools)
        elif role_type == RoleType.ML_ENGINEER:
            mlops_tools = {"Docker", "Kubernetes", "MLflow", "Kubeflow", "Apache Airflow"}
            score += len(set(tools) & mlops_tools)

        if score > 0:
            scores[role_type] = score

    if not scores:
        return RoleType.UNKNOWN.value

    # Return the role with the highest score
    best_role = max(scores, key=scores.get)
    return best_role.value


# ─────────────────────────────────────────────
# SENIORITY INFERENCE
# ─────────────────────────────────────────────

def infer_seniority(
    raw_title: str,
    description: str,
    min_years: Optional[int],
) -> str:
    """
    Multi-signal seniority inference.
    
    Priority:
    1. Explicit level in title (L3, L4, L5, Senior, Staff, etc.)
    2. Years of experience (from description)
    3. Responsibility verbs in description
    4. Default: Unknown
    """
    signals: List[SeniorityLevel] = []
    title_lower = raw_title.lower() if raw_title else ""
    desc_lower = description.lower() if description else ""

    # 1. Title signals (highest priority)
    for keyword, level in SENIORITY_TITLE_SIGNALS.items():
        if keyword in title_lower:
            signals.append(level)

    # 2. Experience years
    if min_years is not None:
        for low, high, level in EXPERIENCE_SENIORITY_MAP:
            if low <= min_years < high:
                signals.append(level)
                break

    # 3. Responsibility verbs
    for verb, level in SENIORITY_VERB_SIGNALS.items():
        if verb in desc_lower:
            signals.append(level)

    if not signals:
        return SeniorityLevel.UNKNOWN.value

    # Use the highest (most senior) signal — conservative approach
    # Order: JUNIOR < MID < SENIOR < STAFF_LEAD
    rank = {
        SeniorityLevel.JUNIOR: 0,
        SeniorityLevel.MID: 1,
        SeniorityLevel.SENIOR: 2,
        SeniorityLevel.STAFF_LEAD: 3,
        SeniorityLevel.UNKNOWN: -1,
    }

    best = max(signals, key=lambda s: rank.get(s, -1))
    return best.value


# ─────────────────────────────────────────────
# EXPERIENCE EXTRACTION
# ─────────────────────────────────────────────

# Patterns for extracting years of experience
_EXPERIENCE_PATTERNS = [
    # "3+ years" / "3-5 years" / "3 to 5 years"
    re.compile(
        r"(\d+)\s*\+?\s*(?:to|-)\s*\d*\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp\.?)?",
        re.IGNORECASE,
    ),
    # "3+ years of experience"
    re.compile(
        r"(\d+)\s*\+?\s*(?:years?|yrs?)\s+(?:of\s+)?(?:experience|exp\.?)",
        re.IGNORECASE,
    ),
    # "minimum 3 years"
    re.compile(
        r"(?:minimum|at\s+least|min\.?)\s*(\d+)\s*(?:years?|yrs?)",
        re.IGNORECASE,
    ),
    # "experience: 3+ years"
    re.compile(
        r"experience\s*:\s*(\d+)\s*\+?\s*(?:years?|yrs?)",
        re.IGNORECASE,
    ),
]

# Patterns that capture the full experience sentence/line
_EXPERIENCE_SENTENCE_RE = re.compile(
    r"[^\n.]*(?:\d+\s*\+?\s*(?:years?|yrs?))[^\n.]*[.\n]?",
    re.IGNORECASE,
)


def extract_experience(description: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Extract minimum years of experience and the raw experience sentence.
    
    Returns:
        (min_years_experience, experience_text)
        min_years is None if not explicitly stated.
    """
    if not description:
        return None, None

    min_years = None

    # Try each pattern, take the first match
    for pattern in _EXPERIENCE_PATTERNS:
        match = pattern.search(description)
        if match:
            try:
                min_years = int(match.group(1))
                break
            except (ValueError, IndexError):
                continue

    # Extract the raw sentence mentioning experience
    experience_text = None
    sent_match = _EXPERIENCE_SENTENCE_RE.search(description)
    if sent_match:
        experience_text = sent_match.group(0).strip()

    return min_years, experience_text


# ─────────────────────────────────────────────
# EMPLOYMENT TYPE & WORK MODE
# ─────────────────────────────────────────────

def infer_employment_type(
    description: str, page_metadata: Dict[str, str]
) -> str:
    """Infer employment type from description and page metadata."""
    # Check page metadata first (more reliable)
    emp_type_raw = page_metadata.get("employment_type", "")
    for keyword, emp_type in EMPLOYMENT_TYPE_SIGNALS.items():
        if keyword in emp_type_raw.lower():
            return emp_type.value

    # Fall back to description
    desc_lower = description.lower() if description else ""
    for keyword, emp_type in EMPLOYMENT_TYPE_SIGNALS.items():
        if keyword in desc_lower:
            return emp_type.value

    return EmploymentType.UNKNOWN.value


def infer_work_mode(
    description: str, raw_location: str
) -> str:
    """Infer work mode from description and location."""
    combined = f"{description} {raw_location}".lower()
    for keyword, mode in WORK_MODE_SIGNALS.items():
        if keyword in combined:
            return mode.value
    return WorkMode.UNKNOWN.value


# ─────────────────────────────────────────────
# DATE NORMALIZATION
# ─────────────────────────────────────────────

_RELATIVE_DATE_RE = re.compile(
    r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago",
    re.IGNORECASE,
)

_UNIT_TO_DAYS = {
    "second": 0,
    "minute": 0,
    "hour": 0,
    "day": 1,
    "week": 7,
    "month": 30,
    "year": 365,
}


def normalize_date(
    raw_date: str, reference_date: Optional[datetime] = None
) -> Optional[str]:
    """
    Convert a relative date string to ISO-8601 format.
    
    Examples:
        "3 days ago" → "2026-02-07" (relative to reference_date)
        "N/A" → None
    """
    if not raw_date or raw_date.strip().upper() == "N/A":
        return None

    if reference_date is None:
        reference_date = datetime.now()

    match = _RELATIVE_DATE_RE.search(raw_date)
    if match:
        amount = int(match.group(1))
        unit = match.group(2).lower()
        days = _UNIT_TO_DAYS.get(unit, 0) * amount
        result_date = reference_date - timedelta(days=days)
        return result_date.strftime("%Y-%m-%d")

    # Try direct ISO parse
    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw_date.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


# ─────────────────────────────────────────────
# MASTER PARSER
# ─────────────────────────────────────────────

def parse_job(
    raw_title: str,
    company: str,
    raw_location: str,
    date_posted_raw: str,
    job_url: str,
    job_description_raw: str,
    page_metadata: Optional[Dict[str, str]] = None,
    scrape_timestamp: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Parse a single job record into the full output schema.
    
    ⚠️  DEPRECATION: This function is no longer used by the core scraper pipeline.
    The scraper now focuses on raw data collection. For parsing, use a separate
    preprocessing pipeline. This remains for backward compatibility.
    
    Returns None if the record fails quality checks (no description, no skills).
    """
    warnings.warn(
        "parse_job() is deprecated and no longer used by the scraper pipeline. "
        "Use a separate preprocessing pipeline for data normalization.",
        DeprecationWarning,
        stacklevel=2
    )
    
    from src.preprocessing.vocabularies import SCHEMA_VERSION, DATASET_VERSION

    if page_metadata is None:
        page_metadata = {}

    if scrape_timestamp is None:
        scrape_timestamp = datetime.now().isoformat()

    # ── Quality gate 1: description must exist ──
    if not job_description_raw or len(job_description_raw.strip()) < 50:
        logger.warning(f"DISCARDED (no description): {job_url}")
        return None

    # ── Core fields ──
    job_id = generate_job_id(job_url)
    normalized_title = normalize_title(raw_title)
    title_keywords = extract_title_keywords(raw_title)

    # ── Skills ──
    skills_required, skills_optional, tools_frameworks = extract_skills(
        job_description_raw
    )

    # ── Quality gate 2: must have at least one skill ──
    if not skills_required and not skills_optional:
        logger.warning(f"DISCARDED (no skills found): {job_url}")
        return None

    # ── Experience ──
    min_years, experience_text = extract_experience(job_description_raw)

    # ── Seniority ──
    seniority = infer_seniority(raw_title, job_description_raw, min_years)

    # ── Role type ──
    role_type = infer_role_type(raw_title, job_description_raw, tools_frameworks)

    # ── Employment type & work mode ──
    employment_type = infer_employment_type(job_description_raw, page_metadata)
    work_mode = infer_work_mode(job_description_raw, raw_location)

    # ── Location ──
    location = parse_location(raw_location)

    # ── Date ──
    date_posted_normalized = normalize_date(date_posted_raw)

    return {
        "job_id": job_id,
        "company": company if company != "N/A" else None,
        "raw_title": raw_title,
        "normalized_title": normalized_title,
        "title_keywords": title_keywords,
        "role_type": role_type,
        "seniority_level": seniority,
        "skills_required": skills_required,
        "skills_optional": skills_optional,
        "tools_frameworks": tools_frameworks,
        "min_years_experience": min_years,
        "experience_text": experience_text,
        "employment_type": employment_type,
        "work_mode": work_mode,
        "city": location["city"],
        "region": location["region"],
        "country": location["country"],
        "date_posted_raw": date_posted_raw if date_posted_raw != "N/A" else None,
        "date_posted_normalized": date_posted_normalized,
        "job_description_raw": job_description_raw,
        "source": "linkedin",
        "job_url": job_url,
        "scrape_timestamp": scrape_timestamp,
        "schema_version": SCHEMA_VERSION,
        "dataset_version": DATASET_VERSION,
    }
