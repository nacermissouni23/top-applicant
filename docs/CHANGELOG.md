# Changelog

All notable changes to the Top Applicant project are documented here.

## [1.0.0] - 2025-02-16 — FINAL RAW DATA COLLECTOR FREEZE

### Status: STABLE & FROZEN
The scraper and raw data schema are now frozen at v1.0.0. All major features are complete and production-ready. No breaking changes to the raw schema will be introduced.

### Major Features (v1.0.0)
- **Raw-Only Architecture**: Pure data collection with zero parsing/normalization in the scraper
- **Immutable Schema**: v1.0.0 frozen schema for reproducibility and backward compatibility
- **Content Hashing**: SHA-256 hashing for all raw fields enabling deduplication and change detection
- **Nested Record Structure**: Organized schema with clear sections (metadata, identity, card, page, company, quality, hashing)
- **Exponential Backoff**: Smart rate limiting with 2^attempt formula and randomization
- **Interim Checkpointing**: Saves every 10 records to prevent data loss
- **Company Deduplication**: Smart caching (one fetch per company)
- **Quality Tracking**: Extraction quality ratings (high/medium/low) with field-level metrics
- **Session Pooling**: HTTP connection reuse for efficiency

### Architecture Improvements
- **Separated Concerns**: Parsing moved to separate preprocessing pipeline (no longer in scraper)
- **Refactored Extractors**: DescriptionExtractor and CompanyExtractor updated with proper session handling and retry logic
- **Improved Error Handling**: Explicit JSON serialization errors, no silent failures
- **Context Manager**: Proper resource cleanup with __enter__/__exit__ support
- **Better Logging**: Enhanced `_log_failure()` with detailed context metadata

### Raw Schema Structure
**Job Records** (per-job metadata):
- `scraper_version` (string): "1.0.0" (frozen)
- `raw_schema_version` (string): "1.0.0" (frozen)
- `scrape_metadata` (dict): Search context, timestamp, user agent
- `job_identity` (dict): Job URL, hashed job ID
- `job_card_raw` (dict): Title, company, location, posting date (from listing card)
- `job_page_raw` (dict): Full description (text+HTML), salary, applicants, criteria fields, embedded JSON
- `company_info` (dict): Company URL, hashed company ID reference
- `quality_tracking` (dict): Extraction quality, selector hits, retry count
- `hashing` (dict): Content hashes (description, job URL)

**Company Records** (per-company metadata):
- `scraper_version` (string): "1.0.0" (frozen)
- `raw_schema_version` (string): "1.0.0" (frozen)
- `company_identity` (dict): Company URL, hashed company ID
- `company_page_raw` (dict): About section (text+HTML), industry, size, HQ, type, specialties
- `hashing` (dict): Content hashes (about text, company URL)
- `timestamps` (dict): First seen, last seen
- `quality_tracking` (dict): Selector hits, retry count

### Folder Structure (v1.0.0)
```
data/raw/
├── jobs/              # Job archive (immutable)
├── companies/         # Company archive (immutable)
├── crawl_logs/        # Crawl execution logs
└── html_snapshots/    # Optional: Raw HTML snapshots
```

### Deprecations
- **job_parser.py**: Deprecated (parsing moved to separate pipeline)
- **location_parser.py**: Deprecated (location parsing moved to separate pipeline)
- **simple_linkedin_scraper.py**: Deprecated (use LinkedInScraper instead)
- **PARSE_DATA pipeline mode**: Removed (pipeline is now raw-only)

### Bug Fixes & Improvements
1. Session reuse in extractors (prevents memory leaks, enables connection pooling)
2. Company extraction with exponential backoff retry logic
3. Single interim checkpoint file (no disk bloat)
4. Simplified scrape flow (removed nested calls)
5. Proper CSV export (drops list columns with warning)
6. Extraction quality tracking (high/medium/low)
7. Selector failure detection (monitoring + alerts)
8. Explicit JSON serialization errors (no silent failures)
9. Context manager for session cleanup
10. Enhanced failure logging with job/company context
11. Rate limiting normalization across phases
12. Robust company URL normalization
13. Company URL deduplication and caching
14. Detailed extraction metadata preservation

### Notebook Updates
- **01_data_collection.ipynb**: Updated to v1.0.0 raw-only mode, removed PARSE_DATA configuration, removed analytics cells (for future preprocessing pipeline)
- Focuses on raw data collection, quality metrics, and next steps for preprocessing

### Freezing Policy (v1.0.0)
- Raw schema is immutable — no breaking changes
- Scraper version remains at 1.0.0 indefinitely (or marked as deprecated if major rewrite needed)
- All raw data collected with v1.0.0 is guaranteed compatible with v1.0.0 tools
- Any future improvements or changes will be released as v2.0+ (new scraper)

### Future Work
- Preprocessing pipeline v1.0 (separate from scraper): parsing, normalization, skill extraction
- Parquet format support for raw exports (future enhancement)
- HTML snapshot storage for forensics/auditing
- Advanced deduplication based on content similarity

---

## Versioning
This project follows semantic versioning (MAJOR.MINOR.PATCH).

**v1.0.0 is frozen and stable. Use this version for production data collection.**

For preprocessing and analysis, waiting for dedicated preprocessing_pipeline v1.0.
