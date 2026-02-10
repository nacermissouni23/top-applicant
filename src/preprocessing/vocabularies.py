"""
Controlled vocabularies, synonym maps, and enum definitions for job parsing.

This module is the single source of truth for all normalization rules.
All downstream parsers import from here — never hardcode categories elsewhere.
"""

from enum import Enum
from typing import Dict, List, Set

# ─────────────────────────────────────────────
# ENUMS
# ─────────────────────────────────────────────

class RoleType(str, Enum):
    APPLIED_DS = "Applied Data Scientist"
    ANALYTICS_DS = "Analytics-focused Data Scientist"
    ML_ENGINEER = "Machine Learning Engineer"
    RESEARCH_SCIENTIST = "Research Scientist"
    GENAI_LLM = "GenAI / LLM Engineer"
    UNKNOWN = "Unknown"


class SeniorityLevel(str, Enum):
    JUNIOR = "Junior"
    MID = "Mid"
    SENIOR = "Senior"
    STAFF_LEAD = "Staff/Lead"
    UNKNOWN = "Unknown"


class EmploymentType(str, Enum):
    FULL_TIME = "Full-time"
    PART_TIME = "Part-time"
    CONTRACT = "Contract"
    INTERNSHIP = "Internship"
    TEMPORARY = "Temporary"
    VOLUNTEER = "Volunteer"
    UNKNOWN = "Unknown"


class WorkMode(str, Enum):
    REMOTE = "Remote"
    HYBRID = "Hybrid"
    ONSITE = "Onsite"
    UNKNOWN = "Unknown"


# ─────────────────────────────────────────────
# TITLE NORMALIZATION
# ─────────────────────────────────────────────

# Map noisy title fragments → canonical title.
# Order matters: first match wins. Patterns are checked via substring (lowered).
TITLE_NORMALIZATION_RULES: List[tuple] = [
    # Must come before generic "data scientist"
    ("machine learning engineer", "Machine Learning Engineer"),
    ("ml engineer", "Machine Learning Engineer"),
    ("mle", "Machine Learning Engineer"),
    ("applied scientist", "Applied Scientist"),
    ("research scientist", "Research Scientist"),
    ("research engineer", "Research Engineer"),
    ("genai engineer", "GenAI / LLM Engineer"),
    ("llm engineer", "GenAI / LLM Engineer"),
    ("gen ai engineer", "GenAI / LLM Engineer"),
    ("ai engineer", "AI Engineer"),
    ("data analyst", "Data Analyst"),
    ("data engineer", "Data Engineer"),
    ("analytics engineer", "Analytics Engineer"),
    ("data scientist", "Data Scientist"),
    ("statistician", "Statistician"),
    ("quantitative analyst", "Quantitative Analyst"),
    ("quant researcher", "Quantitative Researcher"),
]

# Keywords to extract from raw titles
TITLE_KEYWORD_PATTERNS: List[str] = [
    "ML", "AI", "NLP", "LLM", "GenAI", "Gen AI", "Generative AI",
    "Computer Vision", "CV", "Deep Learning", "Reinforcement Learning",
    "Fraud", "Risk", "Ads", "Search", "Ranking", "Recommendation",
    "Supply Chain", "Healthcare", "Fintech", "E-commerce",
    "Production", "Platform", "Infrastructure", "Applied",
    "Analytics", "Growth", "Marketing", "Product",
    "L3", "L4", "L5", "L6", "L7",
    "Senior", "Staff", "Lead", "Principal", "Junior", "Intern",
]

# ─────────────────────────────────────────────
# SKILLS & TOOLS SYNONYM MAP
# ─────────────────────────────────────────────

# Maps raw text variants → canonical name
SKILL_SYNONYMS: Dict[str, str] = {
    # Languages
    "python": "Python",
    "python3": "Python",
    "py": "Python",
    "r": "R",
    "r language": "R",
    "sql": "SQL",
    "scala": "Scala",
    "java": "Java",
    "javascript": "JavaScript",
    "js": "JavaScript",
    "typescript": "TypeScript",
    "ts": "TypeScript",
    "c++": "C++",
    "cpp": "C++",
    "c#": "C#",
    "golang": "Go",
    "go": "Go",
    "julia": "Julia",
    "rust": "Rust",
    "bash": "Bash",
    "shell": "Shell",
    "sas": "SAS",
    "matlab": "MATLAB",
    "stata": "Stata",

    # ML / DL frameworks
    "tensorflow": "TensorFlow",
    "tf": "TensorFlow",
    "pytorch": "PyTorch",
    "torch": "PyTorch",
    "keras": "Keras",
    "scikit-learn": "scikit-learn",
    "sklearn": "scikit-learn",
    "xgboost": "XGBoost",
    "lightgbm": "LightGBM",
    "catboost": "CatBoost",
    "jax": "JAX",
    "hugging face": "Hugging Face",
    "huggingface": "Hugging Face",
    "transformers": "Hugging Face Transformers",
    "langchain": "LangChain",
    "llamaindex": "LlamaIndex",
    "llama index": "LlamaIndex",
    "openai": "OpenAI API",
    "gpt": "GPT",
    "chatgpt": "ChatGPT",

    # Data / cloud tools
    "spark": "Apache Spark",
    "pyspark": "PySpark",
    "apache spark": "Apache Spark",
    "hadoop": "Hadoop",
    "hive": "Hive",
    "kafka": "Kafka",
    "airflow": "Apache Airflow",
    "apache airflow": "Apache Airflow",
    "dbt": "dbt",
    "snowflake": "Snowflake",
    "bigquery": "BigQuery",
    "redshift": "Redshift",
    "databricks": "Databricks",
    "aws": "AWS",
    "amazon web services": "AWS",
    "gcp": "GCP",
    "google cloud": "GCP",
    "azure": "Azure",
    "microsoft azure": "Azure",
    "sagemaker": "AWS SageMaker",
    "vertex ai": "Vertex AI",

    # MLOps / DevOps
    "docker": "Docker",
    "kubernetes": "Kubernetes",
    "k8s": "Kubernetes",
    "mlflow": "MLflow",
    "kubeflow": "Kubeflow",
    "wandb": "Weights & Biases",
    "weights and biases": "Weights & Biases",
    "neptune": "Neptune.ai",
    "ci/cd": "CI/CD",
    "cicd": "CI/CD",
    "git": "Git",
    "github": "GitHub",
    "gitlab": "GitLab",
    "terraform": "Terraform",

    # Visualization
    "tableau": "Tableau",
    "power bi": "Power BI",
    "powerbi": "Power BI",
    "looker": "Looker",
    "matplotlib": "Matplotlib",
    "seaborn": "Seaborn",
    "plotly": "Plotly",
    "d3": "D3.js",

    # Databases
    "postgresql": "PostgreSQL",
    "postgres": "PostgreSQL",
    "mysql": "MySQL",
    "mongodb": "MongoDB",
    "mongo": "MongoDB",
    "redis": "Redis",
    "elasticsearch": "Elasticsearch",
    "neo4j": "Neo4j",
    "cassandra": "Cassandra",
    "dynamodb": "DynamoDB",

    # Libraries & methods
    "pandas": "Pandas",
    "numpy": "NumPy",
    "scipy": "SciPy",
    "nltk": "NLTK",
    "spacy": "spaCy",
    "opencv": "OpenCV",
    "statsmodels": "Statsmodels",

    # Concepts (treated as skills)
    "machine learning": "Machine Learning",
    "ml": "Machine Learning",
    "deep learning": "Deep Learning",
    "dl": "Deep Learning",
    "natural language processing": "NLP",
    "nlp": "NLP",
    "computer vision": "Computer Vision",
    "reinforcement learning": "Reinforcement Learning",
    "rl": "Reinforcement Learning",
    "generative ai": "Generative AI",
    "genai": "Generative AI",
    "gen ai": "Generative AI",
    "llm": "Large Language Models",
    "large language model": "Large Language Models",
    "large language models": "Large Language Models",
    "rag": "RAG",
    "retrieval augmented generation": "RAG",
    "time series": "Time Series",
    "forecasting": "Forecasting",
    "a/b testing": "A/B Testing",
    "ab testing": "A/B Testing",
    "bayesian": "Bayesian Methods",
    "causal inference": "Causal Inference",
    "recommendation systems": "Recommendation Systems",
    "recommender systems": "Recommendation Systems",
    "feature engineering": "Feature Engineering",
    "data pipelines": "Data Pipelines",
    "etl": "ETL",
    "elt": "ELT",
}

# These are tools/frameworks (subset of skills) — used to populate tools_frameworks
TOOLS_AND_FRAMEWORKS: Set[str] = {
    "Python", "R", "SQL", "Scala", "Java", "JavaScript", "TypeScript",
    "C++", "C#", "Go", "Julia", "Rust", "Bash", "Shell", "SAS", "MATLAB",
    "TensorFlow", "PyTorch", "Keras", "scikit-learn", "XGBoost", "LightGBM",
    "CatBoost", "JAX", "Hugging Face", "Hugging Face Transformers",
    "LangChain", "LlamaIndex", "OpenAI API",
    "Apache Spark", "PySpark", "Hadoop", "Hive", "Kafka",
    "Apache Airflow", "dbt", "Snowflake", "BigQuery", "Redshift",
    "Databricks", "AWS", "GCP", "Azure", "AWS SageMaker", "Vertex AI",
    "Docker", "Kubernetes", "MLflow", "Kubeflow", "Weights & Biases",
    "Neptune.ai", "CI/CD", "Git", "GitHub", "GitLab", "Terraform",
    "Tableau", "Power BI", "Looker", "Matplotlib", "Seaborn", "Plotly",
    "D3.js", "PostgreSQL", "MySQL", "MongoDB", "Redis", "Elasticsearch",
    "Neo4j", "Cassandra", "DynamoDB", "Pandas", "NumPy", "SciPy",
    "NLTK", "spaCy", "OpenCV", "Statsmodels",
}


# ─────────────────────────────────────────────
# ROLE TYPE INFERENCE KEYWORDS
# ─────────────────────────────────────────────

# Keywords/phrases that signal a particular role type (checked in description)
ROLE_TYPE_SIGNALS: Dict[str, List[str]] = {
    RoleType.GENAI_LLM: [
        "llm", "large language model", "generative ai", "genai", "gen ai",
        "prompt engineering", "rag", "retrieval augmented", "langchain",
        "fine-tuning", "fine tuning", "rlhf", "instruction tuning",
        "chatbot", "conversational ai", "foundation model",
    ],
    RoleType.RESEARCH_SCIENTIST: [
        "publish", "publications", "research paper", "arxiv", "neurips",
        "icml", "iclr", "cvpr", "acl", "aaai", "novel algorithm",
        "state-of-the-art", "state of the art", "theoretical",
        "advance the field", "phd required", "ph.d. required",
    ],
    RoleType.ML_ENGINEER: [
        "deploy model", "model deployment", "production model",
        "model serving", "mlops", "ml infrastructure", "ml platform",
        "feature store", "model monitoring", "model pipeline",
        "real-time inference", "batch inference", "microservice",
        "scaling", "latency", "throughput",
    ],
    RoleType.ANALYTICS_DS: [
        "stakeholder", "business insight", "dashboard", "kpi",
        "reporting", "product analytics", "growth analytics",
        "experimentation", "a/b test", "ab test", "metric",
        "sql-heavy", "data-driven decision", "business intelligence",
    ],
    RoleType.APPLIED_DS: [
        "build model", "building model", "predictive model",
        "classification", "regression", "clustering",
        "feature engineering", "model training", "end-to-end",
        "solve business", "applied machine learning",
    ],
}


# ─────────────────────────────────────────────
# SENIORITY INFERENCE
# ─────────────────────────────────────────────

# Title-level signals
SENIORITY_TITLE_SIGNALS: Dict[str, SeniorityLevel] = {
    "intern": SeniorityLevel.JUNIOR,
    "junior": SeniorityLevel.JUNIOR,
    "entry level": SeniorityLevel.JUNIOR,
    "entry-level": SeniorityLevel.JUNIOR,
    "associate": SeniorityLevel.JUNIOR,
    "l3": SeniorityLevel.MID,
    "mid-level": SeniorityLevel.MID,
    "mid level": SeniorityLevel.MID,
    "l4": SeniorityLevel.MID,
    "senior": SeniorityLevel.SENIOR,
    "sr.": SeniorityLevel.SENIOR,
    "sr ": SeniorityLevel.SENIOR,
    "l5": SeniorityLevel.SENIOR,
    "staff": SeniorityLevel.STAFF_LEAD,
    "lead": SeniorityLevel.STAFF_LEAD,
    "principal": SeniorityLevel.STAFF_LEAD,
    "director": SeniorityLevel.STAFF_LEAD,
    "head of": SeniorityLevel.STAFF_LEAD,
    "l6": SeniorityLevel.STAFF_LEAD,
    "l7": SeniorityLevel.STAFF_LEAD,
}

# Responsibility verbs that imply seniority (found in description)
SENIORITY_VERB_SIGNALS: Dict[str, SeniorityLevel] = {
    "mentor": SeniorityLevel.SENIOR,
    "lead a team": SeniorityLevel.STAFF_LEAD,
    "manage a team": SeniorityLevel.STAFF_LEAD,
    "own the roadmap": SeniorityLevel.STAFF_LEAD,
    "define strategy": SeniorityLevel.STAFF_LEAD,
    "set technical direction": SeniorityLevel.STAFF_LEAD,
    "architect": SeniorityLevel.SENIOR,
    "design system": SeniorityLevel.SENIOR,
    "cross-functional": SeniorityLevel.SENIOR,
    "influence": SeniorityLevel.SENIOR,
    "drive": SeniorityLevel.SENIOR,
}

# Experience-year thresholds for seniority inference
EXPERIENCE_SENIORITY_MAP = [
    (0, 2, SeniorityLevel.JUNIOR),
    (2, 5, SeniorityLevel.MID),
    (5, 8, SeniorityLevel.SENIOR),
    (8, 100, SeniorityLevel.STAFF_LEAD),
]


# ─────────────────────────────────────────────
# EMPLOYMENT TYPE KEYWORDS
# ─────────────────────────────────────────────

EMPLOYMENT_TYPE_SIGNALS: Dict[str, EmploymentType] = {
    "full-time": EmploymentType.FULL_TIME,
    "full time": EmploymentType.FULL_TIME,
    "part-time": EmploymentType.PART_TIME,
    "part time": EmploymentType.PART_TIME,
    "contract": EmploymentType.CONTRACT,
    "contractor": EmploymentType.CONTRACT,
    "freelance": EmploymentType.CONTRACT,
    "internship": EmploymentType.INTERNSHIP,
    "intern": EmploymentType.INTERNSHIP,
    "temporary": EmploymentType.TEMPORARY,
    "temp": EmploymentType.TEMPORARY,
    "volunteer": EmploymentType.VOLUNTEER,
}


# ─────────────────────────────────────────────
# WORK MODE KEYWORDS
# ─────────────────────────────────────────────

WORK_MODE_SIGNALS: Dict[str, WorkMode] = {
    "remote": WorkMode.REMOTE,
    "fully remote": WorkMode.REMOTE,
    "work from home": WorkMode.REMOTE,
    "wfh": WorkMode.REMOTE,
    "hybrid": WorkMode.HYBRID,
    "on-site": WorkMode.ONSITE,
    "onsite": WorkMode.ONSITE,
    "on site": WorkMode.ONSITE,
    "in-office": WorkMode.ONSITE,
    "in office": WorkMode.ONSITE,
}


# ─────────────────────────────────────────────
# DATASET VERSIONING
# ─────────────────────────────────────────────

SCHEMA_VERSION = "0.2"
DATASET_VERSION = "v0.2"

SCHEMA_FIELDS = [
    "job_id", "company",
    "raw_title", "normalized_title", "title_keywords",
    "role_type", "seniority_level",
    "skills_required", "skills_optional", "tools_frameworks",
    "min_years_experience", "experience_text",
    "employment_type", "work_mode",
    "city", "region", "country",
    "date_posted_raw", "date_posted_normalized",
    "job_description_raw",
    "source", "job_url", "scrape_timestamp",
    "schema_version", "dataset_version",
]
