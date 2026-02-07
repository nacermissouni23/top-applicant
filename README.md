# Top Applicant: Global Job Description Analysis

## Project Goal
The primary objective of this project is to analyze real-world job descriptions to extract underlying patterns, identify high-demand skills, and define emerging role archetypes through data mining and machine learning techniques.

## Motivation
As the global labor market becomes increasingly dynamic, understanding the specific requirements and evolving expectations of employers is crucial for job seekers, educational institutions, and recruiters. This research seeks to bridge the gap between candidate qualifications and industry needs by providing data-driven insights into the current employment landscape.

## Data Mining Pipeline
1. **Data Collection**: Scraping job descriptions from various global platforms.
2. **Data Cleaning**: Standardizing formats and handling missing or noisy information.
3. **Text Preprocessing**: Tokenization, lemmatization, and removal of stop words to prepare text for analysis.
4. **Feature Engineering**: Vectorization and extraction of specific key phrases, skills, and requirements.
5. **Analysis & Modeling**: Applying clustering and classification algorithms to identify role archetypes and skill trends.

## Repository Structure Overview
- `data/`: Contains raw, interim, and processed datasets.
- `notebooks/`: Modular Jupyter notebooks for step-by-step experimentation.
- `src/`: Reusable Python modules for scraping, preprocessing, and feature engineering.
- `outputs/`: Generated figures and tables for reporting.
- `docs/`: Technical notes and research documentation.

## Environment Setup
Follow these steps to set up a clean, reproducible environment for this project.

### 1. Create Virtual Environment
Ensure you are in the project root directory (`top-applicant/`):
```bash
python -m venv venv
```

### 2. Activate Environment
**Windows (PowerShell):**
```powershell
.\venv\Scripts\Activate.ps1
```
**macOS / Linux:**
```bash
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```
