"""
DEPRECATED: This module is no longer maintained.

Use src.scraping.linkedin_scraper.LinkedInScraper instead, which provides:
  - Exponential backoff on rate limiting
  - Full job description extraction
  - Company page extraction
  - Data parsing and schema normalization
  - Better error handling and logging

SimpleLinkedInScraper is kept here only for reference/historical purposes.
"""

import warnings
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import argparse

warnings.warn(
    "SimpleLinkedInScraper is deprecated. Use LinkedInScraper instead.",
    DeprecationWarning,
    stacklevel=2
)

class SimpleLinkedInScraper:
    def __init__(self):
        self.base_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def scrape(self, keywords, location, limit=10):
        print(f"Scraping {limit} jobs for '{keywords}' in '{location}'...")
        jobs = []
        start = 0
        
        while len(jobs) < limit:
            print(f"Fetching jobs starting at offset {start}...")
            params = {
                "keywords": keywords,
                "location": location,
                "start": start
            }
            
            try:
                response = requests.get(self.base_url, params=params, headers=self.headers)
                
                if response.status_code != 200:
                    print(f"Error: Received status code {response.status_code}")
                    break
                
                soup = BeautifulSoup(response.text, "html.parser")
                job_cards = soup.find_all("li")
                
                if not job_cards:
                    print("No more jobs found.")
                    break
                
                for card in job_cards:
                    if len(jobs) >= limit:
                        break
                        
                    try:
                        # Extract basic info from the card
                        title_tag = card.find("h3", class_="base-search-card__title")
                        company_tag = card.find("h4", class_="base-search-card__subtitle")
                        location_tag = card.find("span", class_="job-search-card__location")
                        link_tag = card.find("a", class_="base-card__full-link")
                        date_tag = card.find("time", class_="job-search-card__listdate")
                        
                        title = title_tag.get_text(strip=True) if title_tag else "N/A"
                        company = company_tag.get_text(strip=True) if company_tag else "N/A"
                        loc = location_tag.get_text(strip=True) if location_tag else "N/A"
                        link = link_tag["href"] if link_tag else "N/A"
                        date_posted = date_tag.get_text(strip=True) if date_tag else "N/A"
                        
                        # Clean up link (remove query params for cleaner ID)
                        if "?" in link:
                            link = link.split("?")[0]
                        
                        jobs.append({
                            "Title": title,
                            "Company": company,
                            "Location": loc,
                            "Date Posted": date_posted,
                            "Link": link
                        })
                        print(f"Found: {title} at {company}")
                        
                    except Exception as e:
                        print(f"Error parsing card: {e}")
                        continue
                
                start += 25 # LinkedIn loads 25 jobs at a time
                time.sleep(random.uniform(1, 3)) # Be polite
                
            except Exception as e:
                print(f"Request failed: {e}")
                break
                
        return pd.DataFrame(jobs)

    def save_results(self, df):
        if not df.empty:
            output_dir = "data/raw"
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, "linkedin_jobs.csv")
            df.to_csv(output_file, index=False)
            print(f"Saved {len(df)} jobs to {output_file}")
            print("Preview:")
            print(df.head())
        else:
            print("No data to save.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple LinkedIn Job Scraper")
    parser.add_argument("--keywords", default="Data Scientist", help="Job search keywords")
    parser.add_argument("--location", default="Remote", help="Job search location")
    parser.add_argument("--limit", type=int, default=10, help="Number of jobs to scrape")
    
    args = parser.parse_args()
    
    scraper = SimpleLinkedInScraper()
    df = scraper.scrape(args.keywords, args.location, args.limit)
    scraper.save_results(df)
