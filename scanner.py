#!/usr/bin/env python3
"""
Job Monitor Scanner - Memory efficient, incremental saves
"""

import json
import re
import time
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config.json"
DB_FILE = BASE_DIR / "jobs-db.json"
LOG_FILE = BASE_DIR / "run-log.md"

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
})


def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)


def load_db():
    with open(DB_FILE) as f:
        return json.load(f)


def save_db(db):
    db["lastUpdated"] = datetime.now().isoformat()
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=2)


def append_log(date, time_str, company, status, new_jobs, relevant, notes):
    with open(LOG_FILE, "a") as f:
        f.write(f"| {date} | {time_str} | {company} | {status} | {new_jobs} | {relevant} | {notes} |\n")


def is_relevant_role(title, target_roles):
    title_lower = title.lower()
    return any(role.lower() in title_lower for role in target_roles)


def extract_job_id(url):
    match = re.search(r'/(\d+)$', url)
    return match.group(1) if match else url


def fetch_deloitte_jobs(base_url, db, company_id):
    """Fetch jobs with incremental saves."""
    jobs = db["companies"].get(company_id, {})
    per_page = 10
    base = "https://apply.deloitte.com/en_US/careers/SearchJobs/"
    
    # Get total
    print("Getting job count...", flush=True)
    resp = SESSION.get(base_url, timeout=30)
    match = re.search(r'(\d+)\s*jobs', resp.text)
    total = int(match.group(1)) if match else 750
    pages = (total // per_page) + 1
    print(f"Total: {total} jobs, {pages} pages", flush=True)
    
    new_found = 0
    for i in range(pages):
        offset = i * per_page
        url = f"{base}?jobRecordsPerPage={per_page}&jobOffset={offset}"
        
        try:
            resp = SESSION.get(url, timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            for link in soup.select('a[href*="/careers/JobDetail/"]'):
                href = link.get('href', '')
                title = link.get_text(strip=True)
                
                if '/careers/JobDetail/' in href and title:
                    if not href.startswith('http'):
                        href = f"https://apply.deloitte.com{href}"
                    
                    job_id = extract_job_id(href)
                    
                    if job_id not in jobs:
                        jobs[job_id] = {
                            "id": job_id,
                            "title": title,
                            "url": href,
                            "firstSeen": datetime.now().isoformat()
                        }
                        new_found += 1
            
            if (i + 1) % 10 == 0:
                print(f"  Page {i+1}/{pages}: {len(jobs)} total, {new_found} new", flush=True)
                # Save progress
                db["companies"][company_id] = jobs
                save_db(db)
            
            time.sleep(0.2)  # Be polite
            
        except Exception as e:
            print(f"  Error page {i}: {e}", flush=True)
            continue
    
    # Final save
    db["companies"][company_id] = jobs
    save_db(db)
    
    print(f"Done: {len(jobs)} total jobs", flush=True)
    return jobs, new_found


def main():
    config = load_config()
    db = load_db()
    
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")
    
    for company in config["companies"]:
        print(f"\n{'='*50}", flush=True)
        print(f"Scanning: {company['name']}", flush=True)
        print(f"Referrers: {', '.join(company['referrers'])}", flush=True)
        print(f"{'='*50}", flush=True)
        
        company_id = company["id"]
        stored_count = len(db["companies"].get(company_id, {}))
        is_baseline = stored_count == 0
        
        jobs, new_count = fetch_deloitte_jobs(company["url"], db, company_id)
        
        # Find relevant new jobs
        target_roles = config["targetRoles"]
        relevant = [j for j in jobs.values() 
                   if is_relevant_role(j["title"], target_roles) 
                   and j.get("firstSeen", "").startswith(now.strftime("%Y-%m-%d"))]
        
        # Log
        if is_baseline:
            status = "BASELINE"
            notes = f"Initial scan, {len(jobs)} jobs indexed"
        elif relevant:
            status = "🎯 MATCH"
            notes = f"{len(relevant)} relevant found!"
        elif new_count > 0:
            status = "NEW"
            notes = f"{new_count} new (not matching)"
        else:
            status = "No change"
            notes = "-"
        
        append_log(date_str, time_str, company["name"], status, new_count, len(relevant), notes)
        
        # Summary
        print(f"\n{'='*50}", flush=True)
        print("SUMMARY", flush=True)
        print(f"{'='*50}", flush=True)
        print(f"\n{company['name']}:", flush=True)
        print(f"  Total jobs: {len(jobs)}", flush=True)
        print(f"  New jobs: {new_count}", flush=True)
        print(f"  Relevant: {len(relevant)}", flush=True)
        
        if relevant:
            print(f"\n  🎯 RELEVANT JOBS (contact {', '.join(company['referrers'])}):", flush=True)
            for job in relevant:
                print(f"    • {job['title']}", flush=True)
                print(f"      {job['url']}", flush=True)


if __name__ == "__main__":
    main()
