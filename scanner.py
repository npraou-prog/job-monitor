#!/usr/bin/env python3
"""
Job Monitor Scanner - Multi-company support
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
    match = re.search(r'/(\d+)', url)
    return match.group(1) if match else url


# =============================================================================
# DELOITTE FETCHER
# =============================================================================
def fetch_deloitte_jobs(base_url, db, company_id):
    """Fetch Deloitte jobs with pagination."""
    jobs = db["companies"].get(company_id, {})
    per_page = 10
    base = "https://apply.deloitte.com/en_US/careers/SearchJobs/"
    
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
                db["companies"][company_id] = jobs
                save_db(db)
            
            time.sleep(0.2)
            
        except Exception as e:
            print(f"  Error page {i}: {e}", flush=True)
            continue
    
    db["companies"][company_id] = jobs
    save_db(db)
    
    print(f"Done: {len(jobs)} total jobs", flush=True)
    return jobs, new_found


# =============================================================================
# CISCO FETCHER (Playwright - JS rendered)
# =============================================================================
def fetch_cisco_jobs(base_url, db, company_id):
    """Fetch Cisco jobs using Playwright (JS rendered page)."""
    from playwright.sync_api import sync_playwright
    
    jobs = db["companies"].get(company_id, {})
    per_page = 10
    
    print("Launching browser...", flush=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # Get total count - use proper URL with ?
        first_url = f"{base_url}?from=0"
        print(f"Loading {first_url}...", flush=True)
        page.goto(first_url, wait_until='networkidle', timeout=60000)
        page.wait_for_timeout(2000)  # Wait for dynamic content
        
        try:
            count_el = page.locator('text=/\\d+ results/i').first
            count_text = count_el.inner_text()
            match = re.search(r'(\d+)', count_text)
            total = int(match.group(1)) if match else 500
        except:
            total = 500
        
        pages = (total // per_page) + 1
        print(f"Total: {total} jobs, {pages} pages", flush=True)
        
        new_found = 0
        for i in range(pages):
            offset = i * per_page
            url = f"{base_url}?from={offset}"
            
            try:
                page.goto(url, wait_until='networkidle', timeout=30000)
                page.wait_for_timeout(1000)  # Wait for JS
                
                links = page.locator('a[href*="/job/"]').all()
                
                for link in links:
                    try:
                        href = link.get_attribute('href') or ''
                        title = link.inner_text().strip()
                        
                        if '/job/' in href and title:
                            if not href.startswith('http'):
                                href = f"https://careers.cisco.com{href}"
                            
                            job_id = extract_job_id(href)
                            
                            if job_id not in jobs:
                                jobs[job_id] = {
                                    "id": job_id,
                                    "title": title,
                                    "url": href,
                                    "firstSeen": datetime.now().isoformat()
                                }
                                new_found += 1
                    except:
                        continue
                
                if (i + 1) % 10 == 0:
                    print(f"  Page {i+1}/{pages}: {len(jobs)} total, {new_found} new", flush=True)
                    db["companies"][company_id] = jobs
                    save_db(db)
                
            except Exception as e:
                print(f"  Error page {i}: {e}", flush=True)
                continue
        
        browser.close()
    
    db["companies"][company_id] = jobs
    save_db(db)
    
    print(f"Done: {len(jobs)} total jobs", flush=True)
    return jobs, new_found


# =============================================================================
# VISA FETCHER (Playwright - JS rendered, single page with all results)
# =============================================================================
def fetch_visa_jobs(base_url, db, company_id):
    """Fetch Visa jobs using Playwright."""
    from playwright.sync_api import sync_playwright
    
    jobs = db["companies"].get(company_id, {})
    
    print("Launching browser...", flush=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print(f"Loading {base_url[:60]}...", flush=True)
        page.goto(base_url, wait_until='networkidle', timeout=60000)
        page.wait_for_timeout(3000)  # Wait for dynamic content
        
        # Get total count
        try:
            count_el = page.locator('text=/\\d+ results/i').first
            count_text = count_el.inner_text()
            match = re.search(r'(\d+)', count_text)
            total = int(match.group(1)) if match else 0
            print(f"Total: {total} jobs", flush=True)
        except:
            total = 0
            print("Could not determine job count", flush=True)
        
        # Find all job links with REF IDs
        links = page.locator('a').all()
        new_found = 0
        
        for link in links:
            try:
                href = link.get_attribute('href') or ''
                title = link.inner_text().strip()
                
                # Check if it's a job link (contains REF ID pattern)
                if 'REF' in href and '/jobs/' in href:
                    # Extract job ID (REF followed by alphanumeric)
                    job_id_match = re.search(r'(REF\w+)', href)
                    if job_id_match:
                        job_id = job_id_match.group(1)
                        
                        if job_id not in jobs:
                            full_url = href if href.startswith('http') else f"https://corporate.visa.com{href}"
                            jobs[job_id] = {
                                "id": job_id,
                                "title": title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat()
                            }
                            new_found += 1
            except:
                continue
        
        browser.close()
    
    db["companies"][company_id] = jobs
    save_db(db)
    
    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# GLOBAL PARTNERS FETCHER (Playwright - JS rendered, single page)
# =============================================================================
def fetch_globalpartners_jobs(base_url, db, company_id):
    """Fetch Global Partners jobs using Playwright."""
    from playwright.sync_api import sync_playwright
    
    jobs = db["companies"].get(company_id, {})
    
    print("Launching browser...", flush=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print(f"Loading {base_url}...", flush=True)
        page.goto(base_url, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(5000)  # Wait for JS to render jobs
        
        new_found = 0
        
        # Find all job links - pattern: /job-slug/job/P1-XXXXXX-X
        links = page.locator('a[href*="/job/"]').all()
        
        for link in links:
            try:
                href = link.get_attribute('href') or ''
                
                # Extract job ID (P1-XXXXXX-X pattern)
                job_id_match = re.search(r'(P1-\d+-\d+)', href)
                if job_id_match:
                    job_id = job_id_match.group(1)
                    
                    # Extract title from URL slug (e.g., /data-science-intern/job/...)
                    title_match = re.search(r'/([^/]+)/job/', href)
                    title = title_match.group(1).replace('-', ' ').title() if title_match else "Unknown"
                    
                    if job_id not in jobs:
                        full_url = f"https://careers.globalp.com{href}" if not href.startswith('http') else href
                        jobs[job_id] = {
                            "id": job_id,
                            "title": title,
                            "url": full_url,
                            "firstSeen": datetime.now().isoformat()
                        }
                        new_found += 1
            except:
                continue
        
        browser.close()
    
    db["companies"][company_id] = jobs
    save_db(db)
    
    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# FIDELITY FETCHER (Playwright - paginated)
# =============================================================================
def fetch_fidelity_jobs(base_url, db, company_id):
    """Fetch Fidelity jobs using Playwright with pagination."""
    from playwright.sync_api import sync_playwright
    
    jobs = db["companies"].get(company_id, {})
    
    print("Launching browser...", flush=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        print(f"Loading {base_url}...", flush=True)
        page.goto(base_url, wait_until='networkidle', timeout=90000)
        page.wait_for_timeout(5000)
        
        # Get total count from "X open roles" text
        try:
            count_text = page.locator('text=/\\d+ open roles/i').first.inner_text()
            match = re.search(r'(\d+)', count_text)
            total = int(match.group(1)) if match else 500
        except:
            total = 500
        
        per_page = 20
        pages = (total // per_page) + 1
        print(f"Total: {total} jobs, {pages} pages", flush=True)
        
        new_found = 0
        
        for i in range(1, pages + 1):
            url = f"{base_url}?page={i}" if i > 1 else base_url
            
            try:
                if i > 1:
                    page.goto(url, wait_until='networkidle', timeout=60000)
                    page.wait_for_timeout(3000)
                
                # Find job links - pattern: /en/jobs/{id}/{slug}/
                links = page.locator('a[href*="/en/jobs/"]').all()
                
                for link in links:
                    try:
                        href = link.get_attribute('href') or ''
                        
                        # Match job detail pages: /en/jobs/1234567/job-title/
                        job_match = re.search(r'/en/jobs/(\d+)/([^/]+)/', href)
                        if job_match:
                            job_id = job_match.group(1)
                            title_slug = job_match.group(2)
                            title = title_slug.replace('-', ' ').title()
                            
                            if job_id not in jobs:
                                full_url = f"https://jobs.fidelity.com{href}" if not href.startswith('http') else href
                                jobs[job_id] = {
                                    "id": job_id,
                                    "title": title,
                                    "url": full_url,
                                    "firstSeen": datetime.now().isoformat()
                                }
                                new_found += 1
                    except:
                        continue
                
                if i % 5 == 0:
                    print(f"  Page {i}/{pages}: {len(jobs)} total, {new_found} new", flush=True)
                    db["companies"][company_id] = jobs
                    save_db(db)
                    
            except Exception as e:
                print(f"  Error page {i}: {e}", flush=True)
                continue
        
        browser.close()
    
    db["companies"][company_id] = jobs
    save_db(db)
    
    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# COMPANY ROUTER
# =============================================================================
FETCHERS = {
    "deloitte": fetch_deloitte_jobs,
    "cisco": fetch_cisco_jobs,
    "visa": fetch_visa_jobs,
    "globalpartners": fetch_globalpartners_jobs,
    "fidelity": fetch_fidelity_jobs,
}


def fetch_jobs(company, db):
    """Route to appropriate fetcher based on company ID."""
    company_id = company["id"]
    fetcher = FETCHERS.get(company_id)
    
    if fetcher:
        return fetcher(company["url"], db, company_id)
    else:
        print(f"  ⚠️ No fetcher for {company_id}, skipping.", flush=True)
        return {}, 0


# =============================================================================
# MAIN
# =============================================================================
def main(target_company=None):
    """
    Run scanner. If target_company specified, only scan that company.
    Usage: python scanner.py [company_name]
    """
    config = load_config()
    db = load_db()
    
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")
    
    # Filter companies if target specified
    companies_to_scan = config["companies"]
    if target_company:
        target_lower = target_company.lower()
        companies_to_scan = [
            c for c in config["companies"] 
            if c["id"].lower() == target_lower or c["name"].lower() == target_lower
        ]
        if not companies_to_scan:
            print(f"❌ Company '{target_company}' not found.", flush=True)
            print(f"Available: {', '.join(c['name'] for c in config['companies'])}", flush=True)
            return
    
    for company in companies_to_scan:
        print(f"\n{'='*50}", flush=True)
        print(f"Scanning: {company['name']}", flush=True)
        print(f"Referrers: {', '.join(company['referrers'])}", flush=True)
        print(f"{'='*50}", flush=True)
        
        company_id = company["id"]
        stored_count = len(db["companies"].get(company_id, {}))
        is_baseline = stored_count == 0
        
        jobs, new_count = fetch_jobs(company, db)
        
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
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else None
    main(target)
