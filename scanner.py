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


# Keywords that indicate citizenship/clearance requirements
RESTRICTION_KEYWORDS = {
    'citizen': [
        'us citizen', 'u.s. citizen', 'american citizen', 'citizenship required',
        'must be a citizen', 'citizens only', 'us citizenship', 'u.s. citizenship',
        'requires citizenship', 'citizen only'
    ],
    'clearance': [
        'security clearance', 'secret clearance', 'top secret', 'ts/sci',
        'clearance required', 'public trust', 'dod clearance', 'active clearance',
        'requires clearance', 'must have clearance', 'clearance eligible',
        'interim clearance', 'sci clearance', 'polygraph'
    ],
    'gc': [
        'green card', 'permanent resident', 'gc holder', 'gc required',
        'must be a permanent resident', 'lawful permanent'
    ],
}


def has_restrictions(job):
    return bool(job.get('restrictions'))


def restriction_label(job):
    r = job.get('restrictions', [])
    if not r:
        return ''
    labels = []
    if 'citizen' in r:
        labels.append('US Citizen Only')
    if 'clearance' in r:
        labels.append('Clearance Required')
    if 'gc' in r:
        labels.append('GC/PR Only')
    return ' | '.join(labels)


def detect_restrictions(text):
    """Detect citizenship/clearance requirements from text."""
    text_lower = text.lower()
    restrictions = []
    
    for restriction_type, keywords in RESTRICTION_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            restrictions.append(restriction_type)
    
    return restrictions


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
                        restrictions = detect_restrictions(title)
                        jobs[job_id] = {
                            "id": job_id,
                            "title": title,
                            "url": href,
                            "firstSeen": datetime.now().isoformat(),
                            "restrictions": restrictions
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
                                restrictions = detect_restrictions(title)
                                jobs[job_id] = {
                                    "id": job_id,
                                    "title": title,
                                    "url": href,
                                    "firstSeen": datetime.now().isoformat(),
                                    "restrictions": restrictions
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
                            restrictions = detect_restrictions(title)
                            jobs[job_id] = {
                                "id": job_id,
                                "title": title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat(),
                                "restrictions": restrictions
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
                        restrictions = detect_restrictions(title)
                        jobs[job_id] = {
                            "id": job_id,
                            "title": title,
                            "url": full_url,
                            "firstSeen": datetime.now().isoformat(),
                            "restrictions": restrictions
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
                                restrictions = detect_restrictions(title)
                                jobs[job_id] = {
                                    "id": job_id,
                                    "title": title,
                                    "url": full_url,
                                    "firstSeen": datetime.now().isoformat(),
                                    "restrictions": restrictions
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
# SYNOPSYS FETCHER (Static HTML with pagination)
# =============================================================================
def fetch_synopsys_jobs(base_url, db, company_id):
    """Fetch Synopsys jobs - static HTML, no JS needed."""
    jobs = db["companies"].get(company_id, {})
    per_page = 15
    
    print("Getting job count...", flush=True)
    resp = SESSION.get(base_url, timeout=30)
    match = re.search(r'(\d+)\s*results', resp.text)
    total = int(match.group(1)) if match else 500
    pages = (total // per_page) + 1
    print(f"Total: {total} jobs, {pages} pages", flush=True)
    
    new_found = 0
    for i in range(pages):
        url = f"{base_url}?pg={i+1}" if i > 0 else base_url
        
        try:
            resp = SESSION.get(url, timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            for link in soup.select('a[href*="/job/"]'):
                href = link.get('href', '')
                title = link.get_text(strip=True)
                
                if '/job/' in href and title:
                    # Extract job ID from URL pattern: /job/city/title/44408/JOB_ID
                    job_id_match = re.search(r'/(\d{8,})$', href)
                    if job_id_match:
                        job_id = job_id_match.group(1)
                        
                        if job_id not in jobs:
                            full_url = f"https://careers.synopsys.com{href}" if not href.startswith('http') else href
                            restrictions = detect_restrictions(title)
                            jobs[job_id] = {
                                "id": job_id,
                                "title": title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat(),
                                "restrictions": restrictions
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
    
    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# CHEWY FETCHER (Playwright - JS rendered)
# =============================================================================
def fetch_chewy_jobs(base_url, db, company_id):
    """Fetch Chewy jobs using Playwright."""
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
        
        new_found = 0
        page_num = 0
        max_pages = 50  # Safety limit
        
        while page_num < max_pages:
            # Find job links - Chewy uses /us/en/job/ pattern
            links = page.locator('a[href*="/job/"]').all()
            
            found_on_page = 0
            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    title_el = link.locator('h2, h3, .job-title').first
                    title = title_el.inner_text() if title_el.count() > 0 else link.inner_text().strip()
                    
                    if '/job/' in href and title:
                        # Extract job ID from URL
                        job_id_match = re.search(r'/job/([^/]+)', href)
                        if job_id_match:
                            job_id = job_id_match.group(1)
                            
                            if job_id not in jobs:
                                full_url = f"https://careers.chewy.com{href}" if not href.startswith('http') else href
                                restrictions = detect_restrictions(title)
                                jobs[job_id] = {
                                    "id": job_id,
                                    "title": title[:200],  # Truncate long titles
                                    "url": full_url,
                                    "firstSeen": datetime.now().isoformat(),
                                    "restrictions": restrictions
                                }
                                new_found += 1
                                found_on_page += 1
                except:
                    continue
            
            page_num += 1
            if (page_num) % 5 == 0:
                print(f"  Page {page_num}: {len(jobs)} total, {new_found} new", flush=True)
            
            # Try to go to next page
            try:
                next_btn = page.locator('button:has-text("Next"), a:has-text("Next")').first
                if next_btn.count() > 0 and next_btn.is_enabled():
                    next_btn.click()
                    page.wait_for_timeout(2000)
                else:
                    break  # No more pages
            except:
                break
        
        browser.close()
    
    db["companies"][company_id] = jobs
    save_db(db)
    
    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# CIGNA FETCHER (Playwright - JS rendered, handles both Cigna brands)
# =============================================================================
def fetch_cigna_jobs(base_url, db, company_id):
    """Fetch Cigna Group jobs using Playwright (works for both Healthcare and Evernorth URLs)."""
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
        page.wait_for_timeout(4000)

        # Get total count
        total = 500
        try:
            count_text = page.locator('text=/\\d+ jobs/i, text=/\\d+ results/i').first.inner_text()
            match = re.search(r'(\d+)', count_text)
            total = int(match.group(1)) if match else 500
            print(f"Total: {total} jobs", flush=True)
        except:
            print("Could not determine job count, estimating 500", flush=True)

        new_found = 0
        page_num = 0
        max_pages = 100

        while page_num < max_pages:
            # Cigna uses /en_US/job/ or /us/en/job/ patterns
            links = page.locator('a[href*="/job/"]').all()

            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    title = link.inner_text().strip()

                    job_id_match = re.search(r'/job/([^/?#]+)', href)
                    if job_id_match and title:
                        job_id = job_id_match.group(1)

                        if job_id not in jobs:
                            full_url = href if href.startswith('http') else f"https://jobs.thecignagroup.com{href}"
                            restrictions = detect_restrictions(title)
                            jobs[job_id] = {
                                "id": job_id,
                                "title": title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat(),
                                "restrictions": restrictions
                            }
                            new_found += 1
                except:
                    continue

            page_num += 1

            if page_num % 5 == 0:
                print(f"  Page {page_num}: {len(jobs)} total, {new_found} new", flush=True)
                db["companies"][company_id] = jobs
                save_db(db)

            # Try next page button
            try:
                next_btn = page.locator('a[aria-label="Next"], button[aria-label="Next"], a:has-text("Next"), li.next a').first
                if next_btn.count() > 0 and next_btn.is_visible():
                    next_btn.click()
                    page.wait_for_timeout(3000)
                else:
                    break
            except:
                break

        browser.close()

    db["companies"][company_id] = jobs
    save_db(db)

    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# ELEVANCE HEALTH FETCHER (Playwright - JS rendered, paginated)
# =============================================================================
def fetch_elevancehealth_jobs(base_url, db, company_id):
    """Fetch Elevance Health jobs using Playwright."""
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
        page.wait_for_timeout(4000)

        # Get total count
        total = 500
        try:
            count_text = page.locator('text=/\\d+ jobs/i, text=/\\d+ results/i, text=/\\d+ open/i').first.inner_text()
            match = re.search(r'(\d+)', count_text)
            total = int(match.group(1)) if match else 500
            print(f"Total: {total} jobs", flush=True)
        except:
            print("Could not determine job count, estimating 500", flush=True)

        new_found = 0
        page_num = 0
        max_pages = 100

        while page_num < max_pages:
            # Elevance pattern: /job-title-slug/job/HEX_ID
            links = page.locator('a[href*="/job/"]').all()

            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    title = link.inner_text().strip()

                    # Elevance pattern: /job-title-slug/job/HEX_ID
                    job_id_match = re.search(r'/job/([A-Fa-f0-9]{8,})', href)
                    if job_id_match and title and 'Apply Now' not in title:
                        job_id = job_id_match.group(1)

                        if job_id not in jobs:
                            full_url = href if href.startswith('http') else f"https://careers.elevancehealth.com{href}"
                            restrictions = detect_restrictions(title)
                            jobs[job_id] = {
                                "id": job_id,
                                "title": title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat(),
                                "restrictions": restrictions
                            }
                            new_found += 1
                except:
                    continue

            page_num += 1

            if page_num % 5 == 0:
                print(f"  Page {page_num}: {len(jobs)} total, {new_found} new", flush=True)
                db["companies"][company_id] = jobs
                save_db(db)

            # Try next page
            try:
                next_btn = page.locator('a[aria-label="Next"], button[aria-label="Next"], a:has-text("Next")').first
                if next_btn.count() > 0 and next_btn.is_visible():
                    next_btn.click()
                    page.wait_for_timeout(3000)
                else:
                    break
            except:
                break

        browser.close()

    db["companies"][company_id] = jobs
    save_db(db)

    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# GENERAL MOTORS FETCHER (Playwright - JS rendered)
# =============================================================================
def fetch_gm_jobs(base_url, db, company_id):
    """Fetch General Motors jobs using Playwright."""
    from playwright.sync_api import sync_playwright

    jobs = db["companies"].get(company_id, {})

    print("Launching browser...", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()

        # GM job listings live at /en/jobs/ not root
        gm_jobs_url = "https://search-careers.gm.com/en/jobs/"
        print(f"Loading {gm_jobs_url}...", flush=True)
        page.goto(gm_jobs_url, wait_until='networkidle', timeout=90000)
        page.wait_for_timeout(4000)

        # Get total count
        total = 500
        try:
            count_text = page.locator('text=/\\d+ jobs/i, text=/\\d+ results/i').first.inner_text()
            match = re.search(r'(\d+)', count_text)
            total = int(match.group(1)) if match else 500
            print(f"Total: {total} jobs", flush=True)
        except:
            print("Could not determine job count, estimating 500", flush=True)

        new_found = 0
        page_num = 0
        max_pages = 100

        while page_num < max_pages:
            # GM pattern: /en/jobs/jr-XXXXXXXXX/job-title/
            links = page.locator('a[href*="/en/jobs/jr-"]').all()

            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    title = link.inner_text().strip()

                    job_id_match = re.search(r'/en/jobs/(jr-\d+)/', href)
                    if job_id_match and title and len(title) > 3:
                        job_id = job_id_match.group(1)

                        if job_id not in jobs:
                            full_url = href if href.startswith('http') else f"https://search-careers.gm.com{href}"
                            restrictions = detect_restrictions(title)
                            jobs[job_id] = {
                                "id": job_id,
                                "title": title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat(),
                                "restrictions": restrictions
                            }
                            new_found += 1
                except:
                    continue

            page_num += 1

            if page_num % 5 == 0:
                print(f"  Page {page_num}: {len(jobs)} total, {new_found} new", flush=True)
                db["companies"][company_id] = jobs
                save_db(db)

            # Try next page
            try:
                next_btn = page.locator('a[aria-label="Next"], button[aria-label="Next"], a:has-text("Next")').first
                if next_btn.count() > 0 and next_btn.is_visible():
                    next_btn.click()
                    page.wait_for_timeout(3000)
                else:
                    break
            except:
                break

        browser.close()

    db["companies"][company_id] = jobs
    save_db(db)

    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# COCA-COLA FETCHER (Playwright - JS rendered)
# =============================================================================
def fetch_cocacola_jobs(base_url, db, company_id):
    """Fetch Coca-Cola jobs using Playwright."""
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
        page.wait_for_timeout(4000)

        # Get total count
        total = 200
        try:
            count_text = page.locator('text=/\\d+ Live Results/i, text=/\\d+ jobs/i, text=/\\d+ results/i').first.inner_text()
            match = re.search(r'(\d+)', count_text)
            total = int(match.group(1)) if match else 200
            print(f"Total: {total} jobs", flush=True)
        except:
            print("Could not determine job count, estimating 200", flush=True)

        new_found = 0
        page_num = 0
        max_pages = 50

        while page_num < max_pages:
            # Coca-Cola pattern: /job/{numeric_id}/{job-title-slug}/
            links = page.locator('a[href*="/job/"]').all()

            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    title = link.inner_text().strip()

                    # Match: /job/23130156/job-title-slug/
                    job_id_match = re.search(r'/job/(\d+)/', href)
                    if job_id_match and title and len(title) > 3:
                        job_id = job_id_match.group(1)

                        if job_id not in jobs:
                            full_url = href if href.startswith('http') else f"https://careers.coca-colacompany.com{href}"
                            restrictions = detect_restrictions(title)
                            jobs[job_id] = {
                                "id": job_id,
                                "title": title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat(),
                                "restrictions": restrictions
                            }
                            new_found += 1
                except:
                    continue

            page_num += 1

            if page_num % 5 == 0:
                print(f"  Page {page_num}: {len(jobs)} total, {new_found} new", flush=True)
                db["companies"][company_id] = jobs
                save_db(db)

            # Try next page
            try:
                next_btn = page.locator('a[aria-label="Next"], button[aria-label="Next"], a:has-text("Next"), button:has-text("Next")').first
                if next_btn.count() > 0 and next_btn.is_visible():
                    next_btn.click()
                    page.wait_for_timeout(3000)
                else:
                    break
            except:
                break

        browser.close()

    db["companies"][company_id] = jobs
    save_db(db)

    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# META FETCHER (Playwright - JS rendered)
# =============================================================================
def fetch_meta_jobs(base_url, db, company_id):
    """Fetch Meta jobs using Playwright with infinite scroll."""
    from playwright.sync_api import sync_playwright
    
    jobs = db["companies"].get(company_id, {})
    
    print("Launching browser...", flush=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        # Load job search page
        search_url = "https://www.metacareers.com/jobs"
        print(f"Loading {search_url}...", flush=True)
        page.goto(search_url, wait_until='networkidle', timeout=90000)
        page.wait_for_timeout(5000)
        
        # Get total count
        total = 1000
        try:
            count_text = page.locator('text=/\\d+ Items/i').first.inner_text()
            match = re.search(r'(\d+)', count_text)
            total = int(match.group(1)) if match else 1000
            print(f"Total: {total} jobs", flush=True)
        except:
            print("Could not get job count, estimating 1000", flush=True)
        
        new_found = 0
        last_count = 0
        no_change_rounds = 0
        max_scrolls = 200  # Safety limit
        
        for scroll_num in range(max_scrolls):
            # Find job links - Meta uses /profile/job_details/ID pattern
            links = page.locator('a[href*="/profile/job_details/"]').all()
            
            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    
                    # Match: /profile/job_details/123456789
                    job_match = re.search(r'/profile/job_details/(\d+)', href)
                    if job_match:
                        job_id = job_match.group(1)
                        
                        if job_id not in jobs:
                            # Get title from h3 inside the link
                            try:
                                title_el = link.locator('h3').first
                                title = title_el.inner_text().strip() if title_el else ""
                            except:
                                title = ""
                            
                            if not title:
                                title = f"Meta Job {job_id}"
                            
                            full_url = f"https://www.metacareers.com/profile/job_details/{job_id}"
                            restrictions = detect_restrictions(title)
                            jobs[job_id] = {
                                "id": job_id,
                                "title": title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat(),
                                "restrictions": restrictions
                            }
                            new_found += 1
                except:
                    continue
            
            # Check if we're still finding new jobs
            if len(jobs) == last_count:
                no_change_rounds += 1
                if no_change_rounds >= 5:
                    print(f"  No new jobs after 5 scrolls, stopping", flush=True)
                    break
            else:
                no_change_rounds = 0
                last_count = len(jobs)
            
            # Scroll down to load more
            page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            page.wait_for_timeout(1500)
            
            # Progress update every 10 scrolls
            if (scroll_num + 1) % 10 == 0:
                print(f"  Scroll {scroll_num + 1}: {len(jobs)} total, {new_found} new", flush=True)
                db["companies"][company_id] = jobs
                save_db(db)
            
            # Stop if we have all jobs
            if len(jobs) >= total:
                print(f"  Reached total ({total}), stopping", flush=True)
                break
        
        browser.close()
    
    db["companies"][company_id] = jobs
    save_db(db)
    
    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found



# =============================================================================
# INTEL FETCHER (Playwright - Workday JS rendered, paginated)
# =============================================================================
def fetch_intel_jobs(base_url, db, company_id):
    """Fetch Intel jobs from Workday using Playwright."""
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

        # Get total count
        total = 500
        try:
            count_text = page.locator('text=/\\d+ jobs/i, text=/\\d+ results/i').first.inner_text()
            match = re.search(r'(\d+)', count_text)
            total = int(match.group(1)) if match else 500
            print(f"Total: {total} jobs", flush=True)
        except:
            print("Could not determine job count, estimating 500", flush=True)

        new_found = 0
        page_num = 0
        max_pages = 100

        while page_num < max_pages:
            # Intel Workday pattern: /en-US/External/job/{location}/{title}_JR{id}
            links = page.locator('a[href*="/External/job/"]').all()

            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    title = link.inner_text().strip()

                    # Extract JR job ID — only US locations
                    job_id_match = re.search(r'_(JR\d+)', href)
                    is_us = '/US-' in href
                    if job_id_match and title and len(title) > 3 and is_us:
                        job_id = job_id_match.group(1)

                        if job_id not in jobs:
                            full_url = href if href.startswith('http') else f"https://intel.wd1.myworkdayjobs.com{href}"
                            restrictions = detect_restrictions(title)
                            jobs[job_id] = {
                                "id": job_id,
                                "title": title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat(),
                                "restrictions": restrictions
                            }
                            new_found += 1
                except:
                    continue

            page_num += 1

            if page_num % 5 == 0:
                print(f"  Page {page_num}: {len(jobs)} total, {new_found} new", flush=True)
                db["companies"][company_id] = jobs
                save_db(db)

            # Try next page button
            try:
                next_btn = page.locator('button[aria-label="next"]').first
                if next_btn.count() > 0 and next_btn.is_visible() and next_btn.is_enabled():
                    next_btn.click()
                    page.wait_for_timeout(3000)
                else:
                    break
            except:
                break

        browser.close()

    db["companies"][company_id] = jobs
    save_db(db)

    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# IBM FETCHER (Playwright - JS rendered, paginated)
# =============================================================================
def fetch_ibm_jobs(base_url, db, company_id):
    """Fetch IBM jobs using Playwright."""
    from playwright.sync_api import sync_playwright

    jobs = db["companies"].get(company_id, {})

    print("Launching browser...", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()

        print(f"Loading {base_url[:80]}...", flush=True)
        page.goto(base_url, wait_until='domcontentloaded', timeout=90000)
        page.wait_for_timeout(8000)

        # Get total count
        total = 500
        try:
            count_text = page.locator('text=/\\d+ job/i').first.inner_text()
            match = re.search(r'(\d+)', count_text)
            total = int(match.group(1)) if match else 500
            print(f"Total: {total} jobs", flush=True)
        except:
            print("Could not determine job count, estimating 500", flush=True)

        new_found = 0
        page_num = 0
        max_pages = 100
        last_job_count = 0
        no_change_rounds = 0

        while page_num < max_pages:
            # IBM pattern: careers.ibm.com/careers/JobDetail?jobId={id}
            links = page.locator('a[href*="JobDetail"]').all()

            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    title = link.inner_text().strip()

                    job_id_match = re.search(r'jobId=(\d+)', href)
                    if job_id_match and title and len(title) > 3:
                        job_id = job_id_match.group(1)
                        # Clean title (IBM puts category/level/location in same element)
                        clean_title = title.split('\n')[1].strip() if '\n' in title else title

                        if job_id not in jobs:
                            full_url = f"https://careers.ibm.com/careers/JobDetail?jobId={job_id}"
                            restrictions = detect_restrictions(clean_title)
                            jobs[job_id] = {
                                "id": job_id,
                                "title": clean_title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat(),
                                "restrictions": restrictions
                            }
                            new_found += 1
                except:
                    continue

            page_num += 1

            if page_num % 5 == 0:
                print(f"  Page {page_num}: {len(jobs)} total, {new_found} new", flush=True)
                db["companies"][company_id] = jobs
                save_db(db)

            # Check if stuck
            if len(jobs) == last_job_count:
                no_change_rounds += 1
                if no_change_rounds >= 3:
                    print("  No new jobs after 3 pages, stopping", flush=True)
                    break
            else:
                no_change_rounds = 0
                last_job_count = len(jobs)

            # Try next page
            try:
                next_btn = page.locator('button[aria-label*="Next"], a[aria-label*="Next"], button:has-text("Next")').first
                if next_btn.count() > 0 and next_btn.is_visible() and next_btn.is_enabled():
                    next_btn.click()
                    page.wait_for_timeout(4000)
                else:
                    break
            except:
                break

        browser.close()

    db["companies"][company_id] = jobs
    save_db(db)

    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# MORGAN STANLEY FETCHER (Playwright - Eightfold AI, paginated)
# =============================================================================
def fetch_morganstanley_jobs(base_url, db, company_id):
    """Fetch Morgan Stanley jobs from Eightfold AI using Playwright."""
    from playwright.sync_api import sync_playwright

    jobs = db["companies"].get(company_id, {})

    print("Launching browser...", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()

        print(f"Loading {base_url[:80]}...", flush=True)
        page.goto(base_url, wait_until='domcontentloaded', timeout=90000)
        page.wait_for_timeout(6000)

        new_found = 0
        page_num = 0
        max_pages = 100
        no_change_rounds = 0
        last_count = 0

        while page_num < max_pages:
            # Morgan Stanley Eightfold pattern: /careers/job/{id}
            links = page.locator('a[href*="/careers/job/"]').all()

            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.inner_text().strip()

                    job_id_match = re.search(r'/careers/job/(\d+)', href)
                    if not job_id_match or len(text) < 3:
                        continue

                    job_id = job_id_match.group(1)

                    # US-only filter
                    if 'United States' not in text and 'United States' not in href:
                        # Check location from text
                        if not any(state in text for state in [
                            ', New York,', ', Texas,', ', California,', ', Florida,',
                            ', Georgia,', ', Maryland,', ', Virginia,', ', Illinois,',
                            ', Massachusetts,', ', New Jersey,', ', Arizona,', ', Colorado,',
                            ', Utah,', ', Washington,', ', North Carolina,'
                        ]):
                            continue

                    # Clean title (first line of text)
                    clean_title = text.split('\n')[0].strip()

                    if job_id not in jobs and len(clean_title) > 3:
                        full_url = f"https://morganstanley.eightfold.ai/careers/job/{job_id}?source=mscom"
                        restrictions = detect_restrictions(clean_title)
                        jobs[job_id] = {
                            "id": job_id,
                            "title": clean_title,
                            "url": full_url,
                            "firstSeen": datetime.now().isoformat(),
                            "restrictions": restrictions
                        }
                        new_found += 1
                except:
                    continue

            page_num += 1

            if page_num % 5 == 0:
                print(f"  Page {page_num}: {len(jobs)} total, {new_found} new", flush=True)
                db["companies"][company_id] = jobs
                save_db(db)

            # Check if stuck
            if len(jobs) == last_count:
                no_change_rounds += 1
                if no_change_rounds >= 3:
                    print("  No new jobs after 3 pages, stopping", flush=True)
                    break
            else:
                no_change_rounds = 0
                last_count = len(jobs)

            # Next page
            try:
                next_btn = page.locator('button[aria-label*="Next"], a[aria-label*="Next"], button:has-text("Next"), [data-ph-at-id*="next"]').first
                if next_btn.count() > 0 and next_btn.is_visible() and next_btn.is_enabled():
                    next_btn.click()
                    page.wait_for_timeout(4000)
                else:
                    # Try URL-based pagination
                    current_start = int(re.search(r'start=(\d+)', page.url).group(1)) if 'start=' in page.url else 0
                    next_url = re.sub(r'start=\d+', f'start={current_start + 20}', page.url)
                    if next_url != page.url:
                        page.goto(next_url, wait_until='domcontentloaded', timeout=30000)
                        page.wait_for_timeout(4000)
                    else:
                        break
            except:
                break

        browser.close()

    db["companies"][company_id] = jobs
    save_db(db)

    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# CHILDREN'S HEALTHCARE OF ATLANTA (CHOA) FETCHER (Playwright - Phenom People)
# =============================================================================
def fetch_choa_jobs(base_url, db, company_id):
    """Fetch CHOA jobs using Playwright."""
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
        page.goto(base_url, wait_until='domcontentloaded', timeout=90000)
        page.wait_for_timeout(5000)

        new_found = 0
        page_num = 0
        max_pages = 100
        no_change_rounds = 0
        last_count = 0

        per_page = 10
        offset = 0

        while page_num < max_pages:
            # Navigate via URL pagination: ?from=offset&s=1
            if offset == 0:
                page_url = base_url
            else:
                page_url = f"{base_url}&from={offset}&s=1"

            if page_num > 0:
                page.goto(page_url, wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(3000)

            # CHOA pattern: /us/en/job/R-{id}/{title}
            links = page.locator('a[href*="/us/en/job/R-"]').all()

            found_this_page = 0
            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    title = link.inner_text().strip()

                    job_id_match = re.search(r'/job/(R-\d+)/', href)
                    if job_id_match and title and len(title) > 3:
                        job_id = job_id_match.group(1)

                        if job_id not in jobs:
                            full_url = href if href.startswith('http') else f"https://careers.choa.org{href}"
                            restrictions = detect_restrictions(title)
                            jobs[job_id] = {
                                "id": job_id,
                                "title": title,
                                "url": full_url,
                                "firstSeen": datetime.now().isoformat(),
                                "restrictions": restrictions
                            }
                            new_found += 1
                            found_this_page += 1
                except:
                    continue

            page_num += 1
            offset += per_page

            if page_num % 10 == 0:
                print(f"  Page {page_num}: {len(jobs)} total, {new_found} new", flush=True)
                db["companies"][company_id] = jobs
                save_db(db)

            # Stop if no new jobs found on this page
            if found_this_page == 0:
                no_change_rounds += 1
                if no_change_rounds >= 2:
                    print(f"  No new jobs on page {page_num}, stopping", flush=True)
                    break
            else:
                no_change_rounds = 0

        browser.close()

    db["companies"][company_id] = jobs
    save_db(db)

    print(f"Done: {len(jobs)} total jobs, {new_found} new", flush=True)
    return jobs, new_found


# =============================================================================
# QUALCOMM FETCHER (Playwright - Eightfold AI, US-only, paginated)
# =============================================================================
def fetch_qualcomm_jobs(base_url, db, company_id):
    """Fetch Qualcomm jobs using Playwright - US only."""
    from playwright.sync_api import sync_playwright

    jobs = db["companies"].get(company_id, {})

    print("Launching browser...", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()

        print(f"Loading {base_url[:80]}...", flush=True)
        page.goto(base_url, wait_until='domcontentloaded', timeout=90000)
        page.wait_for_timeout(6000)

        new_found = 0
        page_num = 0
        max_pages = 100
        no_change_rounds = 0
        last_count = 0

        while page_num < max_pages:
            links = page.locator('a[href*="/careers/job/"]').all()

            for link in links:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.inner_text().strip()

                    job_id_match = re.search(r'/careers/job/(\d+)', href)
                    if not job_id_match or len(text) < 3:
                        continue

                    job_id = job_id_match.group(1)

                    # US-only filter
                    us_states = [
                        'United States', ', California', ', Texas', ', New York',
                        ', Georgia', ', Washington', ', Colorado', ', Arizona',
                        ', Illinois', ', Massachusetts', ', New Jersey', ', Virginia',
                        ', North Carolina', ', Florida', ', Oregon', ', Michigan'
                    ]
                    if not any(s in text for s in us_states):
                        continue

                    clean_title = text.split('\n')[0].strip()

                    if job_id not in jobs and len(clean_title) > 3:
                        full_url = f"https://careers.qualcomm.com/careers/job/{job_id}?domain=qualcomm.com"
                        restrictions = detect_restrictions(clean_title)
                        jobs[job_id] = {
                            "id": job_id,
                            "title": clean_title,
                            "url": full_url,
                            "firstSeen": datetime.now().isoformat(),
                            "restrictions": restrictions
                        }
                        new_found += 1
                except:
                    continue

            page_num += 1

            if page_num % 5 == 0:
                print(f"  Page {page_num}: {len(jobs)} total, {new_found} new", flush=True)
                db["companies"][company_id] = jobs
                save_db(db)

            if len(jobs) == last_count:
                no_change_rounds += 1
                if no_change_rounds >= 3:
                    print("  No new jobs after 3 pages, stopping", flush=True)
                    break
            else:
                no_change_rounds = 0
                last_count = len(jobs)

            # URL-based pagination: start=0, start=10, start=20...
            try:
                current_start = int(re.search(r'start=(\d+)', page.url).group(1)) if 'start=' in page.url else 0
                next_start = current_start + 10
                next_url = re.sub(r'start=\d+', f'start={next_start}', page.url)
                if next_url == page.url:
                    # start param not in URL yet, add it
                    sep = '&' if '?' in page.url else '?'
                    next_url = f"{page.url}{sep}start={next_start}"
                page.goto(next_url, wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(3000)
            except:
                break

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
    "synopsys": fetch_synopsys_jobs,
    "chewy": fetch_chewy_jobs,
    "meta": fetch_meta_jobs,
    "cigna-healthcare": fetch_cigna_jobs,
    "cigna-evernorth": fetch_cigna_jobs,
    "elevancehealth": fetch_elevancehealth_jobs,
    "gm": fetch_gm_jobs,
    "cocacola": fetch_cocacola_jobs,
    "intel": fetch_intel_jobs,
    "ibm": fetch_ibm_jobs,
    "morganstanley": fetch_morganstanley_jobs,
    "choa": fetch_choa_jobs,
    "qualcomm": fetch_qualcomm_jobs,
}


def fetch_jobs(company, db):
    """Route to appropriate fetcher based on company ID."""
    company_id = company["id"]
    fetcher = FETCHERS.get(company_id)
    
    if fetcher:
        try:
            return fetcher(company["url"], db, company_id)
        except Exception as e:
            print(f"  ❌ {company_id} fetcher crashed: {e}", flush=True)
            return db["companies"].get(company_id, {}), 0
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
        clean_count = len([j for j in relevant if not has_restrictions(j)])
        restr_count = len([j for j in relevant if has_restrictions(j)])

        if is_baseline:
            status = "BASELINE"
            notes = f"Initial scan, {len(jobs)} jobs indexed"
        elif relevant:
            status = "🎯 MATCH"
            restr_note = f" ({restr_count} restricted)" if restr_count else ""
            notes = f"{len(relevant)} relevant found!{restr_note}"
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
        _restr = len([j for j in relevant if has_restrictions(j)])
        print(f"  Relevant: {len(relevant)}", flush=True)
        if _restr:
            print(f"  Restricted (tagged): {_restr}", flush=True)
        
        if relevant:
            print(f"\n  🎯 RELEVANT JOBS (contact {', '.join(company['referrers'])}):", flush=True)
            for job in relevant:
                tag = f"  ⚠️ [{restriction_label(job)}]" if has_restrictions(job) else ""
                print(f"    • {job['title']}{tag}", flush=True)
                print(f"      {job['url']}", flush=True)


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else None
    main(target)
