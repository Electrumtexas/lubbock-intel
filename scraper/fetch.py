"""
Lubbock County Motivated Seller Lead Scraper
Built specifically for Tyler Technologies EagleWeb system
erecord.lubbockcounty.gov
"""

import json, re, csv, io, os, time, logging, traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
BASE_URL    = "https://erecord.lubbockcounty.gov"
SEARCH_URL  = BASE_URL + "/recorder/eagleweb/docSearch.jsp"
RESULTS_URL = BASE_URL + "/recorder/eagleweb/docSearchResults.jsp"
DETAIL_URL  = BASE_URL + "/recorder/eagleweb/viewDoc.jsp"
OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
CAD_CACHE_PATH = Path("data/cad_cache.json")

def load_cad_cache() -> dict:
    """Load cached CAD lookups. Keys are owner names, values are address dicts or None."""
    try:
        if CAD_CACHE_PATH.exists():
            return json.loads(CAD_CACHE_PATH.read_text())
    except Exception:
        pass
    return {}

def save_cad_cache(cache: dict):
    """Save CAD cache to disk."""
    try:
        CAD_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CAD_CACHE_PATH.write_text(json.dumps(cache, indent=2))
    except Exception as e:
        log.warning(f"Could not save CAD cache: {e}")

LEAD_KEYWORDS = {
    "LIS PENDENS":               "LP",
    "NOTICE OF FORECLOSURE":     "NOFC",
    "FORECLOSURE":               "NOFC",
    "TAX DEED":                  "TAXDEED",
    "ABSTRACT OF JUDGMENT":      "JUD",
    "CERTIFIED JUDGMENT":        "CCJ",
    "DOMESTIC JUDGMENT":         "DRJUD",
    "JUDGMENT":                  "JUD",
    "CORP TAX LIEN":             "LNCORPTX",
    "CORPORATE TAX":             "LNCORPTX",
    "IRS LIEN":                  "LNIRS",
    "FEDERAL TAX LIEN":          "LNFED",
    "FEDERAL LIEN":              "LNFED",
    "MECHANIC":                  "LNMECH",
    "MATERIALMAN":               "LNMECH",
    "HOA LIEN":                  "LNHOA",
    "HOMEOWNER":                 "LNHOA",
    "MEDICAID":                  "MEDLN",
    "LETTERS TESTAMENTARY":      "PRO",
    "LETTERS OF ADMINISTRATION": "PRO",
    "PROBATE":                   "PRO",
    "NOTICE OF COMMENCEMENT":    "NOC",
    "RELEASE OF LIS PENDENS":    "RELLP",
    "RELEASE LIS PENDENS":       "RELLP",
    "LIEN":                      "LN",
    # Divorce / family law — court-ordered property dispositions
    "DIVORCE":                   "DIV",
    "DISSOLUTION OF MARRIAGE":   "DIV",
    "MARITAL SETTLEMENT":        "DIV",
    "FAMILY LAW ORDER":          "DIV",
    "QUALIFIED DOMESTIC":        "DIV",
    # Trustee / substitute trustee sale notices — Texas non-judicial foreclosure
    "SUBSTITUTE TRUSTEE":        "NOFC",
    "TRUSTEE'S SALE":            "NOFC",
    "TRUSTEE SALE":              "NOFC",
    "NOTICE OF SALE":            "NOFC",
    "APPOINTMENT OF SUBSTITUTE": "NOFC",
    # EagleWeb stores trustee sale notices under the generic type "POSTING"
    "POSTING":                   "NOFC",
}

LEAD_TYPES = {
    "LP":      {"label": "Lis Pendens",           "cat": "foreclosure"},
    "NOFC":    {"label": "Notice of Foreclosure",  "cat": "foreclosure"},
    "TAXDEED": {"label": "Tax Deed",               "cat": "tax"},
    "JUD":     {"label": "Judgment",               "cat": "judgment"},
    "CCJ":     {"label": "Certified Judgment",     "cat": "judgment"},
    "DRJUD":   {"label": "Domestic Judgment",      "cat": "judgment"},
    "LNCORPTX":{"label": "Corp Tax Lien",          "cat": "lien"},
    "LNIRS":   {"label": "IRS Lien",               "cat": "lien"},
    "LNFED":   {"label": "Federal Lien",           "cat": "lien"},
    "LN":      {"label": "Lien",                   "cat": "lien"},
    "LNMECH":  {"label": "Mechanic Lien",          "cat": "lien"},
    "LNHOA":   {"label": "HOA Lien",               "cat": "lien"},
    "MEDLN":   {"label": "Medicaid Lien",          "cat": "lien"},
    "PRO":     {"label": "Probate",                "cat": "probate"},
    "NOC":     {"label": "Notice of Commencement", "cat": "other"},
    "RELLP":   {"label": "Release Lis Pendens",    "cat": "release"},
    "DIV":     {"label": "Divorce / Family Order", "cat": "probate"},
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("scraper")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

LOGIN_URL = BASE_URL + "/recorder/web/login.jsp"

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    for attempt in range(3):
        try:
            # Step 1 — load the login page to get any session cookies
            log.info("Loading login page...")
            r = session.get(LOGIN_URL, timeout=30)
            log.info(f"Login page status: {r.status_code}")

            # Step 2 — submit Public Login
            log.info("Submitting Public Login...")
            payload = {"submit": "Public Login", "guest": "true"}
            r2 = session.post(BASE_URL + "/recorder/web/loginPOST.jsp", data=payload, timeout=30, allow_redirects=True)
            log.info(f"After public login: {r2.status_code} — {r2.url}")

            if "docSearch" in r2.url or "eagleweb" in r2.url:
                log.info("Successfully logged in as public user")
                return session

            # Step 3 — if not redirected, try GET to docSearch directly
            r3 = session.get(SEARCH_URL, timeout=30)
            log.info(f"DocSearch direct: {r3.status_code} — {r3.url}")
            if "docSearch" in r3.url:
                log.info("Session active — on search page")
                return session

            log.warning(f"Unexpected URL after login: {r2.url}")

        except Exception as e:
            log.warning(f"Session attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)

    log.error("Could not establish session after 3 attempts")
    return session

def post_search(session, start, end):
    payload = {
        "DocNumID":                   "",
        "BookVolPageIDBook":           "",
        "BookVolPageIDVolume":         "",
        "BookVolPageIDPage":           "",
        "RecDateIDStart":              start,
        "RecDateIDEnd":                end,
        "BothNamesIDSearchString":     "",
        "BothNamesIDSearchType":       "Basic Searching",
        "GrantorIDSearchString":       "",
        "GrantorIDSearchType":         "Basic Searching",
        "GranteeIDSearchString":       "",
        "GranteeIDSearchType":         "Basic Searching",
        "PlattedLegalIDSubdivision":   "",
        "PlattedLegalIDLot":           "",
        "PlattedLegalIDBlock":         "",
        "PlattedLegalIDTract":         "",
        "PlattedLegalIDUnit":          "",
        "LegalRemarksIDSearchString":  "",
        "LegalRemarksIDSearchType":    "Starts With",
        "AllDocuments":                "ALL",
        "docTypeTotal":                "129",
    }
    search_post_url = "https://erecord.lubbockcounty.gov/recorder/eagleweb/docSearchPOST.jsp"
    for attempt in range(3):
        try:
            log.info(f"POSTing search {start} to {end} (attempt {attempt+1})")
            r = session.post(search_post_url, data=payload, timeout=30, allow_redirects=True)
            log.info(f"Response: {r.status_code} — {r.url}")
            if "docSearchResults" in r.url or "items found" in r.text.lower() or "Party One" in r.text:
                log.info("Search results reached")
                return True
        except Exception as e:
            log.warning(f"Search attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return False

def fetch_results_page(session, page):
    url = f"{RESULTS_URL}?searchId=0&page={page}"
    for attempt in range(3):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200:
                return r.text
        except Exception as e:
            log.warning(f"Page {page} attempt {attempt+1}: {e}")
            time.sleep(2 ** attempt)
    return None

def parse_results_page(html):
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # Find total count line — "1,255 items found"
    count_text = soup.get_text()
    total_match = re.search(r"([\d,]+)\s+items?\s+found", count_text, re.I)
    if total_match:
        log.info(f"  Total in system: {total_match.group(1)} items")

    # Find the results table
    table = None
    for t in soup.find_all("table"):
        txt = t.get_text()
        if "Party One" in txt or "Filing Date" in txt or "Recording Date" in txt:
            table = t
            break

    if table:
        trs = table.find_all("tr")
        for tr in trs:
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            # Extract node link
            node = ""
            for a in tr.find_all("a", href=True):
                m = re.search(r"node=([A-Z0-9]+)", a["href"])
                if m:
                    node = m.group(1)
                    break

            cell_texts = [td.get_text(" ", strip=True) for td in tds]
            full = " ".join(cell_texts).upper()

            # Skip header rows
            if "PARTY ONE" in full and "FILING DATE" in full:
                continue
            if "DESCRIPTION" in full and "SUMMARY" in full:
                continue

            # First cell has doc type + doc number
            first_lines = [l.strip() for l in tds[0].get_text("\n", strip=True).split("\n") if l.strip()]
            desc    = first_lines[0] if first_lines else cell_texts[0]
            doc_num = first_lines[-1] if len(first_lines) > 1 else ""

            # Pull date and parties from remaining cells
            filed, party1, party2 = "", "", ""
            for ct in cell_texts[1:]:
                dm = re.search(r"(\d{2}/\d{2}/\d{4})", ct)
                if dm and not filed:
                    filed = dm.group(1)
                    continue
                # Party names — filter out label words
                clean = re.sub(r"(Party One|Party Two|Filing Date|Recording Date)", "", ct, flags=re.I).strip()
                if clean and not party1:
                    party1 = clean
                elif clean and not party2:
                    party2 = clean

            if desc or node:
                rows.append({
                    "description": desc.strip(),
                    "doc_num":     doc_num.strip(),
                    "filed":       filed.strip(),
                    "party_one":   party1.strip(),
                    "party_two":   party2.strip(),
                    "node":        node,
                })

    # Check for more pages
    has_more = bool(
        soup.find("a", string=re.compile(r"^next$|^>$", re.I)) or
        soup.find("a", href=re.compile(r"page=\d+"))
    )
    return rows, has_more

def match_lead_type(description, grantor="", grantee=""):
    desc = description.upper()
    for keyword in sorted(LEAD_KEYWORDS.keys(), key=len, reverse=True):
        if keyword in desc:
            # POSTING is a generic EagleWeb document type — filter to trustee sales only.
            # In Texas, every Notice of Trustee Sale names "PUBLIC" as a grantee because
            # the notice is addressed to the borrower AND the general public.
            # This is true regardless of whether the lender is a bank or an individual.
            if keyword == "POSTING":
                grantee_upper = grantee.upper()
                if "PUBLIC" not in grantee_upper:
                    return None
            return LEAD_KEYWORDS[keyword]
    return None

def fetch_detail(session, node):
    url = f"{DETAIL_URL}?node={node}"
    for attempt in range(3):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200:
                return parse_detail(r.text, node, url)
        except Exception as e:
            log.warning(f"Detail {node} attempt {attempt+1}: {e}")
            time.sleep(2 ** attempt)
    return {"clerk_url": url, "node": node}

def parse_detail(html, node, url):
    soup = BeautifulSoup(html, "lxml")
    data = {"clerk_url": url, "node": node}

    # Table-based fields (most reliable for Tyler Tech)
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        label = cells[0].get_text(strip=True).upper()
        value = cells[1].get_text(strip=True)
        if not value:
            continue
        if "DOCUMENT NUMBER" in label or "CERTIFICATE NUMBER" in label:
            data["doc_num"] = value
        elif "RECORDING DATE" in label:
            data["filed"] = value.split()[0]
        elif "DOCUMENT DATE" in label and "filed" not in data:
            data["filed"] = value.split()[0]
        elif "GRANTOR" in label and "NAME" not in label:
            data["owner"] = value
        elif "GRANTEE" in label and "NAME" not in label:
            data["grantee"] = value
        elif "LEGAL" in label:
            data.setdefault("legal", value[:200])
        elif "AMOUNT" in label or "CONSIDERATION" in label:
            try:
                amt = float(re.sub(r"[^\d.]", "", value))
                if amt >= 100:  # ignore recording fees
                    data["amount"] = amt
            except Exception:
                pass

    # Fallback: parse via labeled divs / free text
    full = soup.get_text("\n")
    if "owner" not in data:
        m = re.search(r"Grantor\s*\n\s*([A-Z][A-Z ,.'&\-]{2,60})", full)
        if m:
            data["owner"] = m.group(1).strip()
    if "grantee" not in data:
        m = re.search(r"Grantee\s*\n\s*([A-Z][A-Z ,.'&\-]{2,60})", full)
        if m:
            data["grantee"] = m.group(1).strip()
    if "filed" not in data:
        m = re.search(r"Recording Date\s*\n\s*(\d{2}/\d{2}/\d{4})", full)
        if m:
            data["filed"] = m.group(1)
    if "amount" not in data:
        m = re.search(r"\$\s*([\d,]+\.?\d*)", full)
        if m:
            try:
                amt = float(m.group(1).replace(",", ""))
                if amt >= 100:  # ignore recording fees ($25 etc)
                    data["amount"] = amt
            except Exception:
                pass

    if "filed" in data:
        data["filed"] = normalize_date(data["filed"])

    return data

def normalize_date(raw):
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    try:
        return datetime.strptime(raw, "%m/%d/%Y %I:%M:%S %p").strftime("%Y-%m-%d")
    except Exception:
        pass
    return raw[:10]

def enrich_from_cad(session, owner: str) -> dict:
    """
    Search Lubbock CAD using the JSON quick-search API, then fetch
    the detail page to extract property and mailing addresses.
    """
    if not owner or len(owner.strip()) < 3:
        return {}

    # Clean name — strip suffixes that confuse the search
    name_clean = owner.upper()
    # Remove probate/estate suffixes
    name_clean = re.sub(r"\bDEC'?D\b.*", "", name_clean)
    name_clean = re.sub(r"\bESTATE OF\b.*", "", name_clean)
    name_clean = re.sub(r"\bAKA\b.*", "", name_clean)
    name_clean = re.sub(r"\bDBA\b.*", "", name_clean)
    name_clean = re.sub(r"\bLLC\b.*", "", name_clean)
    name_clean = re.sub(r"\bINC\b.*", "", name_clean)
    name_clean = re.sub(r"\bCORP\b.*", "", name_clean)
    name_clean = re.sub(r"\bLTD\b.*", "", name_clean)
    name_clean = re.sub(r"\bPLLC\b.*", "", name_clean)
    name_clean = re.sub(r"[^A-Z0-9 &]", " ", name_clean).strip()
    name_clean = re.sub(r"\s+", " ", name_clean).strip()
    # Keep only first 3 words max for cleaner matching
    words = name_clean.split()
    if len(words) > 3:
        name_clean = " ".join(words[:3])
    if len(name_clean) < 3:
        return {}

    cad_headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer":    "https://lubbockcad.org/",
        "Accept":     "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        # Use the AJAX quick-search endpoint
        api_url = (
            "https://lubbockcad.org/ProxyT/Search/Properties/quick/"
            f"?f={requests.utils.quote(name_clean)}&pn=1&st=4&so=desc&pt=RP;PP;MH;NR&ty=2026"
        )
        # Retry up to 3 times with backoff if rate limited
        r = None
        for attempt in range(3):
            r = session.get(api_url, headers=cad_headers, timeout=20)
            if r.status_code == 200 and r.text.strip():
                break
            wait = 3 * (attempt + 1)
            log.warning(f"CAD rate limit hit, waiting {wait}s...")
            time.sleep(wait)
        if not r or not r.text.strip():
            return {}

        data = r.json()
        results = data.get("ResultList", [])
        if not results:
            return {}

        # Take the first residential property result
        hit = None
        for res in results:
            if res.get("PropertyQuickRefID", "").startswith("R"):
                hit = res
                break
        if not hit:
            hit = results[0]

        prop_qid   = hit.get("PropertyQuickRefID", "")
        party_qid  = hit.get("PartyQuickRefID", "")
        situs_addr = hit.get("SitusAddress", "") or ""

        # Parse situs address: "3006 56TH ST, LUBBOCK, TX  79413"
        prop_address, prop_city, prop_state, prop_zip = "", "LUBBOCK", "TX", ""
        if situs_addr:
            parts = [p.strip() for p in situs_addr.split(",")]
            prop_address = parts[0] if parts else ""
            if len(parts) >= 3:
                prop_city = parts[1].strip()
                last = parts[-1].split()
                if len(last) >= 2:
                    prop_state = last[-2]
                    prop_zip   = last[-1]
            elif len(parts) == 2:
                last = parts[-1].split()
                if len(last) >= 3:
                    prop_city  = last[0]
                    prop_state = last[1]
                    prop_zip   = last[2]

        # If we have property+party IDs, fetch detail for mailing address
        mail_address, mail_city, mail_state, mail_zip = "", "LUBBOCK", "TX", ""
        if prop_qid and party_qid:
            detail_url = (
                f"https://lubbockcad.org/Property-Detail/"
                f"PropertyQuickRefID/{prop_qid}/PartyQuickRefID/{party_qid}/"
            )
            r2 = session.get(detail_url, timeout=20)
            if r2.status_code == 200:
                soup = BeautifulSoup(r2.text, "lxml")
                mail_el = soup.find(id="dnn_ctr416_View_tdOIMailingAddress")
                if mail_el:
                    mail_raw = mail_el.get_text(" ", strip=True)
                    # Format: "4401 18TH ST LUBBOCK, TX 79416-5709"
                    # or multiline
                    lines = [l.strip() for l in re.split(r"[,\n]", mail_raw) if l.strip()]
                    if lines:
                        mail_address = lines[0]
                    if len(lines) >= 3:
                        mail_city  = lines[1]
                        last = lines[2].split()
                        if len(last) >= 2:
                            mail_state = last[0]
                            mail_zip   = last[-1]
                    elif len(lines) == 2:
                        last = lines[1].split()
                        if len(last) >= 3:
                            mail_city  = last[0]
                            mail_state = last[1]
                            mail_zip   = last[2]
                        elif len(last) >= 2:
                            mail_state = last[0]
                            mail_zip   = last[1]

        return {
            "prop_address": prop_address,
            "prop_city":    prop_city,
            "prop_state":   prop_state or "TX",
            "prop_zip":     prop_zip,
            "mail_address": mail_address,
            "mail_city":    mail_city,
            "mail_state":   mail_state or "TX",
            "mail_zip":     mail_zip,
        }

    except Exception as e:
        log.warning(f"CAD lookup failed for '{owner}': {e}")
        return {}


def compute_score(rec, cutoff_date):
    flags = []
    score = 30
    doc_type = rec.get("doc_type", "")
    amount   = rec.get("amount") or 0
    owner    = rec.get("owner", "")
    filed    = rec.get("filed", "")

    if doc_type == "LP":             flags.append("Lis pendens")
    if doc_type in ("NOFC", "LP"):   flags.append("Pre-foreclosure")
    if doc_type in ("JUD","CCJ","DRJUD"): flags.append("Judgment lien")
    if doc_type in ("TAXDEED","LNCORPTX","LNIRS","LNFED"): flags.append("Tax lien")
    if doc_type == "LNMECH":         flags.append("Mechanic lien")
    if doc_type == "PRO":            flags.append("Probate / estate")
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b", owner.upper()): flags.append("LLC / corp owner")

    score += len(flags) * 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags: score += 20
    if amount > 100_000:   score += 15
    elif amount > 50_000:  score += 10

    try:
        if datetime.strptime(filed[:10], "%Y-%m-%d") >= datetime.strptime(cutoff_date, "%Y-%m-%d"):
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    if rec.get("prop_address"): score += 5
    return min(score, 100), list(dict.fromkeys(flags))

def export_ghl_csv(records):
    out = io.StringIO()
    cols = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
            "Property Address","Property City","Property State","Property Zip",
            "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
            "Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    w = csv.DictWriter(out, fieldnames=cols)
    w.writeheader()
    for r in records:
        parts = (r.get("owner") or "").strip().split()
        w.writerow({
            "First Name": parts[0] if parts else "",
            "Last Name":  " ".join(parts[1:]) if len(parts) > 1 else "",
            "Mailing Address": r.get("mail_address",""),
            "Mailing City":    r.get("mail_city",""),
            "Mailing State":   r.get("mail_state",""),
            "Mailing Zip":     r.get("mail_zip",""),
            "Property Address": r.get("prop_address",""),
            "Property City":    r.get("prop_city",""),
            "Property State":   r.get("prop_state",""),
            "Property Zip":     r.get("prop_zip",""),
            "Lead Type":       r.get("cat_label",""),
            "Document Type":   r.get("doc_type",""),
            "Date Filed":      r.get("filed",""),
            "Document Number": r.get("doc_num",""),
            "Amount/Debt Owed": r.get("amount",""),
            "Seller Score":    r.get("score",0),
            "Motivated Seller Flags": " | ".join(r.get("flags",[])),
            "Source":          "Lubbock County Clerk",
            "Public Records URL": r.get("clerk_url",""),
        })
    return out.getvalue()

def main():
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    start_str  = start_dt.strftime("%m/%d/%Y")
    end_str    = end_dt.strftime("%m/%d/%Y")
    cutoff_iso = start_dt.strftime("%Y-%m-%d")

    log.info(f"Date range: {start_str} to {end_str}")

    session = build_session()

    # Load CAD address cache
    cad_cache = load_cad_cache()
    log.info(f"CAD cache loaded: {len(cad_cache)} entries")

    # Break date range into 14-day chunks to avoid portal limits
    matched_rows = []
    total_seen   = 0
    chunk_days   = 14
    chunk_start  = start_dt

    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(days=chunk_days), end_dt)
        cs = chunk_start.strftime("%m/%d/%Y")
        ce = chunk_end.strftime("%m/%d/%Y")
        log.info(f"Searching chunk: {cs} → {ce}")

        ok = post_search(session, cs, ce)
        if not ok:
            log.warning(f"Chunk {cs}-{ce} failed — skipping")
            chunk_start = chunk_end
            continue

        page = 1
        while True:
            log.info(f"  Fetching page {page}...")
            html = fetch_results_page(session, page)
            if not html:
                break

            rows, has_more = parse_results_page(html)
            log.info(f"    Page {page}: {len(rows)} rows, has_more={has_more}")

            if not rows:
                break

            total_seen += len(rows)
            for row in rows:
                dt = match_lead_type(row["description"], row.get("party_one",""), row.get("party_two",""))
                if dt:
                    row["doc_type"]  = dt
                    row["cat"]       = LEAD_TYPES[dt]["cat"]
                    row["cat_label"] = LEAD_TYPES[dt]["label"]
                    matched_rows.append(row)
                    log.info(f"    MATCH: {row['description']} [{dt}] node={row['node']}")
                else:
                    # Log ALL unmatched descriptions so we can see what we're missing
                    log.info(f"    SKIP:  {row['description']}")

            if not has_more:
                break
            page += 1
            if page > 50:
                break
            time.sleep(0.5)

        chunk_start = chunk_end
        time.sleep(1)  # pause between chunks

    # Deduplicate by node
    seen_nodes = set()
    deduped = []
    for row in matched_rows:
        key = row.get("node") or row.get("doc_num") or str(row)
        if key not in seen_nodes:
            seen_nodes.add(key)
            deduped.append(row)
    matched_rows = deduped

    log.info(f"Scanned {total_seen} rows total, {len(matched_rows)} matches (deduplicated)")

    enriched = []
    for i, row in enumerate(matched_rows):
        log.info(f"Detail {i+1}/{len(matched_rows)}: {row.get('node')}")
        try:
            detail = fetch_detail(session, row["node"]) if row.get("node") else {}
            rec = {**row, **detail}
            for k in ("prop_address","prop_city","prop_state","prop_zip",
                      "mail_address","mail_city","mail_state","mail_zip"):
                rec.setdefault(k, "")
            if not rec.get("owner"):   rec["owner"]   = rec.get("party_one","")
            if not rec.get("grantee"): rec["grantee"] = rec.get("party_two","")
            # CAD address enrichment — check cache first, then live lookup
            # Skip known unmatchable entities
            SKIP_NAMES = {"U S OF AMERICA", "TEXAS STATE OF", "UNITED STATES",
                          "IRS", "USA", "UNITED STATES OF AMERICA"}

            if not rec.get("prop_address"):
                cad_name = rec.get("grantee") or rec.get("owner") or ""
                cad = {}

                # Build list of names to try
                names_to_try = []
                if cad_name and cad_name.upper() not in SKIP_NAMES:
                    names_to_try.append(cad_name)
                owner = rec.get("owner","")
                if owner and owner != cad_name and owner.upper() not in SKIP_NAMES:
                    names_to_try.append(owner)

                for try_name in names_to_try:
                    # Check cache first
                    if try_name in cad_cache:
                        cached = cad_cache[try_name]
                        if cached:
                            cad = cached
                            log.info(f"  CAD cache hit: {try_name}")
                        else:
                            log.info(f"  CAD cache miss (known): {try_name}")
                        break

                    # Live lookup
                    log.info(f"  CAD lookup: {try_name}")
                    result = enrich_from_cad(session, try_name)
                    cad_cache[try_name] = result or None
                    time.sleep(2)
                    if result:
                        cad = result
                        break

                # Legal description fallback
                if not cad and rec.get("legal"):
                    legal = rec["legal"].upper().strip()
                    subdiv = re.split(
                        r"\s+(?:LOT|L\s+\d|L\d|BLK|BLOCK|BLCK|SEC|SECTION|UNIT|PHASE|LT\s+\d|LT\d)",
                        legal, maxsplit=1
                    )[0].strip()
                    subdiv = re.sub(r"[:\d]+$", "", subdiv).strip()
                    subdiv = re.sub(r"\s+", " ", subdiv).strip()
                    if len(subdiv) >= 4 and subdiv not in cad_cache:
                        log.info(f"  CAD legal fallback: {subdiv}")
                        cad = enrich_from_cad(session, subdiv)
                        cad_cache[subdiv] = cad or None
                        time.sleep(2)

                if cad:
                    rec.update(cad)
                    log.info(f"  CAD match: {cad.get('prop_address','')}")
            rec["score"], rec["flags"] = compute_score(rec, cutoff_iso)
            enriched.append(rec)
            time.sleep(0.3)
        except Exception as e:
            log.error(f"Error on {row.get('node')}: {e}\n{traceback.format_exc()}")

    # Save updated CAD cache
    save_cad_cache(cad_cache)
    log.info(f"CAD cache saved: {len(cad_cache)} entries")

    enriched.sort(key=lambda x: x.get("score",0), reverse=True)
    with_address = sum(1 for r in enriched if r.get("prop_address"))
    log.info(f"Final: {len(enriched)} records, {with_address} with address")

    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Lubbock County Clerk (Tyler Technologies EagleWeb)",
        "date_range":   {"start": cutoff_iso, "end": end_dt.strftime("%Y-%m-%d")},
        "total":        len(enriched),
        "with_address": with_address,
        "records":      enriched,
    }

    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"Saved -> {path}")

    csv_path = Path("data/ghl_export.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(export_ghl_csv(enriched))
    log.info(f"GHL CSV -> {csv_path} ({len(enriched)} rows)")
    log.info("Done.")

if __name__ == "__main__":
    main()
