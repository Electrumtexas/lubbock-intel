"""
Lubbock County Motivated Seller Lead Scraper
Fetches: Lis Pendens, Foreclosure, Tax Deed, Judgments, Liens, Probate, NOC
Sources: erecord.lubbockcounty.gov + lubbockcad.org bulk parcel data
"""

import asyncio
import json
import re
import csv
import io
import os
import sys
import time
import zipfile
import logging
import tempfile
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── Optional dbfread ──────────────────────────────────────────────────────────
try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False
    logging.warning("dbfread not installed – parcel lookup disabled")

# ── Optional Playwright ───────────────────────────────────────────────────────
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    logging.warning("playwright not installed – falling back to requests")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_URL = "https://erecord.lubbockcounty.gov/recorder/eagleweb/docSearch.jsp"
CAD_BASE  = "https://lubbockcad.org"

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]

LEAD_TYPES = {
    "LP":      {"label": "Lis Pendens",              "cat": "foreclosure"},
    "NOFC":    {"label": "Notice of Foreclosure",    "cat": "foreclosure"},
    "TAXDEED": {"label": "Tax Deed",                 "cat": "tax"},
    "JUD":     {"label": "Judgment",                 "cat": "judgment"},
    "CCJ":     {"label": "Certified Judgment",       "cat": "judgment"},
    "DRJUD":   {"label": "Domestic Judgment",        "cat": "judgment"},
    "LNCORPTX":{"label": "Corp Tax Lien",            "cat": "lien"},
    "LNIRS":   {"label": "IRS Lien",                 "cat": "lien"},
    "LNFED":   {"label": "Federal Lien",             "cat": "lien"},
    "LN":      {"label": "Lien",                     "cat": "lien"},
    "LNMECH":  {"label": "Mechanic Lien",            "cat": "lien"},
    "LNHOA":   {"label": "HOA Lien",                 "cat": "lien"},
    "MEDLN":   {"label": "Medicaid Lien",            "cat": "lien"},
    "PRO":     {"label": "Probate",                  "cat": "probate"},
    "NOC":     {"label": "Notice of Commencement",   "cat": "other"},
    "RELLP":   {"label": "Release Lis Pendens",      "cat": "release"},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")


# ─────────────────────────────────────────────────────────────────────────────
# PARCEL LOOKUP  (Lubbock CAD bulk download)
# ─────────────────────────────────────────────────────────────────────────────

class ParcelLookup:
    """Downloads the Lubbock CAD bulk parcel DBF and builds owner→address map."""

    OWNER_COLS   = ["OWNER", "OWN1", "OWNERNAME"]
    SITE_COLS    = ["SITE_ADDR", "SITEADDR", "SITEADDRESS"]
    SCITY_COLS   = ["SITE_CITY", "SITECITY"]
    SZIP_COLS    = ["SITE_ZIP",  "SITEZIP"]
    MAIL1_COLS   = ["ADDR_1", "MAILADR1", "MAILADDR1", "MAILADDRESS"]
    MCITY_COLS   = ["CITY", "MAILCITY"]
    MSTATE_COLS  = ["STATE", "MAILSTATE"]
    MZIP_COLS    = ["ZIP", "MAILZIP"]

    def __init__(self):
        self._map: dict[str, dict] = {}   # normalised_name → record

    # ── helpers ──────────────────────────────────────────────────────────────
    @staticmethod
    def _get(row: dict, cols: list[str], default="") -> str:
        for c in cols:
            v = row.get(c) or row.get(c.lower()) or ""
            if v:
                return str(v).strip()
        return default

    @staticmethod
    def _norm(name: str) -> str:
        return re.sub(r"\s+", " ", name.upper().strip())

    def _name_variants(self, raw: str) -> list[str]:
        n = self._norm(raw)
        parts = n.replace(",", " ").split()
        variants = [n]
        if len(parts) >= 2:
            variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
            variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
        return variants

    # ── download ──────────────────────────────────────────────────────────────
    def _try_download(self, session: requests.Session) -> Optional[bytes]:
        """Try several known CAD download endpoints; return raw bytes or None."""
        candidates = [
            f"{CAD_BASE}/wp-content/uploads/data/parcel.zip",
            f"{CAD_BASE}/downloads/parcel.zip",
            f"{CAD_BASE}/data/parcel.zip",
            f"{CAD_BASE}/download",
        ]
        headers = {"User-Agent": "Mozilla/5.0 (motivated-seller-scraper/1.0)"}

        # First try: scrape the CAD site for a download link
        try:
            r = session.get(CAD_BASE, headers=headers, timeout=30)
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"\.(dbf|zip)$", href, re.I) and "parcel" in href.lower():
                    url = href if href.startswith("http") else CAD_BASE + href
                    candidates.insert(0, url)
        except Exception:
            pass

        for url in candidates:
            for attempt in range(3):
                try:
                    log.info(f"Trying parcel download: {url} (attempt {attempt+1})")
                    r = session.get(url, headers=headers, timeout=60, stream=True)
                    if r.status_code == 200 and len(r.content) > 1000:
                        log.info(f"Downloaded {len(r.content):,} bytes from {url}")
                        return r.content
                except Exception as e:
                    log.warning(f"  failed: {e}")
                    time.sleep(2 ** attempt)

        return None

    def load(self):
        if not HAS_DBF:
            log.warning("dbfread unavailable – skipping parcel load")
            return
        session = requests.Session()
        raw = self._try_download(session)
        if not raw:
            log.warning("Could not download parcel file – address enrichment skipped")
            return
        self._parse(raw)
        log.info(f"Parcel lookup loaded: {len(self._map):,} entries")

    def _parse(self, raw: bytes):
        try:
            with tempfile.TemporaryDirectory() as tmp:
                # If it's a zip, extract
                if raw[:2] == b"PK":
                    zpath = Path(tmp) / "parcel.zip"
                    zpath.write_bytes(raw)
                    with zipfile.ZipFile(zpath) as zf:
                        dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                        if not dbf_names:
                            log.warning("No DBF inside zip")
                            return
                        zf.extract(dbf_names[0], tmp)
                        dbf_path = Path(tmp) / dbf_names[0]
                else:
                    dbf_path = Path(tmp) / "parcel.dbf"
                    dbf_path.write_bytes(raw)

                for rec in DBF(str(dbf_path), encoding="utf-8", char_detect=True):
                    row = {k.upper(): v for k, v in dict(rec).items()}
                    owner = self._get(row, self.OWNER_COLS)
                    if not owner:
                        continue
                    entry = {
                        "prop_address": self._get(row, self.SITE_COLS),
                        "prop_city":    self._get(row, self.SCITY_COLS),
                        "prop_state":   "TX",
                        "prop_zip":     self._get(row, self.SZIP_COLS),
                        "mail_address": self._get(row, self.MAIL1_COLS),
                        "mail_city":    self._get(row, self.MCITY_COLS),
                        "mail_state":   self._get(row, self.MSTATE_COLS),
                        "mail_zip":     self._get(row, self.MZIP_COLS),
                    }
                    for variant in self._name_variants(owner):
                        self._map.setdefault(variant, entry)
        except Exception as e:
            log.error(f"Parcel parse error: {e}")

    def lookup(self, owner: str) -> dict:
        if not owner:
            return {}
        for variant in self._name_variants(owner):
            result = self._map.get(variant)
            if result:
                return result
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# CLERK PORTAL SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

class ClerkScraper:

    BASE = "https://erecord.lubbockcounty.gov"
    SEARCH = "/recorder/eagleweb/docSearch.jsp"

    def __init__(self, start_date: str, end_date: str):
        self.start = start_date   # MM/DD/YYYY
        self.end   = end_date
        self.records: list[dict] = []

    # ── Playwright path ───────────────────────────────────────────────────────
    async def _playwright_search(self, doc_type: str) -> list[dict]:
        results = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
await page.goto(self.BASE + self.SEARCH, timeout=30000)
await page.wait_for_load_state("networkidle", timeout=15000)

# Click acknowledgment button if present
try:
    ack = await page.query_selector("input[value='I Acknowledge'], button:has-text('I Acknowledge'), a:has-text('I Acknowledge')")
    if ack:
        await ack.click()
        await page.wait_for_load_state("networkidle", timeout=15000)
        log.info("Clicked I Acknowledge button")
except Exception as e:
    log.warning(f"Acknowledge button not found or already past it: {e}")

                # Fill date range
                for sel, val in [
                    ("input[name='RecordingDateIDStart']", self.start),
                    ("input[name='RecordingDateIDEnd']",   self.end),
                ]:
                    try:
                        await page.fill(sel, val)
                    except Exception:
                        pass

                # Select doc type if dropdown exists
                try:
                    await page.select_option("select[name='DocTypeID']", doc_type)
                except Exception:
                    try:
                        await page.fill("input[name='DocTypeID']", doc_type)
                    except Exception:
                        pass

                # Submit
                try:
                    await page.click("input[type='submit'], button[type='submit']", timeout=5000)
                except Exception:
                    await page.keyboard.press("Enter")

                await page.wait_for_load_state("networkidle", timeout=20000)
                html = await page.content()
                results = self._parse_results(html, doc_type)

                # Pagination
                page_num = 2
                while True:
                    next_btn = await page.query_selector("a:text('Next'), a:text('>'), a[href*='page=" + str(page_num) + "']")
                    if not next_btn:
                        break
                    await next_btn.click()
                    await page.wait_for_load_state("networkidle", timeout=20000)
                    html = await page.content()
                    new = self._parse_results(html, doc_type)
                    if not new:
                        break
                    results.extend(new)
                    page_num += 1
                    if page_num > 20:
                        break

            except PWTimeout:
                log.warning(f"Timeout scraping {doc_type}")
            except Exception as e:
                log.warning(f"Playwright error for {doc_type}: {e}")
            finally:
                await browser.close()
        return results

    # ── Requests fallback ─────────────────────────────────────────────────────
    def _requests_search(self, doc_type: str) -> list[dict]:
        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (motivated-seller-scraper/1.0)",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        results = []
        page = 1
        while True:
            payload = {
                "RecordingDateIDStart": self.start,
                "RecordingDateIDEnd":   self.end,
                "DocTypeID":            doc_type,
                "searchType":           "DocType",
                "page":                 str(page),
            }
            for attempt in range(3):
                try:
                    r = session.post(
                        self.BASE + self.SEARCH,
                        data=payload,
                        headers=headers,
                        timeout=30,
                    )
                    if r.status_code == 200:
                        new = self._parse_results(r.text, doc_type)
                        results.extend(new)
                        if len(new) < 10:   # last page heuristic
                            return results
                        page += 1
                        if page > 20:
                            return results
                        break
                    else:
                        log.warning(f"HTTP {r.status_code} for {doc_type} p{page}")
                        break
                except Exception as e:
                    log.warning(f"Request error {doc_type} p{page} attempt {attempt+1}: {e}")
                    time.sleep(2 ** attempt)
            else:
                break
        return results

    # ── HTML parser ───────────────────────────────────────────────────────────
    def _parse_results(self, html: str, doc_type: str) -> list[dict]:
        """
        Parse the EagleWeb results table. Column order may vary; we detect headers.
        """
        soup = BeautifulSoup(html, "lxml")
        records = []

        # Find result table – EagleWeb uses <table> with class or id containing 'result'
        table = (
            soup.find("table", id=re.compile(r"result", re.I))
            or soup.find("table", class_=re.compile(r"result", re.I))
            or soup.find("table", id=re.compile(r"search", re.I))
        )
        if not table:
            # fallback: largest table on page
            tables = soup.find_all("table")
            if not tables:
                return []
            table = max(tables, key=lambda t: len(t.find_all("tr")))

        rows = table.find_all("tr")
        if not rows:
            return []

        # Detect header row
        headers = []
        header_row_idx = 0
        for i, row in enumerate(rows[:5]):
            cells = [c.get_text(strip=True).upper() for c in row.find_all(["th", "td"])]
            if any(k in " ".join(cells) for k in ("DOC", "DATE", "GRANTOR", "GRANTEE", "TYPE")):
                headers = cells
                header_row_idx = i
                break

        col = {
            "doc_num":  self._find_col(headers, ["DOCUMENT NUMBER", "DOC NUM", "DOC#", "DOCNUM", "INSTRUMENT"]),
            "doc_type": self._find_col(headers, ["DOC TYPE", "DOCTYPE", "TYPE"]),
            "filed":    self._find_col(headers, ["RECORDING DATE", "RECORDED", "DATE FILED", "DATE"]),
            "grantor":  self._find_col(headers, ["GRANTOR", "OWNER", "FROM"]),
            "grantee":  self._find_col(headers, ["GRANTEE", "TO"]),
            "legal":    self._find_col(headers, ["LEGAL", "DESCRIPTION", "LEGAL DESCRIPTION"]),
            "amount":   self._find_col(headers, ["AMOUNT", "CONSIDERATION", "DOLLAR"]),
        }

        for row in rows[header_row_idx + 1:]:
            cells = row.find_all("td")
            if not cells or len(cells) < 2:
                continue
            texts = [c.get_text(strip=True) for c in cells]

            def g(key):
                idx = col.get(key)
                if idx is not None and idx < len(texts):
                    return texts[idx]
                return ""

            # Try to pull a doc link
            link = ""
            for a in row.find_all("a", href=True):
                href = a["href"]
                if any(k in href.lower() for k in ("instrument", "docid", "doc", "detail")):
                    link = href if href.startswith("http") else self.BASE + href
                    break

            rec = {
                "doc_num":  g("doc_num"),
                "doc_type": g("doc_type") or doc_type,
                "filed":    self._normalize_date(g("filed")),
                "owner":    g("grantor"),
                "grantee":  g("grantee"),
                "legal":    g("legal"),
                "amount":   self._parse_amount(g("amount")),
                "clerk_url": link,
                "cat":      LEAD_TYPES.get(doc_type, {}).get("cat", "other"),
                "cat_label": LEAD_TYPES.get(doc_type, {}).get("label", doc_type),
            }

            if rec["doc_num"] or rec["filed"]:
                records.append(rec)

        return records

    @staticmethod
    def _find_col(headers: list[str], candidates: list[str]) -> Optional[int]:
        for cand in candidates:
            for i, h in enumerate(headers):
                if cand in h:
                    return i
        return None

    @staticmethod
    def _normalize_date(raw: str) -> str:
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
        return raw.strip()

    @staticmethod
    def _parse_amount(raw: str) -> Optional[float]:
        s = re.sub(r"[^\d.]", "", raw)
        try:
            return float(s) if s else None
        except ValueError:
            return None

    # ── Public interface ──────────────────────────────────────────────────────
    async def run(self) -> list[dict]:
        all_records = []
        for doc_type in LEAD_TYPES:
            log.info(f"Scraping {doc_type} ({LEAD_TYPES[doc_type]['label']}) …")
            try:
                if HAS_PLAYWRIGHT:
                    recs = await self._playwright_search(doc_type)
                else:
                    recs = self._requests_search(doc_type)
                log.info(f"  → {len(recs)} records")
                all_records.extend(recs)
            except Exception as e:
                log.error(f"Failed {doc_type}: {e}\n{traceback.format_exc()}")
        return all_records


# ─────────────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────────────

def compute_score(rec: dict, cutoff_date: str) -> tuple[int, list[str]]:
    flags = []
    score = 30  # base

    cat      = rec.get("cat", "")
    cat_label = rec.get("cat_label", "")
    doc_type  = rec.get("doc_type", "")
    amount    = rec.get("amount") or 0
    owner     = rec.get("owner", "")
    filed     = rec.get("filed", "")

    # Type-based flags
    if doc_type in ("LP",):
        flags.append("Lis pendens")
    if doc_type in ("NOFC", "LP"):
        flags.append("Pre-foreclosure")
    if doc_type in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
    if doc_type in ("TAXDEED", "LNCORPTX", "LNIRS", "LNFED"):
        flags.append("Tax lien")
    if doc_type == "LNMECH":
        flags.append("Mechanic lien")
    if doc_type == "PRO":
        flags.append("Probate / estate")
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|\bL\.L\.C\b", owner.upper()):
        flags.append("LLC / corp owner")

    # Score per flag
    score += len(flags) * 10

    # Combo bonus: LP + foreclosure
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20

    # Amount bonuses
    if amount and amount > 100_000:
        score += 15
    elif amount and amount > 50_000:
        score += 10

    # New this week
    try:
        filed_dt = datetime.strptime(filed[:10], "%Y-%m-%d")
        cutoff_dt = datetime.strptime(cutoff_date, "%Y-%m-%d")
        if filed_dt >= cutoff_dt:
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    # Has address
    if rec.get("prop_address"):
        score += 5

    return min(score, 100), list(dict.fromkeys(flags))   # dedupe, cap at 100


# ─────────────────────────────────────────────────────────────────────────────
# GHL CSV EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def split_name(full: str) -> tuple[str, str]:
    parts = full.strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def export_ghl_csv(records: list[dict]) -> str:
    out = io.StringIO()
    fieldnames = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    w = csv.DictWriter(out, fieldnames=fieldnames)
    w.writeheader()
    for r in records:
        first, last = split_name(r.get("owner", ""))
        w.writerow({
            "First Name":            first,
            "Last Name":             last,
            "Mailing Address":       r.get("mail_address", ""),
            "Mailing City":          r.get("mail_city", ""),
            "Mailing State":         r.get("mail_state", ""),
            "Mailing Zip":           r.get("mail_zip", ""),
            "Property Address":      r.get("prop_address", ""),
            "Property City":         r.get("prop_city", ""),
            "Property State":        r.get("prop_state", ""),
            "Property Zip":          r.get("prop_zip", ""),
            "Lead Type":             r.get("cat_label", ""),
            "Document Type":         r.get("doc_type", ""),
            "Date Filed":            r.get("filed", ""),
            "Document Number":       r.get("doc_num", ""),
            "Amount/Debt Owed":      r.get("amount", ""),
            "Seller Score":          r.get("score", 0),
            "Motivated Seller Flags": " | ".join(r.get("flags", [])),
            "Source":                "Lubbock County Clerk",
            "Public Records URL":    r.get("clerk_url", ""),
        })
    return out.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)

    start_str   = start_dt.strftime("%m/%d/%Y")
    end_str     = end_dt.strftime("%m/%d/%Y")
    cutoff_iso  = start_dt.strftime("%Y-%m-%d")

    log.info(f"Date range: {start_str} → {end_str}")

    # 1. Load parcel data
    parcel = ParcelLookup()
    parcel.load()

    # 2. Scrape clerk portal
    scraper = ClerkScraper(start_str, end_str)
    raw_records = await scraper.run()
    log.info(f"Total raw records: {len(raw_records)}")

    # 3. Enrich + deduplicate
    seen = set()
    enriched = []
    for r in raw_records:
        try:
            key = (r.get("doc_num", ""), r.get("doc_type", ""), r.get("filed", ""))
            if key in seen:
                continue
            seen.add(key)

            # Parcel lookup
            addr_data = parcel.lookup(r.get("owner", ""))
            r.update(addr_data)

            # Ensure all keys exist
            for k in ("prop_address", "prop_city", "prop_state", "prop_zip",
                      "mail_address", "mail_city", "mail_state", "mail_zip"):
                r.setdefault(k, "")

            # Score
            r["score"], r["flags"] = compute_score(r, cutoff_iso)
            enriched.append(r)
        except Exception as e:
            log.error(f"Enrichment error: {e}")

    enriched.sort(key=lambda x: x.get("score", 0), reverse=True)

    with_address = sum(1 for r in enriched if r.get("prop_address"))
    log.info(f"Enriched: {len(enriched)} records, {with_address} with address")

    # 4. Build output payload
    payload = {
        "fetched_at":    datetime.utcnow().isoformat() + "Z",
        "source":        "Lubbock County Clerk / Lubbock CAD",
        "date_range":    {"start": cutoff_iso, "end": end_dt.strftime("%Y-%m-%d")},
        "total":         len(enriched),
        "with_address":  with_address,
        "records":       enriched,
    }

    # 5. Save JSON
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"Saved → {path}")

    # 6. GHL CSV export
    csv_path = Path("data/ghl_export.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(export_ghl_csv(enriched))
    log.info(f"GHL CSV → {csv_path}")

    log.info("Done.")
    return payload


if __name__ == "__main__":
    asyncio.run(main())
