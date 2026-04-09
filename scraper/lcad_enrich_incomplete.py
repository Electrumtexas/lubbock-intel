"""
lcad_enrich_incomplete.py
=========================
Second-pass enrichment for records missing required fields.
Hits lubbockcad.org/Property-Detail/PropertyQuickRefID/{R}/ directly
and parses the HTML for all missing fields.

Run after build_unified.py when incomplete records remain.
Reads:  dashboard/tax_delinquent.json, dashboard/fire_damage.json
Writes: dashboard/tax_delinquent.json, dashboard/fire_damage.json (in-place)
        dashboard/unified_leads.json (regenerated)
        data/lcad_detail_cache.json  (so we never hit the same R twice)

Usage:
    python scraper/lcad_enrich_incomplete.py
    python scraper/lcad_enrich_incomplete.py --limit 50   # process max 50 per run
    python scraper/lcad_enrich_incomplete.py --dry-run    # show what would be fetched
"""

import json, re, time, sys, argparse, logging
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────
DATA_DIR         = Path("data")
DASHBOARD_DIR    = Path("dashboard")
CACHE_PATH       = DATA_DIR / "lcad_detail_cache.json"
TAX_PATH         = DASHBOARD_DIR / "tax_delinquent.json"
FIRE_PATH        = DASHBOARD_DIR / "fire_damage.json"
UNIFIED_PATH     = DASHBOARD_DIR / "unified_leads.json"

BASE_URL = "https://lubbockcad.org/Property-Detail/PropertyQuickRefID/{r}/"
DELAY    = 2.5    # seconds between requests — be respectful
TIMEOUT  = 20

REQUIRED_FIELDS = [
    'owner_name',
    'situs_address', 'situs_city', 'situs_state', 'situs_zip',
    'mail_address',  'mail_city',  'mail_state',  'mail_zip',
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("lcad_enrich")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── Cache ─────────────────────────────────────────────────────────────────

def load_cache() -> dict:
    try:
        if CACHE_PATH.exists():
            return json.loads(CACHE_PATH.read_text())
    except Exception:
        pass
    return {}

def save_cache(cache: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, indent=2, default=str))


# ── LCAD Page Parser ──────────────────────────────────────────────────────

def fetch_lcad_detail(session: requests.Session, r_number: str) -> Optional[dict]:
    """
    Fetch and parse the LCAD property detail page for an R-number.
    Returns a dict with all found fields, or None on failure.
    """
    url = BASE_URL.format(r=r_number)
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 200:
                return parse_lcad_detail(resp.text, r_number)
            elif resp.status_code == 404:
                log.warning(f"  {r_number}: 404 — not found on LCAD")
                return None
            else:
                log.warning(f"  {r_number}: HTTP {resp.status_code}")
        except Exception as e:
            log.warning(f"  {r_number} attempt {attempt+1}: {e}")
        time.sleep(2 ** attempt)
    return None


def parse_lcad_detail(html: str, r_number: str) -> dict:
    """
    Parse LCAD property detail page HTML.
    Extracts: owner name, situs address, mailing address, and all sub-fields.
    """
    soup = BeautifulSoup(html, "lxml")
    result = {"r_number": r_number, "_source": "lcad_detail"}
    full_text = soup.get_text("\n")

    # ── Owner name ────────────────────────────────────────────────────────
    # DNN module: typically in a span/div with id containing "OwnerName" or label "Owner"
    for pattern in [
        r"Owner\s*\n\s*([A-Z][A-Z ,.'&\-]{2,80})",
        r"Owner Name\s*\n\s*([A-Z][A-Z ,.'&\-]{2,80})",
    ]:
        m = re.search(pattern, full_text)
        if m:
            result["owner_name"] = m.group(1).strip()
            break

    # Try table rows
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        label = cells[0].get_text(strip=True).upper()
        value = cells[1].get_text(strip=True)
        if not value:
            continue
        if "OWNER" in label and "owner_name" not in result:
            result["owner_name"] = value
        elif "SITUS" in label or ("PROPERTY" in label and "ADDRESS" in label):
            if "situs_address" not in result:
                _parse_addr_into(result, value, "situs")
        elif "MAILING" in label and "ADDRESS" in label:
            if "mail_address" not in result:
                _parse_addr_into(result, value, "mail")

    # ── Situs address from header area ────────────────────────────────────
    if "situs_address" not in result:
        # LCAD header shows "Property Address" prominently
        addr_el = (
            soup.find(id=re.compile(r"PropertyAddress", re.I)) or
            soup.find(class_=re.compile(r"property.?address", re.I)) or
            soup.find("td", string=re.compile(r"Property Address", re.I))
        )
        if addr_el:
            raw = addr_el.get_text(" ", strip=True)
            _parse_addr_into(result, raw, "situs")

    # ── Mailing address ───────────────────────────────────────────────────
    if "mail_address" not in result:
        mail_el = (
            soup.find(id=re.compile(r"MailingAddress|MailAddress", re.I)) or
            soup.find(id="dnn_ctr416_View_tdOIMailingAddress")
        )
        if mail_el:
            raw = mail_el.get_text(" ", strip=True)
            _parse_addr_into(result, raw, "mail")

    # ── Regex fallbacks on full text ──────────────────────────────────────
    if "situs_address" not in result:
        m = re.search(
            r"(?:Property Address|Situs Address)\s*[:\n]\s*"
            r"(\d+[^,\n]{3,50}),\s*([A-Z][A-Za-z\s]+),\s*(TX|tx)\s+(\d{5})",
            full_text, re.I
        )
        if m:
            result["situs_address"] = m.group(1).strip()
            result["situs_city"]    = m.group(2).strip()
            result["situs_state"]   = "TX"
            result["situs_zip"]     = m.group(4).strip()

    # ── Page title fallback (shows "R###### - OWNER NAME - ADDRESS") ──────
    title = soup.find("title")
    if title:
        t = title.get_text(strip=True)
        # Common pattern: "R314165 - JCI DIVERSIFIED HOLDINGS LLC - 9 TOMMY FISHER DR..."
        m = re.match(r"R\d+ - (.+?) - (.+)", t)
        if m:
            if "owner_name" not in result:
                result["owner_name"] = m.group(1).strip()

    log.info(f"  {r_number}: found {list(result.keys())}")
    return result


def _parse_addr_into(result: dict, raw: str, prefix: str):
    """
    Parse an address string into prefix_address, prefix_city, prefix_state, prefix_zip.
    Handles: '1115 32ND ST, LUBBOCK, TX  79411'
             '1115 32ND ST LUBBOCK TX 79411'
    """
    raw = raw.strip()
    # Try comma-delimited first
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) >= 3:
        result[f"{prefix}_address"] = parts[0]
        result[f"{prefix}_city"]    = parts[1]
        sz = parts[2].split()
        result[f"{prefix}_state"]   = sz[0] if sz else "TX"
        result[f"{prefix}_zip"]     = sz[1] if len(sz) > 1 else ""
        return
    # Try regex: NUMBER STREET CITY ST ZIP
    m = re.match(
        r"^(\d+\s+\S+(?:\s+\S+){0,5}?)\s+"
        r"([A-Z][A-Za-z\s]+?)\s+(TX|tx)\s+(\d{5})",
        raw.strip()
    )
    if m:
        result[f"{prefix}_address"] = m.group(1).strip()
        result[f"{prefix}_city"]    = m.group(2).strip()
        result[f"{prefix}_state"]   = "TX"
        result[f"{prefix}_zip"]     = m.group(4).strip()
        return
    # Last resort — just store the whole thing as address
    if raw and f"{prefix}_address" not in result:
        result[f"{prefix}_address"] = raw


# ── Missing fields checker ────────────────────────────────────────────────

def missing_fields(rec: dict) -> list:
    return [f for f in REQUIRED_FIELDS if not str(rec.get(f, "") or "").strip()]


def apply_lcad_result(rec: dict, lcad: dict) -> int:
    """
    Apply LCAD detail results to a record, filling only missing fields.
    Returns number of fields filled.
    """
    field_map = {
        "owner_name":    "owner_name",
        "situs_address": "situs_address",
        "situs_city":    "situs_city",
        "situs_state":   "situs_state",
        "situs_zip":     "situs_zip",
        "mail_address":  "mail_address",
        "mail_city":     "mail_city",
        "mail_state":    "mail_state",
        "mail_zip":      "mail_zip",
    }
    filled = 0
    for field, lcad_field in field_map.items():
        if not str(rec.get(field, "") or "").strip():
            val = str(lcad.get(lcad_field, "") or "").strip()
            if val:
                rec[field] = val
                filled += 1
    if filled > 0:
        rec["address_source"] = "lcad_detail"
        # Recompute completeness
        still_missing = missing_fields(rec)
        rec["is_complete"]    = len(still_missing) == 0
        rec["missing_fields"] = still_missing
    return filled


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="LCAD second-pass enrichment")
    parser.add_argument("--limit",   type=int, default=500,
                        help="Max records to process per run (default 500)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be fetched without fetching")
    args = parser.parse_args()

    cache = load_cache()
    log.info(f"Cache: {len(cache)} entries")

    # Load data files
    tax_list  = json.loads(TAX_PATH.read_text())  if TAX_PATH.exists()  else []
    fire_list = json.loads(FIRE_PATH.read_text()) if FIRE_PATH.exists() else []
    all_records = tax_list + fire_list

    # Find incomplete records with R-numbers we haven't tried yet
    to_enrich = []
    seen_r = set()
    for rec in all_records:
        r = str(rec.get("r_number", "") or "").strip()
        if not r or r in seen_r:
            continue
        if r in cache:
            # Already tried — apply cached result if useful
            if cache[r]:
                apply_lcad_result(rec, cache[r])
            continue
        if missing_fields(rec):
            to_enrich.append(rec)
            seen_r.add(r)

    log.info(f"Incomplete records needing LCAD fetch: {len(to_enrich)}")

    if args.dry_run:
        log.info("DRY RUN — would fetch:")
        for rec in to_enrich[:20]:
            log.info(f"  {rec['r_number']} — missing: {missing_fields(rec)}")
        if len(to_enrich) > 20:
            log.info(f"  ... and {len(to_enrich)-20} more")
        return

    # Process up to --limit records
    session = requests.Session()
    processed = 0
    filled_total = 0

    for rec in to_enrich[:args.limit]:
        r = rec["r_number"]
        log.info(f"[{processed+1}/{min(len(to_enrich),args.limit)}] Fetching {r} "
                 f"— missing: {missing_fields(rec)}")

        lcad = fetch_lcad_detail(session, r)
        cache[r] = lcad  # cache even None (means 404/failed)

        if lcad:
            filled = apply_lcad_result(rec, lcad)
            filled_total += filled
            log.info(f"  → Filled {filled} fields. Still missing: {missing_fields(rec)}")

            # Apply to the OTHER dataset if the same R-number appears there
            # (e.g. a fire record that's also in tax)
            for other in all_records:
                if other.get("r_number") == r and other is not rec:
                    apply_lcad_result(other, lcad)
        else:
            log.info(f"  → No data found")

        processed += 1
        time.sleep(DELAY)

    log.info(f"\nDone: {processed} records processed, {filled_total} fields filled")

    # Save everything
    save_cache(cache)
    log.info(f"Cache saved: {len(cache)} entries")

    TAX_PATH.write_text(json.dumps(tax_list, indent=2, default=str))
    FIRE_PATH.write_text(json.dumps(fire_list, indent=2, default=str))
    log.info(f"Tax: {len(tax_list)} | Fire: {len(fire_list)}")

    # Rebuild completeness stats
    now_complete   = sum(1 for r in all_records if r.get("is_complete"))
    still_missing  = sum(1 for r in all_records if not r.get("is_complete"))
    log.info(f"Completeness: {now_complete} complete, {still_missing} still incomplete")

    # Note: run build_unified.py after this to regenerate unified_leads.json


if __name__ == "__main__":
    main()
