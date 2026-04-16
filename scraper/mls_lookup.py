"""
mls_lookup.py
FlexMLS / Spark RESO OData integration for the Lubbock Intel scraper.

Endpoint: replication.sparkapi.com/Reso/OData
(Keys are restricted to the replication endpoint, not the standard sparkapi.com/v1)

Credential tiers (in priority order):
  1. Broker Back Office (BBO) — full access, all fields, private remarks
  2. VOW                      — extended fields, price history, DOM details
  3. IDX                      — public listings, basic fields

Each tier has two credentials:
  API Feed ID   → X-SparkApi-Access-Token header
  Access Token  → Authorization: Bearer header

Usage:
  from mls_lookup import MLSClient
  client = MLSClient()
  listings = client.fetch_area_listings()
  index    = client.build_address_index(listings)
"""

import os
import re
import time
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import requests

log = logging.getLogger("mls")

# RESO OData replication endpoint — required for these credential types
SPARK_API_BASE = "https://replication.sparkapi.com/Reso/OData"
MLS_CITY       = os.getenv("MLS_CITY", "Lubbock")
MLS_STATE      = os.getenv("MLS_STATE", "TX")
MLS_CACHE_PATH = Path("data/mls_cache.json")

_CRED_TIERS = [
    ("BBO", "Broker Back Office"),
    ("VOW", "Virtual Office Website"),
    ("IDX", "Internet Data Exchange"),
]


# ---------------------------------------------------------------------------
# Credential loading
# ---------------------------------------------------------------------------

def _load_credential_sets() -> list[dict]:
    sets = []
    for tier_key, tier_label in _CRED_TIERS:
        cred = {
            "tier":         tier_key,
            "tier_label":   tier_label,
            "api_feed_id":  os.getenv(f"SPARK_{tier_key}_API_FEED_ID", ""),
            "access_token": os.getenv(f"SPARK_{tier_key}_ACCESS_TOKEN", ""),
        }
        if cred["api_feed_id"] or cred["access_token"]:
            sets.append(cred)
    return sets


# ---------------------------------------------------------------------------
# MLS Client
# ---------------------------------------------------------------------------

class MLSClient:
    """
    Wrapper around the FlexMLS RESO OData replication API.

    Fetches property listings using OData query syntax, caches results
    to data/mls_cache.json, and provides address-based lead enrichment.
    """

    def __init__(self, auto_load_dotenv: bool = True):
        if auto_load_dotenv:
            self._load_dotenv()
        self.creds = _load_credential_sets()
        if not self.creds:
            raise RuntimeError(
                "No FlexMLS credentials found in environment variables. "
                "Ensure SPARK_BBO_API_FEED_ID and SPARK_BBO_ACCESS_TOKEN are set."
            )
        self.session = requests.Session()
        self.session.headers.update({
            "Accept":     "application/json",
            "User-Agent": "LubbockIntel/1.0",
        })
        self._active_cred: Optional[dict] = None
        self._area_cache:  Optional[dict] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_dotenv():
        """Load .env file from scraper/ directory if present."""
        env_path = Path(__file__).parent / ".env"
        if not env_path.exists():
            return
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and val and key not in os.environ:
                os.environ[key] = val

    def _set_auth_headers(self, cred: dict):
        """Apply both Spark auth headers for the given credential set."""
        self.session.headers["Authorization"]           = f"Bearer {cred.get('access_token', '')}"
        self.session.headers["X-SparkApi-Access-Token"] = cred.get("api_feed_id", "")

    def _try_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """
        Make an authenticated request, trying all credential tiers if needed.
        Handles 429 rate limits with backoff.
        """
        # Use cached working credential first
        if self._active_cred:
            self._set_auth_headers(self._active_cred)
            try:
                resp = self.session.request(method, url, timeout=30, **kwargs)
                if resp.status_code == 200:
                    return resp
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", "10"))
                    log.warning(f"Rate limited — waiting {wait}s")
                    time.sleep(wait)
                    return self.session.request(method, url, timeout=30, **kwargs)
                if resp.status_code not in (401, 403):
                    log.warning(f"HTTP {resp.status_code}: {resp.text[:300]}")
                    return resp
                # 401/403 — fall through to try other tiers
            except Exception as e:
                log.warning(f"Request error: {e}")

        # Try each tier in order: BBO → VOW → IDX
        for cred in self.creds:
            if not cred.get("access_token"):
                continue
            self._set_auth_headers(cred)
            try:
                resp = self.session.request(method, url, timeout=30, **kwargs)
                if resp.status_code == 200:
                    self._active_cred = cred
                    log.info(f"Authenticated via {cred['tier_label']}")
                    return resp
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", "10"))
                    log.warning(f"Rate limited — waiting {wait}s")
                    time.sleep(wait)
                    resp2 = self.session.request(method, url, timeout=30, **kwargs)
                    if resp2.status_code == 200:
                        self._active_cred = cred
                        return resp2
                log.warning(f"{cred['tier_label']} failed ({resp.status_code}) — trying next...")
            except Exception as e:
                log.warning(f"Request error ({cred['tier_label']}): {e}")

        log.error("All credential sets exhausted — could not authenticate.")
        return None

    def _odata_get(self, resource: str, params: dict) -> Optional[dict]:
        """Make a GET request to the RESO OData endpoint."""
        url  = f"{SPARK_API_BASE}/{resource}"
        resp = self._try_request("GET", url, params=params)
        if resp and resp.status_code == 200:
            try:
                return resp.json()
            except Exception as e:
                log.warning(f"JSON parse error: {e}")
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_area_listings(
        self,
        status: str = "Active",
        max_records: int = 5000,
        use_cache_hours: int = 6,
    ) -> list[dict]:
        """
        Fetch all listings in Lubbock, TX with the given status.

        Uses OData $filter syntax against the Property resource.
        Results cached to data/mls_cache.json for use_cache_hours hours.

        Args:
          status:          "Active", "Pending", "Closed", "ActiveUnderContract"
          max_records:     max listings to return
          use_cache_hours: how long disk cache is considered fresh

        Returns:
          List of normalized listing dicts with mls_* keys.
        """
        # In-memory cache check
        if self._area_cache and self._area_cache.get("status") == status:
            cached_at = datetime.fromisoformat(self._area_cache.get("cached_at", "2000-01-01"))
            if datetime.utcnow() - cached_at < timedelta(hours=use_cache_hours):
                log.info(f"MLS area cache hit ({len(self._area_cache['listings'])} listings)")
                return self._area_cache["listings"]

        # Disk cache check
        disk = self._load_disk_cache()
        if disk and disk.get("status") == status:
            cached_at = datetime.fromisoformat(disk.get("cached_at", "2000-01-01"))
            if datetime.utcnow() - cached_at < timedelta(hours=use_cache_hours):
                log.info(f"MLS disk cache hit ({len(disk['listings'])} listings)")
                self._area_cache = disk
                return disk["listings"]

        log.info(f"Fetching MLS {status} listings for {MLS_CITY}, {MLS_STATE}...")
        all_listings = []
        page_size    = 200   # OData replication endpoint — conservative page size
        skip         = 0
        next_link    = None

        while len(all_listings) < max_records:
            if next_link:
                # Follow OData nextLink directly
                resp = self._try_request("GET", next_link)
                data = resp.json() if resp and resp.status_code == 200 else None
            else:
                params = {
                    "$filter": (
                        f"StandardStatus eq '{status}' and "
                        f"City eq '{MLS_CITY}' and "
                        f"StateOrProvince eq '{MLS_STATE}'"
                    ),
                    "$top":    min(page_size, max_records - len(all_listings)),
                    "$skip":   skip,
                    "$select": (
                        "ListingKey,ListingId,StandardStatus,ListPrice,"
                        "DaysOnMarket,BedsTotal,BathroomsTotalInteger,LivingArea,"
                        "ListingContractDate,UnparsedAddress,StreetNumber,StreetName,"
                        "City,StateOrProvince,PostalCode,PublicRemarks"
                    ),
                }
                data = self._odata_get("Property", params)

            if not data:
                log.warning("No data returned from MLS — stopping pagination")
                break

            records = data.get("value", [])
            if not records:
                break

            for r in records:
                all_listings.append(self._normalize_listing(r))

            log.info(f"  Fetched {len(all_listings)} listings (skip={skip})")

            # OData pagination via nextLink
            next_link = data.get("@odata.nextLink")
            if not next_link:
                break

            skip += len(records)
            time.sleep(0.5)

        log.info(f"MLS fetch complete: {len(all_listings)} {status} listings")

        cache_payload = {
            "status":    status,
            "cached_at": datetime.utcnow().isoformat(),
            "city":      MLS_CITY,
            "state":     MLS_STATE,
            "listings":  all_listings,
        }
        self._area_cache = cache_payload
        self._save_disk_cache(cache_payload)
        return all_listings

    def build_address_index(self, listings: list[dict]) -> dict[str, dict]:
        """
        Build a normalized-address → listing dict for fast O(1) lookups.
        Call this once after fetch_area_listings(), then pass to enrich_lead().
        """
        index = {}
        for listing in listings:
            # Index by both parsed street and full unparsed address
            street = listing.get("street_address", "").strip()
            full   = listing.get("unparsed_address", "").strip()
            if street:
                index[self._normalize_addr_key(street)] = listing
            if full:
                index[self._normalize_addr_key(full)] = listing
        return index

    def enrich_lead(self, lead: dict, address_index: dict) -> dict:
        """
        Attach mls_* fields to a lead by matching its address against the index.
        """
        lead.setdefault("mls_found",          False)
        lead.setdefault("mls_status",         "")
        lead.setdefault("mls_list_price",     None)
        lead.setdefault("mls_days_on_market", None)
        lead.setdefault("mls_beds",           None)
        lead.setdefault("mls_baths",          None)
        lead.setdefault("mls_sqft",           None)
        lead.setdefault("mls_listing_date",   "")
        lead.setdefault("mls_listing_id",     "")
        lead.setdefault("mls_note",           "")

        raw_addr = lead.get("prop_address") or lead.get("situs_address") or ""
        if not raw_addr:
            return lead

        key   = self._normalize_addr_key(raw_addr)
        match = address_index.get(key)

        # Fuzzy fallback: house number + first street word
        if not match:
            parts = key.split()
            if len(parts) >= 2:
                prefix = " ".join(parts[:2])
                for idx_key, idx_val in address_index.items():
                    if idx_key.startswith(prefix):
                        match = idx_val
                        break

        if match:
            price = match.get("mls_list_price") or 0
            dom   = match.get("mls_days_on_market", "?")
            lead.update({
                "mls_found":          True,
                "mls_status":         match.get("mls_status", ""),
                "mls_list_price":     match.get("mls_list_price"),
                "mls_days_on_market": match.get("mls_days_on_market"),
                "mls_beds":           match.get("mls_beds"),
                "mls_baths":          match.get("mls_baths"),
                "mls_sqft":           match.get("mls_sqft"),
                "mls_listing_date":   match.get("mls_listing_date", ""),
                "mls_listing_id":     match.get("mls_listing_id", ""),
                "mls_note":           f"{match.get('mls_status','')} at ${price:,} ({dom} DOM)",
            })
            log.info(f"MLS match: {raw_addr} → {lead['mls_note']}")

        return lead

    # ------------------------------------------------------------------
    # Normalizers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_listing(r: dict) -> dict:
        """Map RESO OData Property fields to our internal schema."""
        street_num  = str(r.get("StreetNumber", "") or "").strip()
        street_name = str(r.get("StreetName",   "") or "").strip()
        street_addr = f"{street_num} {street_name}".strip()
        return {
            "mls_listing_id":     r.get("ListingKey") or r.get("ListingId", ""),
            "mls_status":         r.get("StandardStatus", ""),
            "mls_list_price":     r.get("ListPrice"),
            "mls_days_on_market": r.get("DaysOnMarket"),
            "mls_beds":           r.get("BedsTotal"),
            "mls_baths":          r.get("BathroomsTotalInteger"),
            "mls_sqft":           r.get("LivingArea"),
            "mls_listing_date":   r.get("ListingContractDate", ""),
            "mls_remarks":        (r.get("PublicRemarks") or "")[:300],
            "street_address":     street_addr,
            "unparsed_address":   r.get("UnparsedAddress", ""),
            "city":               r.get("City", ""),
            "state":              r.get("StateOrProvince", ""),
            "zip":                r.get("PostalCode", ""),
        }

    @staticmethod
    def _normalize_addr_key(addr: str) -> str:
        """Normalize an address string to a consistent lookup key."""
        addr = str(addr).upper()
        addr = addr.split(",")[0]                         # street portion only
        addr = re.sub(r"[^A-Z0-9 ]", " ", addr)
        addr = re.sub(r"\s+", " ", addr).strip()
        # Directionals
        for full, abbr in [("NORTH","N"),("SOUTH","S"),("EAST","E"),("WEST","W")]:
            addr = re.sub(rf"\b{full}\b", abbr, addr)
        # Street suffixes
        for full, abbr in [
            ("STREET","ST"),("AVENUE","AVE"),("BOULEVARD","BLVD"),
            ("DRIVE","DR"),("COURT","CT"),("PLACE","PL"),
            ("LANE","LN"),("ROAD","RD"),("CIRCLE","CIR"),
        ]:
            addr = re.sub(rf"\b{full}\b", abbr, addr)
        return addr.lower()

    # ------------------------------------------------------------------
    # Cache I/O
    # ------------------------------------------------------------------

    @staticmethod
    def _load_disk_cache() -> Optional[dict]:
        try:
            if MLS_CACHE_PATH.exists():
                return json.loads(MLS_CACHE_PATH.read_text())
        except Exception:
            pass
        return None

    @staticmethod
    def _save_disk_cache(payload: dict):
        try:
            MLS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            MLS_CACHE_PATH.write_text(json.dumps(payload, indent=2, default=str))
            log.info(f"MLS cache saved → {MLS_CACHE_PATH}")
        except Exception as e:
            log.warning(f"Could not save MLS cache: {e}")


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    client   = MLSClient()
    listings = client.fetch_area_listings(status="Active", max_records=50)
    print(f"\nFetched {len(listings)} active listings in {MLS_CITY}, {MLS_STATE}")
    for l in listings[:10]:
        price = l.get("mls_list_price") or 0
        print(f"  {l['unparsed_address']} — ${price:,} ({l['mls_status']})")
