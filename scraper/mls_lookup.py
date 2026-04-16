"""
mls_lookup.py
FlexMLS / Spark API integration for the Lubbock Intel scraper.

Credential tiers (in priority order for enrichment):
  1. Broker Back Office (BBO) — full access, all fields, private remarks
  2. VOW                      — extended fields, price history, DOM details
  3. IDX                      — public listings, basic fields

Provides two main capabilities:
  1. look_up_address(address)  — check if a specific property is on MLS
  2. fetch_area_listings()     — pull all active listings in Lubbock, TX

Authentication:
  Each tier has two credentials: API Feed ID and Access Token.
  Tries BBO → VOW → IDX in order; uses the first that authenticates.
  API Feed ID goes in the X-SparkApi-Access-Token header.
  Access Token goes in the Authorization: Bearer header.

Usage:
  from mls_lookup import MLSClient
  client = MLSClient()
  listing = client.look_up_address("3006 56th St, Lubbock, TX 79413")
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

SPARK_API_BASE   = os.getenv("SPARK_API_BASE", "https://sparkapi.com/v1")
SPARK_TOKEN_URL  = "https://sparkplatform.com/oauth2/grant"
MLS_CITY         = os.getenv("MLS_CITY", "Lubbock")
MLS_STATE        = os.getenv("MLS_STATE", "TX")
MLS_CACHE_PATH   = Path("data/mls_cache.json")

# Credential tiers in priority order — BBO has the most complete data
_CRED_TIERS = [
    ("BBO", "Broker Back Office"),
    ("VOW", "Virtual Office Website"),
    ("IDX", "Internet Data Exchange"),
]

# ---------------------------------------------------------------------------
# Credential loading
# ---------------------------------------------------------------------------

def _load_credential_sets() -> list[dict]:
    """
    Load credentials for BBO, VOW, and IDX tiers from environment variables.
    Each tier has two values: API Feed ID and Access Token.
    Returns a list of non-empty dicts in priority order: BBO → VOW → IDX.
    """
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
            log.debug(f"Loaded {tier_label} credentials")
    return sets


# ---------------------------------------------------------------------------
# Token refresh
# ---------------------------------------------------------------------------

def _refresh_access_token(cred: dict) -> Optional[str]:
    """
    Use the refresh token to get a new access token via Spark OAuth.
    Returns the new access token string, or None on failure.
    """
    if not cred.get("refresh_token") or not cred.get("client_id") or not cred.get("client_secret"):
        return None
    try:
        resp = requests.post(
            SPARK_TOKEN_URL,
            data={
                "grant_type":    "refresh_token",
                "client_id":     cred["client_id"],
                "client_secret": cred["client_secret"],
                "refresh_token": cred["refresh_token"],
            },
            timeout=20,
        )
        if resp.status_code == 200:
            data = resp.json()
            new_token = data.get("access_token")
            log.info(f"Token refreshed successfully (expires_in={data.get('expires_in')}s)")
            return new_token
        else:
            log.warning(f"Token refresh failed: {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        log.warning(f"Token refresh error: {e}")
    return None


# ---------------------------------------------------------------------------
# MLS Client
# ---------------------------------------------------------------------------

class MLSClient:
    """
    Thin wrapper around the Spark/FlexMLS REST API.

    Automatically:
     - Tries all 3 credential sets to find a working token.
     - Refreshes expired tokens using client_id + client_secret + refresh_token.
     - Caches area listing results to data/mls_cache.json to avoid hammering the API.
    """

    def __init__(self, auto_load_dotenv: bool = True):
        if auto_load_dotenv:
            self._load_dotenv()
        self.creds = _load_credential_sets()
        if not self.creds:
            raise RuntimeError(
                "No FlexMLS credentials found. "
                "Copy scraper/.env.example to scraper/.env and fill in your keys."
            )
        self.session = requests.Session()
        self.session.headers.update({
            "Accept":       "application/json",
            "Content-Type": "application/json",
            "User-Agent":   "LubbockIntel/1.0",
        })
        self._active_cred: Optional[dict] = None
        self._token_expiry: Optional[datetime] = None
        self._area_cache: Optional[dict] = None  # in-memory cache for this run

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_dotenv():
        """Minimal .env loader — avoids requiring python-dotenv."""
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
        """Set both required Spark auth headers for a credential set."""
        self.session.headers["Authorization"]          = f"Bearer {cred.get('access_token', '')}"
        self.session.headers["X-SparkApi-Access-Token"] = cred.get("api_feed_id", "")

    def _try_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """
        Make an authenticated request, trying each credential set if needed.
        Handles 401 (expired token → refresh) and 429 (rate limit → backoff).
        """
        # If we already have a working credential, try it first
        if self._active_cred and self._active_cred.get("access_token"):
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
                if resp.status_code != 401:
                    log.warning(f"HTTP {resp.status_code}: {resp.text[:200]}")
                    return resp
            except Exception as e:
                log.warning(f"Request error: {e}")

        # Try each credential set in order: BBO → VOW → IDX
        for cred in self.creds:
            if not cred.get("access_token"):
                continue

            self._set_auth_headers(cred)
            try:
                resp = self.session.request(method, url, timeout=30, **kwargs)
                if resp.status_code == 200:
                    self._active_cred = cred
                    log.info(f"Authenticated via {cred.get('tier_label', 'unknown')} credentials")
                    return resp
                if resp.status_code == 401:
                    log.warning(f"{cred.get('tier_label','Credential set')} failed (401) — trying next tier...")
                    continue
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", "10"))
                    log.warning(f"Rate limited — waiting {wait}s")
                    time.sleep(wait)
                    resp2 = self.session.request(method, url, timeout=30, **kwargs)
                    if resp2.status_code == 200:
                        self._active_cred = cred
                        return resp2
                log.warning(f"HTTP {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                log.warning(f"Request error with cred set: {e}")

        log.error("All credential sets exhausted — could not authenticate.")
        return None

    def _get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        url = f"{SPARK_API_BASE}{endpoint}"
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

    def look_up_address(self, address: str) -> Optional[dict]:
        """
        Search MLS for a specific property address.

        Returns a dict with listing details if found (active or recently sold),
        or None if not found.

        Example return value:
          {
            "ListingId": "...",
            "ListPrice": 175000,
            "StandardStatus": "Active",
            "ListingContractDate": "2026-03-01",
            "DaysOnMarket": 14,
            "BedsTotal": 3,
            "BathroomsTotalInteger": 2,
            "LivingArea": 1450,
            "UnparsedAddress": "3006 56TH ST, LUBBOCK, TX 79413",
            "mls_status": "Active",
            "mls_list_price": 175000,
            "mls_days_on_market": 14,
          }
        """
        # Normalize: strip city/state/zip for the filter, keep street
        street = self._extract_street(address)
        if not street:
            log.warning(f"Could not parse street from: {address}")
            return None

        params = {
            "_filter": (
                f"StandardStatus Eq 'Active' And "
                f"UnparsedAddress Eq '{street}' And "
                f"City Eq '{MLS_CITY}' And "
                f"StateOrProvince Eq '{MLS_STATE}'"
            ),
            "$top": 5,
        }
        log.info(f"MLS address lookup: {street}")
        data = self._get("/listings", params=params)

        if not data:
            return None

        results = data.get("D", {}).get("Results", [])
        if not results:
            # Try a looser match — partial street address
            params["_filter"] = (
                f"UnparsedAddress Eq '{street}' And "
                f"City Eq '{MLS_CITY}'"
            )
            data = self._get("/listings", params=params)
            results = (data or {}).get("D", {}).get("Results", [])

        if not results:
            log.info(f"No MLS listing found for: {street}")
            return None

        listing = results[0].get("StandardFields", results[0])
        return self._normalize_listing(listing)

    def fetch_area_listings(
        self,
        status: str = "Active",
        max_records: int = 5000,
        use_cache_hours: int = 6,
    ) -> list[dict]:
        """
        Fetch all listings in the configured city/state.

        Results are cached to data/mls_cache.json for `use_cache_hours` hours
        so repeated runs don't hammer the API.

        Args:
          status:           "Active", "Pending", "Closed", or "ActiveUnderContract"
          max_records:      cap on records returned (Spark max per page is 1000)
          use_cache_hours:  how long the cache is considered fresh

        Returns:
          List of normalized listing dicts.
        """
        # Check in-memory cache first
        if self._area_cache and self._area_cache.get("status") == status:
            cached_at = datetime.fromisoformat(self._area_cache.get("cached_at", "2000-01-01"))
            if datetime.utcnow() - cached_at < timedelta(hours=use_cache_hours):
                log.info(f"MLS area cache hit ({len(self._area_cache['listings'])} listings)")
                return self._area_cache["listings"]

        # Check disk cache
        disk = self._load_disk_cache()
        if disk and disk.get("status") == status:
            cached_at = datetime.fromisoformat(disk.get("cached_at", "2000-01-01"))
            if datetime.utcnow() - cached_at < timedelta(hours=use_cache_hours):
                log.info(f"MLS disk cache hit ({len(disk['listings'])} listings)")
                self._area_cache = disk
                return disk["listings"]

        log.info(f"Fetching MLS {status} listings for {MLS_CITY}, {MLS_STATE}...")
        all_listings = []
        page_size = 1000
        skip = 0

        while len(all_listings) < max_records:
            params = {
                "_filter": (
                    f"StandardStatus Eq '{status}' And "
                    f"City Eq '{MLS_CITY}' And "
                    f"StateOrProvince Eq '{MLS_STATE}'"
                ),
                "$top":  min(page_size, max_records - len(all_listings)),
                "$skip": skip,
            }
            data = self._get("/listings", params=params)
            if not data:
                log.warning("No data returned from MLS — stopping pagination")
                break

            results = data.get("D", {}).get("Results", [])
            if not results:
                break

            for r in results:
                fields = r.get("StandardFields", r)
                all_listings.append(self._normalize_listing(fields))

            log.info(f"  Fetched {len(all_listings)} listings so far (skip={skip})")

            # Check if there are more pages
            pagination = data.get("D", {}).get("Pagination", {})
            total = pagination.get("TotalRows", 0)
            if len(all_listings) >= total or len(results) < page_size:
                break

            skip += page_size
            time.sleep(0.3)  # polite pause

        log.info(f"MLS fetch complete: {len(all_listings)} {status} listings")

        # Save cache
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
        Build a dict keyed by normalized street address for fast lookups.
        Use alongside fetch_area_listings() to cross-reference leads without
        making one API call per lead.

        Example:
          idx = client.build_address_index(client.fetch_area_listings())
          match = idx.get(normalize_address("3006 56th St"))
        """
        index = {}
        for listing in listings:
            addr = listing.get("street_address", "")
            if addr:
                index[self._normalize_addr_key(addr)] = listing
        return index

    # ------------------------------------------------------------------
    # Enrichment helper (used by mls_enrich.py)
    # ------------------------------------------------------------------

    def enrich_lead(self, lead: dict, address_index: dict) -> dict:
        """
        Add MLS fields to a lead record by looking up its address in the index.

        Adds these keys to the lead (prefixed with mls_):
          mls_found          bool
          mls_status         str   ("Active", "Pending", "Closed", etc.)
          mls_list_price     int
          mls_days_on_market int
          mls_beds           int
          mls_baths          float
          mls_sqft           int
          mls_listing_date   str   (ISO date)
          mls_listing_id     str
          mls_note           str   (human-readable summary)
        """
        lead.setdefault("mls_found", False)
        lead.setdefault("mls_status", "")
        lead.setdefault("mls_list_price", None)
        lead.setdefault("mls_days_on_market", None)
        lead.setdefault("mls_beds", None)
        lead.setdefault("mls_baths", None)
        lead.setdefault("mls_sqft", None)
        lead.setdefault("mls_listing_date", "")
        lead.setdefault("mls_listing_id", "")
        lead.setdefault("mls_note", "")

        # Try prop_address first, then situs_address
        raw_addr = lead.get("prop_address") or lead.get("situs_address") or ""
        if not raw_addr:
            return lead

        key = self._normalize_addr_key(raw_addr)
        match = address_index.get(key)

        if not match:
            # Try a fuzzy partial match (house number + first word of street)
            short_key = " ".join(key.split()[:2])
            for idx_key, idx_val in address_index.items():
                if idx_key.startswith(short_key):
                    match = idx_val
                    break

        if match:
            lead["mls_found"]          = True
            lead["mls_status"]         = match.get("mls_status", "")
            lead["mls_list_price"]     = match.get("mls_list_price")
            lead["mls_days_on_market"] = match.get("mls_days_on_market")
            lead["mls_beds"]           = match.get("mls_beds")
            lead["mls_baths"]          = match.get("mls_baths")
            lead["mls_sqft"]           = match.get("mls_sqft")
            lead["mls_listing_date"]   = match.get("mls_listing_date", "")
            lead["mls_listing_id"]     = match.get("mls_listing_id", "")
            lead["mls_note"] = (
                f"{match.get('mls_status','')} at "
                f"${match.get('mls_list_price',0):,} "
                f"({match.get('mls_days_on_market','?')} DOM)"
            )
            log.info(f"MLS match: {raw_addr} → {lead['mls_note']}")

        return lead

    # ------------------------------------------------------------------
    # Normalizers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_listing(fields: dict) -> dict:
        """Map Spark StandardFields to our internal schema."""
        return {
            "mls_listing_id":     fields.get("ListingId", ""),
            "mls_status":         fields.get("StandardStatus", ""),
            "mls_list_price":     fields.get("ListPrice"),
            "mls_days_on_market": fields.get("DaysOnMarket"),
            "mls_beds":           fields.get("BedsTotal"),
            "mls_baths":          fields.get("BathroomsTotalInteger"),
            "mls_sqft":           fields.get("LivingArea"),
            "mls_listing_date":   fields.get("ListingContractDate", ""),
            "street_address":     fields.get("StreetNumber", "") + " " + fields.get("StreetName", ""),
            "unparsed_address":   fields.get("UnparsedAddress", ""),
            "city":               fields.get("City", ""),
            "state":              fields.get("StateOrProvince", ""),
            "zip":                fields.get("PostalCode", ""),
        }

    @staticmethod
    def _extract_street(full_address: str) -> str:
        """Pull just the street portion from a full address string."""
        # Strip anything after the first comma (city, state, zip)
        street = full_address.split(",")[0].strip()
        # Remove unit/apt suffixes
        street = re.sub(r"\s+(APT|UNIT|STE|#)\s*\S+", "", street, flags=re.I)
        return street.strip()

    @staticmethod
    def _normalize_addr_key(addr: str) -> str:
        """Lowercase, strip punctuation, normalize whitespace for dict keying."""
        addr = addr.upper()
        addr = addr.split(",")[0]  # street only
        addr = re.sub(r"[^A-Z0-9 ]", " ", addr)
        addr = re.sub(r"\s+", " ", addr).strip()
        # Normalize directionals
        addr = re.sub(r"\bNORTH\b", "N", addr)
        addr = re.sub(r"\bSOUTH\b", "S", addr)
        addr = re.sub(r"\bEAST\b",  "E", addr)
        addr = re.sub(r"\bWEST\b",  "W", addr)
        # Normalize street suffixes
        replacements = {
            "STREET": "ST", "AVENUE": "AVE", "BOULEVARD": "BLVD",
            "DRIVE": "DR", "COURT": "CT", "PLACE": "PL",
            "LANE": "LN", "ROAD": "RD", "CIRCLE": "CIR",
        }
        for full, abbr in replacements.items():
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
# Quick test / standalone usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    client = MLSClient()

    if len(sys.argv) > 1:
        # Test a specific address: python mls_lookup.py "3006 56th St"
        addr = " ".join(sys.argv[1:])
        result = client.look_up_address(addr)
        print(json.dumps(result, indent=2) if result else f"No listing found for: {addr}")
    else:
        # Pull all active listings and print summary
        listings = client.fetch_area_listings(status="Active", max_records=100)
        print(f"\nFetched {len(listings)} active listings in {MLS_CITY}, {MLS_STATE}")
        for l in listings[:5]:
            print(f"  {l['unparsed_address']} — ${l['mls_list_price']:,} ({l['mls_status']})")
