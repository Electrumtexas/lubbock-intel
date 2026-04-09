"""
lcad_lookup.py — LCAD enrichment engine for lubbock-intel

Lookup chain (in order):
  1. AllRes local xlsx  → situs address + assessed value  (instant, ~95% hit rate)
  2. DataExport local txt → owner name + mailing address  (instant, ~60% hit rate)
  3. LCAD CAD API        → fallback for true misses        (slow, rate-limited)
  4. Cache               → so CAD hits are never repeated

Usage:
    from lcad_lookup import LCADLookup
    lu = LCADLookup()
    result = lu.enrich('R100006')
    # result keys: r_number, situs_address, situs_city, situs_state, situs_zip,
    #              owner_name, mail_address, mail_city, mail_state, mail_zip,
    #              assessed_value, legal_description, address_source
"""

import json, time, requests, pandas as pd
from pathlib import Path

# ── Default paths (relative to repo root) ────────────────────────────────
_HERE = Path(__file__).parent.parent   # repo root
ALLRES_PATH     = _HERE / 'data' / 'AllRes_current.xlsx'
DATAEXPORT_PATH = _HERE / 'data' / 'DataExport_current.txt'
CAD_CACHE_PATH  = _HERE / 'data' / 'cad_cache.json'

CAD_API_BASE = 'https://lubbockcad.org/ProxyT/Search/Properties/quick/'
CAD_DELAY    = 2.0   # seconds between API calls
CAD_RETRIES  = 3

# Names that will never match on the CAD site — skip API entirely
CAD_SKIP = {
    'U S OF AMERICA', 'TEXAS STATE OF', 'LUBBOCK CITY OF',
    'LUBBOCK COUNTY', 'SECRETARY OF HOUSING', 'FANNIE MAE',
    'FREDDIE MAC', 'HUD', 'VETERANS AFFAIRS'
}


class LCADLookup:
    def __init__(self, allres_path=None, dataexport_path=None,
                 cache_path=None, verbose=True, use_api=True):
        self.verbose  = verbose
        self.use_api  = use_api
        self.allres   = {}
        self.dedata   = {}   # DataExport
        self.cache    = {}

        ap = Path(allres_path or ALLRES_PATH)
        dp = Path(dataexport_path or DATAEXPORT_PATH)
        cp = Path(cache_path or CAD_CACHE_PATH)

        if ap.exists():
            self._load_allres(ap)
        else:
            self._log(f"WARNING: AllRes not found at {ap}")

        if dp.exists():
            self._load_dataexport(dp)
        else:
            self._log(f"WARNING: DataExport not found at {dp}")

        if cp.exists():
            with open(cp) as f:
                self.cache = json.load(f)
            self._log(f"Cache: {len(self.cache):,} entries")

        self._cache_path = cp

    # ── Loaders ──────────────────────────────────────────────────────────

    def _load_allres(self, path):
        df = pd.read_excel(path,
            usecols=['QuickRefID', 'SitusAddress', 'LegalDescription', 'FinalTotal'])
        df['QuickRefID'] = df['QuickRefID'].astype(str).str.strip()
        # Prefer rows with comma-formatted address (city present)
        df['_has_comma'] = df['SitusAddress'].astype(str).str.contains(',')
        df = (df.sort_values(['QuickRefID', '_has_comma'], ascending=[True, False])
                .drop_duplicates('QuickRefID', keep='first')
                .drop(columns='_has_comma'))
        self.allres = df.set_index('QuickRefID').to_dict('index')
        self._log(f"AllRes: {len(self.allres):,} properties")

    def _load_dataexport(self, path):
        df = pd.read_csv(path,
            usecols=['Quick Ref', 'Owner Name', 'Addr1', 'Addr2', 'City', 'State', 'Zip'],
            low_memory=False, dtype=str)
        df = df[df['Quick Ref'].str.startswith('R', na=False)].drop_duplicates('Quick Ref')
        self.dedata = df.set_index('Quick Ref').to_dict('index')
        self._log(f"DataExport: {len(self.dedata):,} owner records")

    def _log(self, msg):
        if self.verbose:
            print(f"[lcad_lookup] {msg}")

    # ── Address parser ────────────────────────────────────────────────────

    @staticmethod
    def _parse_situs(situs):
        """'1115 32ND ST, LUBBOCK, TX  79411' → (addr, city, state, zip)"""
        parts = [p.strip() for p in str(situs or '').split(',')]
        a, c, st, z = '', '', 'TX', ''
        if len(parts) >= 3:
            a = parts[0]; c = parts[1]
            sz = parts[2].split()
            st = sz[0] if sz else 'TX'
            z  = sz[1] if len(sz) > 1 else ''
        elif len(parts) == 2:
            a, c = parts[0], parts[1]
        elif parts:
            a = parts[0]
        return a, c, st, z

    # ── CAD API fallback ──────────────────────────────────────────────────

    def _api_lookup(self, name, cache_key):
        """Hit LCAD API by owner name. Returns partial dict or None."""
        if cache_key in self.cache:
            return self.cache[cache_key]

        clean = name.upper().strip()
        if any(skip in clean for skip in CAD_SKIP):
            self.cache[cache_key] = None
            return None

        params = {
            'f': 'NAME', 'pn': '1', 'st': '4', 'so': 'desc',
            'pt': 'RP;PP;MH;NR', 'ty': '2026', 'q': name
        }
        headers = {'User-Agent': 'Mozilla/5.0'}

        for attempt in range(CAD_RETRIES):
            try:
                time.sleep(CAD_DELAY)
                resp = requests.get(CAD_API_BASE, params=params,
                                    headers=headers, timeout=15)
                data = resp.json()
                items = data.get('items') or data.get('PropertyList') or []
                if items:
                    item = items[0]
                    result = {
                        'situs_address':   item.get('PropertyAddress', '').strip(),
                        'owner_name':      item.get('OwnerName', '').strip(),
                        'legal_description': item.get('LegalDescription', '').strip(),
                        'address_source':  'cad_api'
                    }
                    self.cache[cache_key] = result
                    return result
                # Empty but valid response — record the miss
                self.cache[cache_key] = None
                return None
            except Exception as e:
                if attempt == CAD_RETRIES - 1:
                    self._log(f"API error for '{name}': {e}")
                time.sleep(2 ** attempt)

        self.cache[cache_key] = None
        return None

    # ── Main enrich method ────────────────────────────────────────────────

    def enrich(self, r_number):
        """
        Enrich a single R-number. Returns a dict with all address fields.
        address_source values: 'full' | 'allres' | 'dataexport' | 'cad_api' | 'none'
        """
        r = str(r_number or '').strip()

        out = {
            'r_number':          r,
            'situs_address':     '',
            'situs_city':        '',
            'situs_state':       'TX',
            'situs_zip':         '',
            'owner_name':        '',
            'mail_address':      '',
            'mail_city':         '',
            'mail_state':        '',
            'mail_zip':          '',
            'assessed_value':    None,
            'legal_description': '',
            'address_source':    'none',
        }

        # ── Layer 1: AllRes ───────────────────────────────────────────────
        if r in self.allres:
            ar = self.allres[r]
            a, c, st, z = self._parse_situs(ar.get('SitusAddress', ''))
            out['situs_address']    = a
            out['situs_city']       = c
            out['situs_state']      = st
            out['situs_zip']        = z
            out['legal_description'] = str(ar.get('LegalDescription', '') or '').strip()
            val = ar.get('FinalTotal')
            out['assessed_value']   = int(val) if pd.notna(val) else None
            out['address_source']   = 'allres'

        # ── Layer 2: DataExport ───────────────────────────────────────────
        if r in self.dedata:
            de = self.dedata[r]
            out['owner_name'] = str(de.get('Owner Name', '') or '').strip()
            a1 = str(de.get('Addr1', '') or '').strip()
            a2 = str(de.get('Addr2', '') or '').strip()
            out['mail_address'] = (a1 + (' ' + a2 if a2 else '')).strip()
            out['mail_city']    = str(de.get('City',  '') or '').strip()
            out['mail_state']   = str(de.get('State', '') or '').strip()
            out['mail_zip']     = str(de.get('Zip',   '') or '').strip()
            out['address_source'] = 'full' if out['address_source'] == 'allres' else 'dataexport'

        # ── Layer 3: CAD API fallback (only for genuine misses) ───────────
        if self.use_api and out['address_source'] == 'none' and r:
            # We have no data at all — try API by R-number pattern
            # (The CAD API searches by name, so we need the owner name first;
            #  if we have no owner, try a subdivision lookup from legal desc)
            pass  # Will be called by the caller with owner name if known

        return out

    def enrich_with_name(self, r_number, owner_name):
        """
        Full enrich including CAD API fallback using owner name.
        Use this version from fetch.py when local lookup misses.
        """
        out = self.enrich(r_number)

        # Only hit API if we're still missing address
        if self.use_api and not out['situs_address'] and owner_name:
            cache_key = r_number or owner_name
            api_result = self._api_lookup(owner_name, cache_key)
            if api_result:
                if api_result.get('situs_address') and not out['situs_address']:
                    raw = api_result['situs_address']
                    a, c, st, z = self._parse_situs(raw)
                    # CAD API returns "STREET CITY TX ZIP" without commas sometimes
                    if not c and ' ' in raw:
                        out['situs_address'] = raw
                    else:
                        out['situs_address'] = a
                        out['situs_city']    = c
                        out['situs_state']   = st
                        out['situs_zip']     = z
                if api_result.get('owner_name') and not out['owner_name']:
                    out['owner_name'] = api_result['owner_name']
                if api_result.get('legal_description') and not out['legal_description']:
                    out['legal_description'] = api_result['legal_description']
                out['address_source'] = 'cad_api'

        return out

    def enrich_batch(self, r_numbers):
        return [self.enrich(r) for r in r_numbers]

    def save_cache(self):
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._cache_path, 'w') as f:
            json.dump(self.cache, f, indent=2)
        self._log(f"Cache saved: {len(self.cache):,} entries → {self._cache_path}")

    def stats(self):
        return {
            'allres_count':     len(self.allres),
            'dataexport_count': len(self.dedata),
            'cache_count':      len(self.cache),
        }
