"""
lcad_lookup.py — Local LCAD enrichment engine for lubbock-intel
Uses AllRes + DataExport files as primary lookup; LCAD API as fallback only.

Usage:
    from lcad_lookup import LCADLookup
    lookup = LCADLookup()
    result = lookup.enrich('R100006')
"""

import pandas as pd
import json
import os
import time
import requests
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / 'data'
ALLRES_PATH = DATA_DIR / 'AllRes_current.xlsx'
DATAEXPORT_PATH = DATA_DIR / 'DataExport_current.txt'
CAD_CACHE_PATH = DATA_DIR / 'cad_cache.json'

CAD_API_BASE = 'https://lubbockcad.org/ProxyT/Search/Properties/quick/'
CAD_DELAY = 2.0


class LCADLookup:
    def __init__(self, allres_path=None, dataexport_path=None, cache_path=None, verbose=True):
        self.verbose = verbose
        self.allres_lookup = {}
        self.dataexport_lookup = {}
        self.cache = {}

        ap = Path(allres_path or ALLRES_PATH)
        dp = Path(dataexport_path or DATAEXPORT_PATH)
        cp = Path(cache_path or CAD_CACHE_PATH)

        if ap.exists():
            self._load_allres(ap)
        elif verbose:
            print(f"[lcad_lookup] WARNING: AllRes file not found at {ap}")

        if dp.exists():
            self._load_dataexport(dp)
        elif verbose:
            print(f"[lcad_lookup] WARNING: DataExport file not found at {dp}")

        if cp.exists():
            with open(cp) as f:
                self.cache = json.load(f)
            if verbose:
                print(f"[lcad_lookup] Cache loaded: {len(self.cache)} entries")

    def _load_allres(self, path):
        df = pd.read_excel(path, usecols=['QuickRefID', 'SitusAddress', 'LegalDescription', 'FinalTotal'])
        df['QuickRefID'] = df['QuickRefID'].astype(str).str.strip()
        df['has_comma'] = df['SitusAddress'].astype(str).str.contains(',')
        df = df.sort_values(['QuickRefID', 'has_comma'], ascending=[True, False])
        df = df.drop_duplicates(subset=['QuickRefID'], keep='first').drop(columns='has_comma')
        self.allres_lookup = df.set_index('QuickRefID').to_dict('index')
        if self.verbose:
            print(f"[lcad_lookup] AllRes loaded: {len(self.allres_lookup):,} properties")

    def _load_dataexport(self, path):
        df = pd.read_csv(path, usecols=['Quick Ref', 'Owner Name', 'Addr1', 'Addr2', 'City', 'State', 'Zip'],
                         low_memory=False, dtype=str)
        df = df[df['Quick Ref'].str.startswith('R', na=False)].drop_duplicates(subset=['Quick Ref'])
        self.dataexport_lookup = df.set_index('Quick Ref').to_dict('index')
        if self.verbose:
            print(f"[lcad_lookup] DataExport loaded: {len(self.dataexport_lookup):,} owner records")

    @staticmethod
    def _parse_situs(situs):
        parts = [p.strip() for p in str(situs or '').split(',')]
        addr, city, state, zip_ = '', '', 'TX', ''
        if len(parts) >= 3:
            addr = parts[0]
            city = parts[1]
            sz = parts[2].split()
            state = sz[0] if sz else 'TX'
            zip_ = sz[1] if len(sz) > 1 else ''
        elif len(parts) == 2:
            addr, city = parts[0], parts[1]
        elif parts:
            addr = parts[0]
        return addr, city, state, zip_

    def _api_lookup(self, name, r_number=None):
        """Fallback LCAD API lookup by owner name."""
        cache_key = r_number or name
        if cache_key in self.cache:
            return self.cache[cache_key]

        SKIP = {'U S OF AMERICA', 'TEXAS STATE OF', 'LUBBOCK CITY OF', 'LUBBOCK COUNTY'}
        if name.upper().strip() in SKIP:
            return None

        params = {'f': 'NAME', 'pn': '1', 'st': '4', 'so': 'desc', 'pt': 'RP;PP;MH;NR', 'ty': '2026'}
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = f"{CAD_API_BASE}?q={requests.utils.quote(name)}"

        for attempt in range(3):
            try:
                time.sleep(CAD_DELAY)
                resp = requests.get(CAD_API_BASE, params={**params, 'q': name},
                                    headers=headers, timeout=15)
                data = resp.json()
                items = data.get('items') or data.get('PropertyList') or []
                if items:
                    item = items[0]
                    result = {
                        'situs_address': item.get('PropertyAddress', ''),
                        'owner_name': item.get('OwnerName', ''),
                        'source': 'api'
                    }
                    self.cache[cache_key] = result
                    return result
                break
            except Exception as e:
                if attempt == 2 and self.verbose:
                    print(f"[lcad_lookup] API error for {name}: {e}")
                time.sleep(2 ** attempt)
        self.cache[cache_key] = None
        return None

    def enrich(self, r_number):
        """
        Enrich an R-number with address + owner data.
        Returns a dict with situs address, owner name, mailing address, assessed value.
        """
        r = str(r_number).strip()
        result = {
            'r_number': r,
            'situs_address': '', 'situs_city': '', 'situs_state': 'TX', 'situs_zip': '',
            'owner_name': '',
            'mail_address': '', 'mail_city': '', 'mail_state': '', 'mail_zip': '',
            'assessed_value': None,
            'legal_description': '',
            'address_source': 'none'
        }

        # Layer 1: AllRes → situs address + assessed value
        if r in self.allres_lookup:
            ar = self.allres_lookup[r]
            situs = str(ar.get('SitusAddress', '') or '').strip()
            result['legal_description'] = str(ar.get('LegalDescription', '') or '').strip()
            val = ar.get('FinalTotal')
            result['assessed_value'] = int(val) if pd.notna(val) else None
            a, c, s, z = self._parse_situs(situs)
            result['situs_address'] = a
            result['situs_city'] = c
            result['situs_state'] = s
            result['situs_zip'] = z
            result['address_source'] = 'allres'

        # Layer 2: DataExport → owner + mailing address
        if r in self.dataexport_lookup:
            de = self.dataexport_lookup[r]
            result['owner_name'] = str(de.get('Owner Name', '') or '').strip()
            addr1 = str(de.get('Addr1', '') or '').strip()
            addr2 = str(de.get('Addr2', '') or '').strip()
            result['mail_address'] = (addr1 + (' ' + addr2 if addr2 else '')).strip()
            result['mail_city'] = str(de.get('City', '') or '').strip()
            result['mail_state'] = str(de.get('State', '') or '').strip()
            result['mail_zip'] = str(de.get('Zip', '') or '').strip()
            result['address_source'] = 'full' if result['address_source'] == 'allres' else 'dataexport'

        return result

    def enrich_batch(self, r_numbers):
        """Enrich a list of R-numbers. Returns list of dicts."""
        return [self.enrich(r) for r in r_numbers]

    def save_cache(self, path=None):
        cp = Path(path or CAD_CACHE_PATH)
        cp.parent.mkdir(parents=True, exist_ok=True)
        with open(cp, 'w') as f:
            json.dump(self.cache, f, indent=2)
        if self.verbose:
            print(f"[lcad_lookup] Cache saved: {len(self.cache)} entries → {cp}")
