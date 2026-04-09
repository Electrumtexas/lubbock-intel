"""
fire_damage.py — Fire damage lead processor for lubbock-intel

Reads: data/StructureFires_current.xlsx  (manually updated via open records request)
Writes: dashboard/fire_damage.json

All records are included with a property_use tag.
Residential records score 72; non-residential score 45.
Residential detection is keyword-based on property_use field.
"""

import pandas as pd
import json
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'scraper'))
from lcad_lookup import LCADLookup

FIRE_PATH = ROOT / 'data' / 'StructureFires_current.xlsx'
OUTPUT_PATH = ROOT / 'dashboard' / 'fire_damage.json'

RESIDENTIAL_KEYWORDS = [
    '1 or 2 family', 'single family', 'multifamily', 'multi-family',
    'dwelling', 'residential', 'attached', 'detached'
]


def is_residential(prop_use: str) -> bool:
    s = str(prop_use or '').lower()
    return any(k in s for k in RESIDENTIAL_KEYWORDS)


def process(fire_path=None, output_path=None):
    fp = Path(fire_path or FIRE_PATH)
    op = Path(output_path or OUTPUT_PATH)

    if not fp.exists():
        print(f"[fire_damage] ERROR: File not found: {fp}")
        sys.exit(1)

    print(f"[fire_damage] Loading {fp.name}...")
    df = pd.read_excel(fp)
    print(f"[fire_damage] {len(df):,} records loaded")

    lookup = LCADLookup()
    leads = []

    for _, row in df.iterrows():
        prop_use = str(row.get('Property Use', '') or '')
        residential = is_residential(prop_use)

        r_raw = row.get('LCAD', '')
        r_number = str(r_raw).strip() if pd.notna(r_raw) else ''
        if r_number in ('nan', 'None', ''):
            r_number = ''

        incident_date = row.get('Incident Date')
        date_str = incident_date.strftime('%Y-%m-%d') if pd.notna(incident_date) else ''

        # ZIP safety
        def safe_zip(z):
            try: return str(int(z))
            except: return ''

        if r_number:
            enriched = lookup.enrich(r_number)
            # Fill situs from fire file if allres didn't have it
            if not enriched['situs_address']:
                enriched['situs_address'] = str(row.get('Location Street Address', '') or '').strip()
                enriched['situs_zip'] = safe_zip(row.get('Location ZIP', ''))
                enriched['situs_city'] = 'Lubbock'
        else:
            enriched = {
                'r_number': '',
                'situs_address': str(row.get('Location Street Address', '') or '').strip(),
                'situs_city': 'Lubbock',
                'situs_state': 'TX',
                'situs_zip': safe_zip(row.get('Location ZIP', '')),
                'owner_name': '', 'mail_address': '',
                'mail_city': '', 'mail_state': '', 'mail_zip': '',
                'assessed_value': None, 'legal_description': '',
                'address_source': 'fire_file'
            }

        flags = ['Fire Damage']
        if not residential:
            flags.append('Non-Residential')

        leads.append({
            **enriched,
            'incident_number': str(row.get('Incident Number', '')),
            'incident_date': date_str,
            'incident_type': str(row.get('Incident Type', '')),
            'property_use': prop_use,
            'is_residential': residential,
            'score': 72 if residential else 45,
            'flags': flags,
            'source': 'fire_damage'
        })

    res_count = sum(1 for l in leads if l['is_residential'])
    addr_count = sum(1 for l in leads if l['situs_address'])
    print(f"[fire_damage] Total: {len(leads)} | Residential: {res_count} | Non-res: {len(leads)-res_count}")
    print(f"[fire_damage] With address: {addr_count} | With R-number: {sum(1 for l in leads if l['r_number'])}")

    op.parent.mkdir(parents=True, exist_ok=True)
    with open(op, 'w') as f:
        json.dump({
            'generated': datetime.utcnow().isoformat() + 'Z',
            'count': len(leads),
            'residential_count': res_count,
            'leads': leads
        }, f, indent=2, default=str)
    print(f"[fire_damage] ✓ Written: {op}")


if __name__ == '__main__':
    process()
