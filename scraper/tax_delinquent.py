"""
tax_delinquent.py — Monthly tax delinquent lead processor for lubbock-intel

Reads: data/DelinquentResidential_current.xlsx  (dropped in monthly from email)
Writes: dashboard/tax_delinquent.json

Scoring (0–100):  Higher balance = higher score (more distressed)
  $10,000+  → 100
  $7,298+   → 90   (90th percentile)
  $3,883+   → 75   (75th percentile)
  $1,823+   → 60   (50th percentile)
  $790+     → 50   (25th percentile)
  < $790    → 40
"""

import pandas as pd
import json
import sys
from pathlib import Path
from datetime import datetime

# Allow running from repo root or scraper/ directory
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'scraper'))
from lcad_lookup import LCADLookup

DELINQUENT_PATH = ROOT / 'data' / 'DelinquentResidential_current.xlsx'
OUTPUT_PATH = ROOT / 'dashboard' / 'tax_delinquent.json'


def score_balance(balance: float) -> int:
    if balance >= 10000: return 100
    if balance >= 7298:  return 90
    if balance >= 3883:  return 75
    if balance >= 1823:  return 60
    if balance >= 790:   return 50
    return 40


def build_flags(enriched: dict, balance: float) -> list:
    flags = ['Tax Delinquent']
    if balance >= 10000:
        flags.append('High Balance')
    elif balance >= 3883:
        flags.append('Above Median')
    # Absentee owner: has mailing info AND mailing city differs from situs city
    if (enriched.get('owner_name') and enriched.get('mail_address') and
            enriched.get('mail_city', '').upper() != enriched.get('situs_city', '').upper()
            and enriched.get('mail_city')):
        flags.append('Absentee Owner')
    if not enriched.get('situs_address'):
        flags.append('No Address')
    return flags


def process(delinquent_path=None, output_path=None, data_date=None):
    dp = Path(delinquent_path or DELINQUENT_PATH)
    op = Path(output_path or OUTPUT_PATH)

    if not dp.exists():
        print(f"[tax_delinquent] ERROR: File not found: {dp}")
        sys.exit(1)

    print(f"[tax_delinquent] Loading {dp.name}...")
    df = pd.read_excel(dp)
    df['QuickRefID'] = df['QuickRefID'].astype(str).str.strip()
    print(f"[tax_delinquent] {len(df):,} records loaded")

    lookup = LCADLookup()
    leads = []

    for _, row in df.iterrows():
        r = str(row['QuickRefID']).strip()
        balance = float(row['SumOfBLCOL_BalanceAmount']) if pd.notna(row.get('SumOfBLCOL_BalanceAmount')) else 0.0
        enriched = lookup.enrich(r)

        lead = {
            **enriched,
            'balance_owed': round(balance, 2),
            'property_type_code': str(row.get('PropertyTypeCode', '') or '').strip(),
            'property_type_desc': str(row.get('PropertyTypeDesc', '') or '').strip(),
            'tax_year': int(row['AdHocTaxYear']) if pd.notna(row.get('AdHocTaxYear')) else None,
            'score': score_balance(balance),
            'data_date': data_date or datetime.today().strftime('%Y-%m-%d'),
            'flags': build_flags(enriched, balance),
            'source': 'tax_delinquent'
        }
        leads.append(lead)

    # Stats
    src = {}
    for l in leads:
        src[l['address_source']] = src.get(l['address_source'], 0) + 1
    print(f"[tax_delinquent] Processed: {len(leads):,} leads")
    print(f"[tax_delinquent] Address sources: {src}")
    print(f"[tax_delinquent] Avg score: {sum(l['score'] for l in leads)/len(leads):.1f}")
    absentee = sum(1 for l in leads if 'Absentee Owner' in l['flags'])
    print(f"[tax_delinquent] Absentee owners: {absentee:,}")

    op.parent.mkdir(parents=True, exist_ok=True)
    with open(op, 'w') as f:
        json.dump({
            'generated': datetime.utcnow().isoformat() + 'Z',
            'data_date': data_date or datetime.today().strftime('%Y-%m-%d'),
            'count': len(leads),
            'leads': leads
        }, f, indent=2, default=str)
    print(f"[tax_delinquent] ✓ Written: {op}")


if __name__ == '__main__':
    process()
