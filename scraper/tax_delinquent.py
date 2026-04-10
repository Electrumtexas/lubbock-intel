"""
tax_delinquent.py — Monthly tax delinquent lead processor for lubbock-intel

Reads:
  data/DelinquentResidential_current.xlsx  — monthly delinquent list from LCAD
  data/DataExport_current.txt              — full tax statement export (all years)

Writes: dashboard/tax_delinquent.json

Scoring accounts for DEPTH of delinquency:
  - Multi-year (2+ tax years with balance) = real distress, score by balance
  - Single-year, full unpaid (ratio ~1.0)  = just current bill, very low signal (score ≤20)
  - Single-year, partial payment (ratio <0.95) = some effort, moderate signal (score ≤35)

Balance tiers (percentile-based from April 2026 Lubbock dataset):
  25th pct: $790    50th: $1,823    75th: $3,883
  90th pct: $7,298  95th: $10,673
"""

import pandas as pd
import json
import re
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'scraper'))
from lcad_lookup import LCADLookup

DELINQUENT_PATH  = ROOT / 'data' / 'DelinquentResidential_current.xlsx'
DATAEXPORT_PATH  = ROOT / 'data' / 'DataExport_current.txt'
OUTPUT_PATH      = ROOT / 'dashboard' / 'tax_delinquent.json'

REQUIRED_FIELDS = [
    'owner_name',
    'situs_address', 'situs_city', 'situs_state', 'situs_zip',
    'mail_address',  'mail_city',  'mail_state',  'mail_zip',
]


# ── Depth lookup from DataExport ──────────────────────────────────────────

def build_depth_lookup(dataexport_path: Path) -> dict:
    """
    Parse DataExport to build year-by-year delinquency depth per R-number.
    Returns dict: r_number → {years_count, delinquent_years, annual_bill, ratio, ...}
    """
    if not dataexport_path.exists():
        print(f"[tax_delinquent] WARNING: DataExport not found at {dataexport_path} — depth data unavailable")
        return {}

    print(f"[tax_delinquent] Building depth lookup from {dataexport_path.name}...")
    df = pd.read_csv(dataexport_path,
        usecols=['Quick Ref', 'Tax Year', 'Fee Amount', 'Fee Balance'],
        low_memory=False, dtype=str)
    df = df[df['Quick Ref'].str.startswith('R', na=False)].copy()
    df['Fee Balance'] = pd.to_numeric(df['Fee Balance'], errors='coerce').fillna(0)
    df['Fee Amount']  = pd.to_numeric(df['Fee Amount'],  errors='coerce').fillna(0)
    df['Tax Year']    = pd.to_numeric(df['Tax Year'],    errors='coerce')

    depth = {}
    for r_num, grp in df.groupby('Quick Ref'):
        delinquent_rows  = grp[grp['Fee Balance'] > 0]
        if delinquent_rows.empty:
            continue
        delinquent_years = sorted(delinquent_rows['Tax Year'].dropna().astype(int).unique().tolist())
        years_count      = len(delinquent_years)
        total_balance    = round(float(delinquent_rows['Fee Balance'].sum()), 2)
        latest_year      = grp['Tax Year'].max()
        annual_bill      = round(float(grp[grp['Tax Year'] == latest_year]['Fee Amount'].sum()), 2)
        ratio            = round(total_balance / annual_bill, 3) if annual_bill > 0 else None
        is_partial       = ratio is not None and ratio < 0.95

        depth[r_num] = {
            'years_count':      years_count,
            'delinquent_years': delinquent_years,
            'annual_bill':      annual_bill,
            'ratio':            ratio,
            'is_partial_pay':   is_partial,
        }

    print(f"[tax_delinquent] Depth lookup: {len(depth):,} R-numbers")
    return depth


# ── Scoring ────────────────────────────────────────────────────────────────

def score_tax(balance: float, flags: list, depth: dict) -> int:
    """
    Score 0–100 combining balance size with delinquency depth.

    Single-year current bill only → heavily downscored (not genuine distress).
    Multi-year → upscored.
    """
    years      = depth.get('years_count', 1) if depth else 1
    ratio      = depth.get('ratio', 1.0)     if depth else 1.0
    is_partial = depth.get('is_partial_pay', False) if depth else False

    # Base score by balance amount
    if   balance < 500:    base = 25
    elif balance < 1_823:  base = 35
    elif balance < 3_883:  base = 48
    elif balance < 7_298:  base = 58
    elif balance < 15_000: base = 68
    elif balance < 30_000: base = 75
    else:                  base = 82

    # Depth adjustment
    if years == 1:
        if is_partial:
            # Making some payments but falling behind
            base = min(base, 35)
        else:
            # Just this year's bill — could pay next week
            base = min(base, 20)
    elif years == 2:
        base += 5   # Two years = pattern forming
    elif years >= 3:
        base += 10  # Three+ = chronic non-payer

    # Flag bonuses
    bonus = sum({'Absentee Owner': 5, 'LLC/Corp Owner': 5}.get(f, 0) for f in flags)
    return min(100, max(10, base + bonus))


def build_flags(enriched: dict, balance: float, depth: dict) -> list:
    flags = ['Tax Delinquent']

    years = depth.get('years_count', 1) if depth else 1
    ratio = depth.get('ratio', 1.0)     if depth else 1.0
    is_partial = depth.get('is_partial_pay', False) if depth else False

    # Delinquency depth flags
    if years == 1 and not is_partial:
        flags.append('Current Year Only')
    elif years == 1 and is_partial:
        flags.append('Partial Payment')
    elif years == 2:
        flags.append('2-Year Delinquent')
    elif years >= 3:
        flags.append('Multi-Year Delinquent')

    # Balance flags
    if balance >= 10_000:
        flags.append('High Balance')
    elif balance >= 3_883:
        flags.append('Above Median')

    # Absentee owner
    if (enriched.get('owner_name') and enriched.get('mail_address') and
            enriched.get('mail_city', '').upper() != enriched.get('situs_city', '').upper()
            and enriched.get('mail_city')):
        flags.append('Absentee Owner')

    # LLC/Corp
    if re.search(r'\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b', (enriched.get('owner_name') or '').upper()):
        flags.append('LLC/Corp Owner')

    if not enriched.get('situs_address'):
        flags.append('No Address')

    return flags


# ── Completeness ───────────────────────────────────────────────────────────

def check_completeness(rec: dict):
    missing = [f for f in REQUIRED_FIELDS if not str(rec.get(f, '') or '').strip()]
    rec['is_complete']    = len(missing) == 0
    rec['missing_fields'] = missing


# ── Main ───────────────────────────────────────────────────────────────────

def process(delinquent_path=None, output_path=None, data_date=None):
    dp = Path(delinquent_path or DELINQUENT_PATH)
    op = Path(output_path or OUTPUT_PATH)

    if not dp.exists():
        print(f"[tax_delinquent] ERROR: File not found: {dp}")
        sys.exit(1)

    # Build depth lookup
    depth_lookup = build_depth_lookup(Path(DATAEXPORT_PATH))

    # Load delinquent list
    print(f"[tax_delinquent] Loading {dp.name}...")
    df = pd.read_excel(dp)
    df['QuickRefID'] = df['QuickRefID'].astype(str).str.strip()
    print(f"[tax_delinquent] {len(df):,} records loaded")

    lookup = LCADLookup()
    leads  = []

    for _, row in df.iterrows():
        r       = str(row['QuickRefID']).strip()
        balance = float(row['SumOfBLCOL_BalanceAmount']) if pd.notna(row.get('SumOfBLCOL_BalanceAmount')) else 0.0
        enriched = lookup.enrich(r)
        depth    = depth_lookup.get(r)

        flags = build_flags(enriched, balance, depth)
        score = score_tax(balance, flags, depth)

        lead = {
            **enriched,
            'balance_owed':       round(balance, 2),
            'property_type_code': str(row.get('PropertyTypeCode', '') or '').strip(),
            'property_type_desc': str(row.get('PropertyTypeDesc', '') or '').strip(),
            'tax_year':           int(row['AdHocTaxYear']) if pd.notna(row.get('AdHocTaxYear')) else None,
            'score':              score,
            'data_date':          data_date or datetime.today().strftime('%Y-%m-%d'),
            'flags':              flags,
            'source':             'tax_delinquent',
            # Depth fields
            'years_count':        depth['years_count']      if depth else 1,
            'delinquent_years':   depth['delinquent_years'] if depth else [],
            'annual_bill':        depth['annual_bill']      if depth else None,
            'delinquency_ratio':  depth['ratio']            if depth else None,
            'is_partial_pay':     depth['is_partial_pay']   if depth else False,
        }

        check_completeness(lead)
        leads.append(lead)

    # Stats
    src = {}
    for l in leads:
        src[l['address_source']] = src.get(l['address_source'], 0) + 1

    single_yr = sum(1 for l in leads if l['years_count'] == 1 and not l['is_partial_pay'])
    multi_yr  = sum(1 for l in leads if l['years_count'] >= 2)
    partial   = sum(1 for l in leads if l['is_partial_pay'])
    absentee  = sum(1 for l in leads if 'Absentee Owner' in l['flags'])

    print(f"[tax_delinquent] Processed: {len(leads):,} leads")
    print(f"[tax_delinquent] Address sources: {src}")
    print(f"[tax_delinquent] Avg score: {sum(l['score'] for l in leads)/len(leads):.1f}")
    print(f"[tax_delinquent] Single-year only: {single_yr:,} | Partial pay: {partial:,} | Multi-year: {multi_yr:,}")
    print(f"[tax_delinquent] Absentee owners: {absentee:,}")

    op.parent.mkdir(parents=True, exist_ok=True)
    with open(op, 'w') as f:
        json.dump(leads, f, separators=(',', ':'), default=str)
    print(f"[tax_delinquent] ✓ Written: {op} ({len(leads):,} records)")


if __name__ == '__main__':
    process()
