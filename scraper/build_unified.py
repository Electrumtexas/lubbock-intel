"""
build_unified.py
Merges clerk records, tax delinquent, and fire damage into a single
unified leads JSON with stacked scoring and cross-enriched addresses.
"""
import json, pandas as pd, os
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────
ALLRES_PATH      = '/mnt/user-data/uploads/AllRes04042026.xlsx'
DATAEXPORT_PATH  = '/mnt/user-data/uploads/DataExport2633759.txt'
TAX_PATH         = '/home/claude/output/tax_delinquent.json'
FIRE_PATH        = '/home/claude/output/fire_damage.json'
CLERK_PATH       = None   # records.json not available yet; handled gracefully
OUT_PATH         = '/home/claude/output/dashboard/unified_leads.json'

# ── Scoring combo bonuses (capped at 100 total) ───────────────────────────
COMBO = {
    ('fire','tax'):          30,
    ('fire','clerk'):        25,
    ('tax','clerk'):         15,
    ('fire','tax','clerk'):  35,   # additive on top of pair bonuses
}

print("Loading reference files...")

# ── AllRes lookup ─────────────────────────────────────────────────────────
df_all = pd.read_excel(ALLRES_PATH,
    usecols=['QuickRefID','SitusAddress','LegalDescription','FinalTotal'])
df_all['QuickRefID'] = df_all['QuickRefID'].astype(str).str.strip()
df_all['has_comma'] = df_all['SitusAddress'].astype(str).str.contains(',')
df_all = df_all.sort_values(['QuickRefID','has_comma'], ascending=[True,False])
df_all = df_all.drop_duplicates(subset=['QuickRefID'], keep='first').drop(columns='has_comma')
allres = df_all.set_index('QuickRefID').to_dict('index')
print(f"  AllRes: {len(allres):,} properties")

# ── DataExport lookup ─────────────────────────────────────────────────────
df_de = pd.read_csv(DATAEXPORT_PATH,
    usecols=['Quick Ref','Owner Name','Addr1','Addr2','City','State','Zip'],
    low_memory=False, dtype=str)
df_de = df_de[df_de['Quick Ref'].str.startswith('R', na=False)].drop_duplicates('Quick Ref')
dataexport = df_de.set_index('Quick Ref').to_dict('index')
print(f"  DataExport: {len(dataexport):,} owner records")

def parse_situs(s):
    parts = [p.strip() for p in str(s or '').split(',')]
    a, c, st, z = '','','TX',''
    if len(parts) >= 3:
        a = parts[0]; c = parts[1]
        sz = parts[2].split(); st = sz[0] if sz else 'TX'; z = sz[1] if len(sz)>1 else ''
    elif len(parts) == 2: a,c = parts[0],parts[1]
    elif parts: a = parts[0]
    return a,c,st,z

def enrich_r(r_num, base=None):
    """Fill missing address/owner fields from AllRes + DataExport."""
    r = str(r_num or '').strip()
    out = base.copy() if base else {}

    # AllRes → situs address (fill only if missing)
    if r in allres:
        ar = allres[r]
        if not out.get('situs_address'):
            a,c,st,z = parse_situs(ar.get('SitusAddress',''))
            out['situs_address'] = a; out['situs_city'] = c
            out['situs_state'] = st; out['situs_zip'] = z
        if not out.get('legal_description'):
            out['legal_description'] = str(ar.get('LegalDescription','') or '').strip()
        if not out.get('assessed_value'):
            val = ar.get('FinalTotal')
            out['assessed_value'] = int(val) if pd.notna(val) else None

    # DataExport → owner + mailing (fill only if missing)
    if r in dataexport:
        de = dataexport[r]
        if not out.get('owner_name'):
            out['owner_name'] = str(de.get('Owner Name','') or '').strip()
        if not out.get('mail_address'):
            a1 = str(de.get('Addr1','') or '').strip()
            a2 = str(de.get('Addr2','') or '').strip()
            out['mail_address']  = (a1+(' '+a2 if a2 else '')).strip()
            out['mail_city']     = str(de.get('City','') or '').strip()
            out['mail_state']    = str(de.get('State','') or '').strip()
            out['mail_zip']      = str(de.get('Zip','') or '').strip()
    return out

# ── Load source data ──────────────────────────────────────────────────────
print("Loading source JSON files...")

with open(TAX_PATH) as f:  tax_list  = json.load(f)
with open(FIRE_PATH) as f: fire_list = json.load(f)
clerk_list = []
if CLERK_PATH and Path(CLERK_PATH).exists():
    with open(CLERK_PATH) as f:
        raw = json.load(f)
        clerk_list = raw.get('records', raw) if isinstance(raw, dict) else raw
print(f"  Tax delinquent: {len(tax_list):,}")
print(f"  Fire damage:    {len(fire_list):,}")
print(f"  Clerk records:  {len(clerk_list):,}")

# ── Index by R-number ─────────────────────────────────────────────────────
# Tax: one record per R-number
tax_by_r = {r['r_number']: r for r in tax_list if r.get('r_number')}

# Fire: multiple incidents possible per R-number; keep list
fire_by_r = {}
for r in fire_list:
    rn = r.get('r_number','')
    if rn:
        fire_by_r.setdefault(rn, []).append(r)

# Clerk: index by owner+address string and by doc_num; we'll match by R-number
# The clerk records don't have R-numbers natively — we match by situs address
# Build address → R-number reverse lookup from AllRes
addr_to_r = {}
for rn, ar in allres.items():
    situs = str(ar.get('SitusAddress','') or '').strip().upper()
    if situs:
        addr_to_r[situs] = rn
# Also build simplified street-only key
def street_key(addr):
    return str(addr or '').strip().upper().split(',')[0].strip()

street_to_r = {}
for rn, ar in allres.items():
    sk = street_key(ar.get('SitusAddress',''))
    if sk: street_to_r[sk] = rn

def clerk_r_number(rec):
    """Try to find R-number for a clerk record via address matching."""
    addr = str(rec.get('prop_address','') or '').strip().upper()
    city = str(rec.get('prop_city','') or '').strip().upper()
    if not addr: return ''
    full = addr + (', '+city if city else '')
    # Try exact full match first
    if full in addr_to_r: return addr_to_r[full]
    # Try street-only match
    if addr in street_to_r: return street_to_r[addr]
    return ''

clerk_by_r = {}
clerk_no_r = []
for rec in clerk_list:
    rn = clerk_r_number(rec)
    if rn:
        clerk_by_r.setdefault(rn, []).append(rec)
    else:
        clerk_no_r.append(rec)

print(f"\nClerk records matched to R-number: {sum(len(v) for v in clerk_by_r.values())} / {len(clerk_list)}")

# ── Build unified property universe ──────────────────────────────────────
# All unique R-numbers across all sources
all_r_numbers = set(tax_by_r.keys()) | set(fire_by_r.keys()) | set(clerk_by_r.keys())
print(f"Unique R-numbers across all sources: {len(all_r_numbers):,}")

unified = []

for rn in all_r_numbers:
    has_tax   = rn in tax_by_r
    has_fire  = rn in fire_by_r
    has_clerk = rn in clerk_by_r

    # ── Build base property record ─────────────────────────────────────
    prop = {
        'r_number': rn,
        'situs_address': '', 'situs_city': '', 'situs_state': 'TX', 'situs_zip': '',
        'owner_name': '',
        'mail_address': '', 'mail_city': '', 'mail_state': '', 'mail_zip': '',
        'assessed_value': None, 'legal_description': '',
        'sources': [],
        'flags': [],
        'distress_factors': [],
    }

    # ── Merge tax data ─────────────────────────────────────────────────
    tax_data = None
    if has_tax:
        td = tax_by_r[rn]
        prop['sources'].append('tax')
        prop['distress_factors'].append('tax')
        prop['flags'] += [f for f in (td.get('flags') or []) if f not in prop['flags']]
        # Copy address fields if we don't have them yet
        for fld in ['situs_address','situs_city','situs_state','situs_zip',
                    'owner_name','mail_address','mail_city','mail_state','mail_zip',
                    'assessed_value','legal_description']:
            if td.get(fld) and not prop.get(fld):
                prop[fld] = td[fld]
        prop['tax_balance']      = td.get('balance_owed', 0)
        prop['tax_score']        = td.get('score', 0)
        prop['tax_year']         = td.get('tax_year')
        prop['property_type']    = td.get('property_type_desc','')
        prop['data_date']        = td.get('data_date','')
        tax_data = td

    # ── Merge fire data ────────────────────────────────────────────────
    fire_incidents = []
    if has_fire:
        fires = fire_by_r[rn]
        prop['sources'].append('fire')
        prop['distress_factors'].append('fire')
        if 'Fire Damage' not in prop['flags']: prop['flags'].append('Fire Damage')
        # Use most recent incident
        fires_sorted = sorted(fires, key=lambda x: x.get('incident_date',''), reverse=True)
        latest = fires_sorted[0]
        for fld in ['situs_address','situs_city','situs_state','situs_zip',
                    'owner_name','mail_address','mail_city','mail_state','mail_zip',
                    'assessed_value','legal_description']:
            if latest.get(fld) and not prop.get(fld):
                prop[fld] = latest[fld]
        fire_incidents = [{
            'incident_number': f.get('incident_number'),
            'incident_date':   f.get('incident_date'),
            'incident_type':   f.get('incident_type'),
            'property_use':    f.get('property_use'),
            'is_residential':  f.get('is_residential', True),
        } for f in fires_sorted]
        prop['fire_score']     = latest.get('score', 0)
        prop['fire_incidents'] = fire_incidents
        prop['fire_latest']    = fires_sorted[0].get('incident_date','')
        prop['fire_property_use'] = latest.get('property_use','')
        prop['is_residential'] = latest.get('is_residential', True)

    # ── Merge clerk data ───────────────────────────────────────────────
    clerk_docs = []
    if has_clerk:
        docs = clerk_by_r[rn]
        prop['sources'].append('clerk')
        prop['distress_factors'].append('clerk')
        for doc in docs:
            for fld in ['owner_name','mail_address','mail_city','mail_state','mail_zip']:
                mapped = {'owner_name':'owner','mail_address':'mail_address',
                          'mail_city':'mail_city','mail_state':'mail_state','mail_zip':'mail_zip'}
                src_fld = mapped.get(fld, fld)
                if doc.get(src_fld) and not prop.get(fld):
                    prop[fld] = doc[src_fld]
            for flag in (doc.get('flags') or []):
                if flag not in prop['flags']: prop['flags'].append(flag)
        clerk_docs = [{
            'doc_type':  d.get('doc_type'),
            'cat':       d.get('cat'),
            'cat_label': d.get('cat_label'),
            'filed':     d.get('filed'),
            'amount':    d.get('amount'),
            'doc_num':   d.get('doc_num'),
            'clerk_url': d.get('clerk_url'),
            'score':     d.get('score',0),
        } for d in docs]
        prop['clerk_score']  = max(d.get('score',0) for d in docs)
        prop['clerk_docs']   = clerk_docs
        prop['clerk_types']  = list(set(d.get('cat_label','') for d in docs if d.get('cat_label')))

    # ── Cross-enrich from AllRes + DataExport for any remaining gaps ───
    prop = enrich_r(rn, prop)

    # ── Score calculation ──────────────────────────────────────────────
    df = set(prop['distress_factors'])
    base = 0
    if 'tax'   in df: base = max(base, prop.get('tax_score', 0))
    if 'fire'  in df: base = max(base, prop.get('fire_score', 0))
    if 'clerk' in df: base = max(base, prop.get('clerk_score', 0))

    combo_bonus = 0
    if df >= {'fire','tax','clerk'}: combo_bonus += COMBO[('fire','tax','clerk')]
    if df >= {'fire','tax'}:         combo_bonus += COMBO[('fire','tax')]
    if df >= {'fire','clerk'}:       combo_bonus += COMBO[('fire','clerk')]
    if df >= {'tax','clerk'}:        combo_bonus += COMBO[('tax','clerk')]

    prop['score']       = min(100, base + combo_bonus)
    prop['combo_bonus'] = combo_bonus
    prop['source_count'] = len(df)

    # ── Distress label ─────────────────────────────────────────────────
    if len(df) == 3:
        prop['distress_label'] = 'Triple Threat'
        if 'Triple Threat' not in prop['flags']: prop['flags'].insert(0, 'Triple Threat')
    elif len(df) == 2:
        prop['distress_label'] = 'Multi-Factor'
        if 'Multi-Factor' not in prop['flags']: prop['flags'].insert(0, 'Multi-Factor')
    else:
        prop['distress_label'] = list(df)[0].replace('tax','Tax Delinquent').replace('fire','Fire Damage').replace('clerk','Clerk Record')

    unified.append(prop)

# ── Also add clerk records with no R-number match ─────────────────────────
for rec in clerk_no_r:
    prop = {
        'r_number': '',
        'situs_address': rec.get('prop_address',''),
        'situs_city':    rec.get('prop_city',''),
        'situs_state':   rec.get('prop_state','TX'),
        'situs_zip':     rec.get('prop_zip',''),
        'owner_name':    rec.get('owner',''),
        'mail_address':  rec.get('mail_address',''),
        'mail_city':     rec.get('mail_city',''),
        'mail_state':    rec.get('mail_state',''),
        'mail_zip':      rec.get('mail_zip',''),
        'assessed_value': None, 'legal_description': '',
        'sources': ['clerk'], 'distress_factors': ['clerk'],
        'flags': rec.get('flags',[]),
        'clerk_docs': [{
            'doc_type': rec.get('doc_type'), 'cat': rec.get('cat'),
            'cat_label': rec.get('cat_label'), 'filed': rec.get('filed'),
            'amount': rec.get('amount'), 'doc_num': rec.get('doc_num'),
            'clerk_url': rec.get('clerk_url'), 'score': rec.get('score',0),
        }],
        'clerk_score':  rec.get('score',0),
        'clerk_types':  [rec.get('cat_label','')] if rec.get('cat_label') else [],
        'score':        rec.get('score',0),
        'combo_bonus':  0,
        'source_count': 1,
        'distress_label': 'Clerk Record',
    }
    unified.append(prop)

# ── Sort by score desc ────────────────────────────────────────────────────
unified.sort(key=lambda x: x['score'], reverse=True)

# ── Stats ─────────────────────────────────────────────────────────────────
triple   = sum(1 for r in unified if r['source_count']==3)
multi    = sum(1 for r in unified if r['source_count']==2)
single   = sum(1 for r in unified if r['source_count']==1)
hot      = sum(1 for r in unified if r['score']>=70)
with_addr = sum(1 for r in unified if r['situs_address'])

print(f"\n{'='*50}")
print(f"UNIFIED LEADS: {len(unified):,} total properties")
print(f"  Triple Threat (all 3 sources): {triple}")
print(f"  Multi-Factor (2 sources):      {multi}")
print(f"  Single source:                 {single}")
print(f"  Hot leads (score ≥70):         {hot}")
print(f"  With address:                  {with_addr}")
print(f"\nTop 5 by score:")
for r in unified[:5]:
    print(f"  [{r['score']}] {r['situs_address']} | {r['owner_name']} | sources: {r['sources']} | bonus: +{r['combo_bonus']}")

# ── Save ──────────────────────────────────────────────────────────────────
os.makedirs('/home/claude/output/dashboard', exist_ok=True)
with open(OUT_PATH, 'w') as f:
    json.dump(unified, f, separators=(',',':'), default=str)
print(f"\n✓ Saved: {OUT_PATH}  ({os.path.getsize(OUT_PATH)/1024:.0f} KB)")
