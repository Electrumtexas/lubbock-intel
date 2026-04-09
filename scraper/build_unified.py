"""
build_unified.py v2 — uses new scoring.py weights
"""
import json, pandas as pd, os, sys
from pathlib import Path
sys.path.insert(0, '/home/claude/build/scraper')
from scoring import score_tax_delinquent, score_fire, apply_combo_bonus, completeness, REQUIRED_FIELDS

ALLRES_PATH     = '/mnt/user-data/uploads/AllRes04042026.xlsx'
DATAEXPORT_PATH = '/mnt/user-data/uploads/DataExport2633759.txt'
TAX_PATH        = '/home/claude/output/tax_delinquent.json'
FIRE_PATH       = '/home/claude/output/fire_damage.json'
OUT_PATH        = '/home/claude/build/dashboard/unified_leads.json'
TAX_OUT_PATH    = '/home/claude/build/dashboard/tax_delinquent.json'
FIRE_OUT_PATH   = '/home/claude/build/dashboard/fire_damage.json'

print("Loading reference files...")
df_all = pd.read_excel(ALLRES_PATH, usecols=['QuickRefID','SitusAddress','LegalDescription','FinalTotal'])
df_all['QuickRefID'] = df_all['QuickRefID'].astype(str).str.strip()
df_all['_hc'] = df_all['SitusAddress'].astype(str).str.contains(',')
df_all = df_all.sort_values(['QuickRefID','_hc'],ascending=[True,False]).drop_duplicates('QuickRefID',keep='first').drop(columns='_hc')
allres = df_all.set_index('QuickRefID').to_dict('index')

df_de = pd.read_csv(DATAEXPORT_PATH, usecols=['Quick Ref','Owner Name','Addr1','Addr2','City','State','Zip'], low_memory=False, dtype=str)
df_de = df_de[df_de['Quick Ref'].str.startswith('R',na=False)].drop_duplicates('Quick Ref')
dedata = df_de.set_index('Quick Ref').to_dict('index')
print(f"  AllRes: {len(allres):,}  DataExport: {len(dedata):,}")

def parse_situs(s):
    parts=[p.strip() for p in str(s or '').split(',')]
    a,c,st,z='','','TX',''
    if len(parts)>=3: a=parts[0];c=parts[1];sz=parts[2].split();st=sz[0] if sz else 'TX';z=sz[1] if len(sz)>1 else ''
    elif len(parts)==2: a,c=parts[0],parts[1]
    elif parts: a=parts[0]
    return a,c,st,z

def enrich(r, base=None):
    out = base.copy() if base else {'r_number':r,'situs_address':'','situs_city':'','situs_state':'TX','situs_zip':'','owner_name':'','mail_address':'','mail_city':'','mail_state':'','mail_zip':'','assessed_value':None,'legal_description':'','address_source':'none'}
    r = str(r or '').strip()
    if r in allres:
        ar=allres[r]; a,c,st,z=parse_situs(ar.get('SitusAddress',''))
        if not out.get('situs_address'): out['situs_address']=a;out['situs_city']=c;out['situs_state']=st;out['situs_zip']=z
        if not out.get('legal_description'): out['legal_description']=str(ar.get('LegalDescription','') or '').strip()
        if not out.get('assessed_value'):
            val=ar.get('FinalTotal'); out['assessed_value']=int(val) if pd.notna(val) else None
        out['address_source']='allres'
    if r in dedata:
        de=dedata[r]
        if not out.get('owner_name'): out['owner_name']=str(de.get('Owner Name','') or '').strip()
        if not out.get('mail_address'):
            a1=str(de.get('Addr1','') or '').strip(); a2=str(de.get('Addr2','') or '').strip()
            out['mail_address']=(a1+(' '+a2 if a2 else '')).strip()
            out['mail_city']=str(de.get('City','') or '').strip()
            out['mail_state']=str(de.get('State','') or '').strip()
            out['mail_zip']=str(de.get('Zip','') or '').strip()
        out['address_source']='full' if out['address_source']=='allres' else 'dataexport'
    return out

print("Loading source JSON files...")
with open(TAX_PATH) as f: tax_list = json.load(f)
with open(FIRE_PATH) as f: fire_list = json.load(f)
print(f"  Tax: {len(tax_list):,}  Fire: {len(fire_list):,}")

# Re-score tax with new weights
print("Re-scoring tax delinquent...")
for r in tax_list:
    old = r.get('score',0)
    r['score'] = score_tax_delinquent(r.get('balance_owed',0), r.get('flags',[]))

# Re-score fire with new weights  
print("Re-scoring fire damage...")
for r in fire_list:
    r['score'] = score_fire(r.get('is_residential',True), r.get('property_use',''), r.get('flags',[]))

# Add completeness flags
for r in tax_list + fire_list:
    complete, missing = completeness(r)
    r['is_complete'] = complete
    r['missing_fields'] = missing

# Save updated individual files
os.makedirs('/home/claude/build/dashboard', exist_ok=True)
with open(TAX_OUT_PATH,'w') as f: json.dump(tax_list, f, separators=(',',':'), default=str)
with open(FIRE_OUT_PATH,'w') as f: json.dump(fire_list, f, separators=(',',':'), default=str)
print(f"  Saved updated tax ({len(tax_list):,}) and fire ({len(fire_list):,})")

# Build unified
print("Building unified leads...")
tax_by_r  = {r['r_number']: r for r in tax_list if r.get('r_number')}
fire_by_r = {}
for r in fire_list:
    rn = r.get('r_number','')
    if rn: fire_by_r.setdefault(rn,[]).append(r)

all_rnums = set(tax_by_r.keys()) | set(fire_by_r.keys())
unified = []

for rn in all_rnums:
    has_tax  = rn in tax_by_r
    has_fire = rn in fire_by_r
    sources  = []
    prop = {'r_number':rn,'situs_address':'','situs_city':'','situs_state':'TX','situs_zip':'','owner_name':'','mail_address':'','mail_city':'','mail_state':'','mail_zip':'','assessed_value':None,'legal_description':'','address_source':'none','sources':[],'flags':[],'distress_factors':[]}

    base_score = 0
    if has_tax:
        td=tax_by_r[rn]; sources.append('tax'); prop['distress_factors'].append('tax')
        for fld in ['situs_address','situs_city','situs_state','situs_zip','owner_name','mail_address','mail_city','mail_state','mail_zip','assessed_value','legal_description']:
            if td.get(fld) and not prop.get(fld): prop[fld]=td[fld]
        for fl in (td.get('flags') or []):
            if fl not in prop['flags']: prop['flags'].append(fl)
        prop['tax_balance']=td.get('balance_owed',0); prop['tax_score']=td.get('score',0)
        prop['tax_year']=td.get('tax_year'); prop['property_type']=td.get('property_type_desc','')
        prop['data_date']=td.get('data_date',''); base_score=max(base_score,td['score'])

    if has_fire:
        fires=sorted(fire_by_r[rn],key=lambda x:x.get('incident_date',''),reverse=True)
        latest=fires[0]; sources.append('fire'); prop['distress_factors'].append('fire')
        if 'Fire Damage' not in prop['flags']: prop['flags'].append('Fire Damage')
        for fld in ['situs_address','situs_city','situs_state','situs_zip','owner_name','mail_address','mail_city','mail_state','mail_zip','assessed_value','legal_description']:
            if latest.get(fld) and not prop.get(fld): prop[fld]=latest[fld]
        prop['fire_score']=latest.get('score',0); prop['fire_latest']=latest.get('incident_date','')
        prop['fire_incidents']=[{'incident_number':f.get('incident_number'),'incident_date':f.get('incident_date'),'incident_type':f.get('incident_type'),'property_use':f.get('property_use'),'is_residential':f.get('is_residential',True)} for f in fires]
        prop['fire_property_use']=latest.get('property_use',''); prop['is_residential']=latest.get('is_residential',True)
        base_score=max(base_score,latest['score'])

    prop['sources']=sources
    prop = enrich(rn, prop)  # cross-enrich any remaining gaps
    prop['score'] = apply_combo_bonus(base_score, set(prop['distress_factors']))
    prop['combo_bonus'] = prop['score'] - base_score
    prop['source_count'] = len(set(prop['distress_factors']))

    if prop['source_count']==3:
        prop['distress_label']='Triple Threat'
        if 'Triple Threat' not in prop['flags']: prop['flags'].insert(0,'Triple Threat')
    elif prop['source_count']==2:
        prop['distress_label']='Multi-Factor'
        if 'Multi-Factor' not in prop['flags']: prop['flags'].insert(0,'Multi-Factor')
    else:
        prop['distress_label']={'tax':'Tax Delinquent','fire':'Fire Damage','clerk':'Clerk Record'}.get(list(set(prop['distress_factors']))[0],'')

    # Completeness
    complete, missing = completeness(prop)
    prop['is_complete'] = complete; prop['missing_fields'] = missing

    unified.append(prop)

unified.sort(key=lambda x: x['score'], reverse=True)

# Stats
triple = sum(1 for r in unified if r['source_count']==3)
multi  = sum(1 for r in unified if r['source_count']==2)
hot    = sum(1 for r in unified if r['score']>=70)
at100  = sum(1 for r in unified if r['score']==100)
incomplete = sum(1 for r in unified if not r['is_complete'])
print(f"\n{'='*50}")
print(f"Unified: {len(unified):,} | Triple: {triple} | Multi: {multi}")
print(f"Score=100: {at100} | Hot≥70: {hot} | Incomplete: {incomplete}")
print(f"Top 5:")
for r in unified[:5]:
    print(f"  [{r['score']}] +{r['combo_bonus']} | {r['situs_address']}, {r['situs_city']} | {r['sources']}")

with open(OUT_PATH,'w') as f: json.dump(unified, f, separators=(',',':'), default=str)
print(f"\n✓ unified_leads.json saved ({os.path.getsize(OUT_PATH)//1024} KB)")
