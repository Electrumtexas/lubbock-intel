"""
build_unified.py v3 — tax + fire + clerk, uses scoring.py weights
"""
import json, re, pandas as pd, os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from scoring import score_tax_delinquent, score_fire, score_clerk, apply_combo_bonus, completeness, REQUIRED_FIELDS

ALLRES_PATH     = 'data/AllRes_current.xlsx'
DATAEXPORT_PATH = 'data/DataExport_current.txt'
TAX_PATH        = 'dashboard/tax_delinquent.json'
FIRE_PATH       = 'dashboard/fire_damage.json'
CLERK_PATH      = 'dashboard/records.json'
OUT_PATH        = 'dashboard/unified_leads.json'
TAX_OUT_PATH    = 'dashboard/tax_delinquent.json'
FIRE_OUT_PATH   = 'dashboard/fire_damage.json'

print("Loading reference files...")
df_all = pd.read_excel(ALLRES_PATH, usecols=['QuickRefID','SitusAddress','LegalDescription','FinalTotal'])
df_all['QuickRefID'] = df_all['QuickRefID'].astype(str).str.strip()
df_all['_hc'] = df_all['SitusAddress'].astype(str).str.contains(',')
df_all = df_all.sort_values(['QuickRefID','_hc'],ascending=[True,False]).drop_duplicates('QuickRefID',keep='first').drop(columns='_hc')
allres = df_all.
