"""
scoring.py — Unified scoring engine for lubbock-intel

All score calculations live here. Import and call score_record() 
or the individual functions. Cap is always 100.

Distress factor priority (most → least likely to sell):
  1. LP / Pre-Foreclosure  — time-pressured, legal deadline
  2. Fire Damage           — physical distress, often uninsured or underinsured
  3. Probate / Estate      — heirs want to liquidate, no emotional attachment
  4. Tax Delinquent        — compounding penalties, eventual forced sale
  5. Judgment Lien         — financial pressure, varies heavily by amount
  6. Mechanic / HOA Lien   — lowest urgency, often resolved without sale

Dollar amounts create tiers within each category.
Flag bonuses are additive on top of base scores.
Multi-source combo bonuses are applied at the unified merge layer.
"""

# ── Combo bonuses (applied in build_unified.py) ───────────────────────────
COMBO_BONUS = {
    frozenset(['fire', 'tax']):           30,
    frozenset(['fire', 'clerk']):         25,
    frozenset(['tax',  'clerk']):         15,
    frozenset(['fire', 'tax', 'clerk']):  35,   # additive on top of pair bonuses
}

# ── Flag bonuses (additive, applied within a single source) ──────────────
FLAG_BONUS = {
    'Absentee Owner': 5,
    'New This Week':  3,
    'LLC/Corp Owner': 5,
}


# ════════════════════════════════════════════════════════════════════════════
# CLERK RECORD SCORING
# ════════════════════════════════════════════════════════════════════════════

def score_clerk(cat, amount, flags=None, doc_type=''):
    """
    Score a single clerk record.
    cat:    'foreclosure' | 'judgment' | 'lien' | 'tax' | 'probate' | 'other' | 'release'
    amount: dollar amount (may be 0 or None for judgment/probate since only recording fee stored)
    flags:  list of flag strings
    """
    flags = flags or []
    amt   = float(amount or 0)

    # ── Base by category + amount ─────────────────────────────────────────
    if cat == 'foreclosure':
        # LP / Pre-Foreclosure — most urgent
        # Amount here is often the loan balance from the filing
        if amt >= 150_000:   base = 75
        elif amt >= 50_000:  base = 65
        elif amt > 0:        base = 58
        else:                base = 55   # no amount — still strong signal
        # Extra if BOTH LP and foreclosure flag on same property
        if 'Pre-Foreclosure' in flags and 'Lis Pendens' in flags:
            base = min(100, base + 10)

    elif cat == 'probate':
        base = 55
        # Deceased estate — heirs more motivated
        if any(x in doc_type.upper() for x in ["EST OF", "DEC'D", "ESTATE OF"]):
            base += 5

    elif cat == 'judgment':
        # NOTE: clerk portal only stores $25 recording fee, not actual judgment.
        # Treat as fixed tiers since real amount is in scanned image only.
        # We give moderate scores — can't verify size without image parsing.
        if amt >= 100_000:   base = 62
        elif amt >= 25_000:  base = 52
        elif amt >= 5_000:   base = 40
        elif amt > 100:      base = 35   # filter out $25 recording fees
        else:                base = 30   # no real amount known

    elif cat == 'tax':
        # IRS / Federal tax liens — serious
        if amt >= 50_000:    base = 58
        elif amt >= 10_000:  base = 48
        elif amt > 100:      base = 35
        else:                base = 28

    elif cat == 'lien':
        # HOA / Mechanic — lowest urgency
        # But large amounts still matter
        if amt >= 25_000:    base = 45
        elif amt >= 5_000:   base = 35
        elif amt > 100:      base = 25
        else:                base = 20

    elif cat == 'release':
        base = 10   # release = resolved, minimal motivated seller signal

    else:
        # 'other', unknown
        base = 25

    # ── Flag bonuses ──────────────────────────────────────────────────────
    bonus = sum(FLAG_BONUS.get(f, 0) for f in flags)

    return min(100, base + bonus)


# ════════════════════════════════════════════════════════════════════════════
# TAX DELINQUENT SCORING
# ════════════════════════════════════════════════════════════════════════════

def score_tax_delinquent(balance, flags=None):
    """
    Score based on actual balance owed.
    Percentile breakpoints from April 2026 Lubbock dataset:
      25th: $790    50th: $1,823    75th: $3,883
      90th: $7,298  95th: $10,673
    """
    flags  = flags or []
    balance = float(balance or 0)

    if balance < 500:          base = 25
    elif balance < 1_823:      base = 35
    elif balance < 3_883:      base = 48
    elif balance < 7_298:      base = 58
    elif balance < 15_000:     base = 68
    elif balance < 30_000:     base = 75
    else:                      base = 82   # extreme — forced sale territory

    bonus = sum(FLAG_BONUS.get(f, 0) for f in flags)
    return min(100, base + bonus)


# ════════════════════════════════════════════════════════════════════════════
# FIRE DAMAGE SCORING
# ════════════════════════════════════════════════════════════════════════════

def score_fire(is_residential, property_use='', flags=None):
    """
    Residential fire = high distress signal.
    Non-residential = much lower — owner may be a business with insurance.
    """
    flags = flags or []
    base  = 62 if is_residential else 30
    bonus = sum(FLAG_BONUS.get(f, 0) for f in flags)
    return min(100, base + bonus)


# ════════════════════════════════════════════════════════════════════════════
# COMBO SCORE (used in build_unified.py)
# ════════════════════════════════════════════════════════════════════════════

def apply_combo_bonus(base_score, sources):
    """
    sources: set of strings e.g. {'fire', 'tax'}
    Returns new score with combo bonuses applied (capped at 100).
    """
    src = frozenset(sources)
    bonus = 0

    # Check all three first (additive on top of pair bonuses)
    triple = frozenset(['fire', 'tax', 'clerk'])
    if triple <= src:
        bonus += COMBO_BONUS[triple]

    # Then pairs
    for pair, pts in COMBO_BONUS.items():
        if len(pair) == 2 and pair <= src:
            bonus += pts

    return min(100, base_score + bonus)


# ════════════════════════════════════════════════════════════════════════════
# COMPLETENESS CHECK
# ════════════════════════════════════════════════════════════════════════════

REQUIRED_FIELDS = [
    'owner_name',
    'situs_address', 'situs_city', 'situs_state', 'situs_zip',
    'mail_address', 'mail_city', 'mail_state', 'mail_zip',
]

def completeness(record):
    """
    Returns (is_complete: bool, missing: list[str])
    A record is complete when all REQUIRED_FIELDS are non-empty.
    """
    missing = [f for f in REQUIRED_FIELDS if not str(record.get(f, '') or '').strip()]
    return len(missing) == 0, missing
