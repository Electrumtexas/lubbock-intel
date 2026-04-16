# Lubbock Intel — Project Context for AI Sessions

Read this file at the start of every session. It captures all architectural decisions,
current state, and pending work so context is never lost between chats.

---

## What This Is

A GitHub Actions-automated distressed property intelligence system for Lubbock County, TX.
Goal: surface motivated sellers from multiple public record sources, stack them into a
unified scored dashboard, and identify off-market opportunities before aggregators catch them.

Owner: Jarrod (jarrod@electrumtexas.com) — real estate investor, Electrum Texas.
Repo: https://github.com/Electrumtexas/lubbock-intel
Dashboard: deployed via GitHub Pages from the `dashboard/` folder.

---

## Data Sources (4 layers)

### 1. County Clerk Records (daily, automated)
- Source: Lubbock County Clerk eRecord portal (Tyler Technologies EagleWeb)
- URL: https://erecord.lubbockcounty.gov
- Script: `scraper/fetch.py`
- Runs: GitHub Actions daily at 7am UTC (2am CT), configurable lookback (default 7 days, max 90)
- Captures: lis pendens, notices of foreclosure/trustee sale, tax deeds, judgments (CCJ/DRJUD),
  federal/IRS/mechanic/HOA/medicaid liens, probate, divorce/family orders
- Address enrichment: LCAD cross-reference via owner name (~72% match rate on 7-day pulls,
  lower on longer lookbacks due to more business/entity filings)
- Output: `dashboard/records.json`, `data/records.json`, `data/ghl_export.csv`
- Note: Judgment amounts in clerk portal show only $25 recording fee — actual amount is in scanned
  image only and cannot be automated

### 2. Tax Delinquent (monthly, manual upload)
- Source: LCAD monthly delinquent residential xlsx (emailed to Jarrod)
- Script: `scraper/tax_delinquent.py`
- Upload process: rename file to `DelinquentResidential_current.xlsx`, commit to `data/`
- Captures: R-number, owner, situs address, mailing address, balance owed, tax year, assessed value
- Future: Gmail API auto-pull planned but not built yet
- Output: `dashboard/tax_delinquent.json`
- Scale: ~9,059 residential properties with unpaid taxes

### 3. Fire Damage (manual, FOIA)
- Source: City of Lubbock open records request (no automation path — city requires formal FOIA)
- Script: `scraper/fire_damage.py`
- Upload process: rename file to `StructureFires_current.xlsx`, commit to `data/`
- Output: `dashboard/fire_damage.json`
- Scale: ~159 structure fire incidents

### 4. LCAD Reference Data (quarterly, manual)
- `data/AllRes_current.xlsx` — 107,000+ all residential properties (address backfill reference)
- `data/DataExport_current.txt` — owner names, mailing addresses
- Used by `build_unified.py` for address enrichment across all three sources

---

## Unified Lead Scoring (`scraper/build_unified.py`)

- Master key: R-number (LCAD QuickRefID) joins all three sources
- Total properties: ~9,166 as of last build
- 32 fire+tax combo properties, all scoring 100
- Distress labels: "Triple Threat" (3 sources), "Multi-Factor" (2 sources)
- Combo bonuses: fire+tax scores higher than fire+judgment
- Source badges shown inline: CLERK / TAX / FIRE
- AllRes bulk LCAD file (`data/AllRes_current.xlsx`) backfills missing addresses

### Scoring Logic (fetch.py)
- Base: 30
- +10 per distress flag
- +20 if LP + foreclosure combo
- +15 if amount > $100K
- +10 if amount > $50K
- +5 if new this week
- +5 if has address

### Known Scoring Issues (to fix)
- Score saturation: 100 is too easy — needs logistic-style reweighting
- Entity/trust ownership (LLC, Corp) should be flagged as its own signal, not just a match failure
- Divorce records not yet added as a source

---

## MLS Integration (built, awaiting credentials)

- Provider: FlexMLS / Spark REST API
- Coverage: Lubbock area only (through LAR — Lubbock Association of Realtors)
- Key contacts:
  - **AJ Johnson** — broker at Our Texas Real Estate — signed IDX agreement
  - **Tonya** — Lubbock Association of Realtors — mls@lubbockrealtors.com — LAR approval
- 3 credential tiers (all stored as GitHub Secrets):
  - **BBO (Broker Back Office)** — full access, all fields, private remarks — use this first
  - **VOW (Virtual Office Website)** — extended fields, price history, DOM details
  - **IDX (Internet Data Exchange)** — public listings, basic fields, 1,500 req/5 min
  - Note: Jarrod requested BBO + IDX; NOT VOW (VOW requires sign-in for public websites)
- Scripts:
  - `scraper/mls_lookup.py` — Spark API client, auth (BBO→VOW→IDX priority), address lookup, bulk fetch
  - `scraper/mls_enrich.py` — cross-references all leads against MLS, flags on-market vs off-market
- Cache: `data/mls_cache.json` (6-hour TTL, gitignored — regenerated each run)
- Output: `dashboard/mls_enriched.json`
- Cost: $50/month Spark developer fee to FBS

### MLS Scoring Bonuses (planned — not yet implemented)
| Condition | Bonus |
|---|---|
| Active listing on distressed property | Small (they want out, but someone got there) |
| Expired listing + tax delinquent | Large (tried to sell, failed, still can't pay taxes) |
| Withdrawn listing + pre-foreclosure | Highest (pulled off market, foreclosure still proceeding) |
| Long DOM 60/90/120+ days | Bonus (motivated, publicly acknowledged) |
| Price reduction history | Bonus (publicly desperate) |
| Off-market (no MLS match at all) | No penalty — this is the prize |

### Key MLS Fields to Pull
- `DaysOnMarket`, `ListPrice`, `BuildingAreaTotal`
- `PublicRemarks` (scan for motivated seller keywords)
- `StandardStatus`, `ListingContractDate`
- Last sold price + date (equity estimation when combined with LCAD assessed value)

### MLS GitHub Secrets (add in repo Settings → Secrets → Actions)
```
SPARK_BBO_CLIENT_ID, SPARK_BBO_CLIENT_SECRET, SPARK_BBO_API_KEY
SPARK_BBO_ACCESS_TOKEN, SPARK_BBO_REFRESH_TOKEN, SPARK_BBO_OPENID_TOKEN
SPARK_VOW_CLIENT_ID, SPARK_VOW_CLIENT_SECRET, SPARK_VOW_API_KEY
SPARK_VOW_ACCESS_TOKEN, SPARK_VOW_REFRESH_TOKEN, SPARK_VOW_OPENID_TOKEN
SPARK_IDX_CLIENT_ID, SPARK_IDX_CLIENT_SECRET, SPARK_IDX_API_KEY
SPARK_IDX_ACCESS_TOKEN, SPARK_IDX_REFRESH_TOKEN, SPARK_IDX_OPENID_TOKEN
```

### Helper files for credential setup (project root, NOT committed to GitHub)
- `FILL_IN_SECRETS.txt` — fill in credentials here in Notepad
- `push_secrets.py` — reads that file and pushes all secrets via `gh` CLI
  (GitHub CLI required: https://cli.github.com/)

---

## GitHub Actions Workflow (`.github/workflows/scrape.yml`)

Steps in order:
1. Run clerk scraper (`fetch.py`)
2. Process tax delinquent if file present
3. Process fire damage if file present
4. LCAD enrichment pass (`lcad_enrich_incomplete.py`, up to 200 records/day, configurable)
5. Build unified leads (`build_unified.py`)
6. MLS enrichment (`mls_enrich.py`) ← new step, awaiting secrets
7. Commit all outputs and push
8. Deploy dashboard to GitHub Pages

Triggers: daily cron at 7am UTC (2am CT) + manual `workflow_dispatch`

---

## File Structure

```
lubbock-intel/
├── .github/workflows/scrape.yml   # GitHub Actions pipeline
├── dashboard/
│   ├── index.html                 # Dashboard UI
│   ├── records.json               # Clerk records output
│   ├── tax_delinquent.json        # Tax delinquent output
│   ├── fire_damage.json           # Fire damage output
│   ├── unified_leads.json         # Merged/scored leads
│   └── mls_enriched.json          # MLS-enriched output (new)
├── data/
│   ├── AllRes_current.xlsx        # LCAD bulk file (107K+ properties, address backfill)
│   ├── DataExport_current.txt     # LCAD data export (owner names, mailing addresses)
│   ├── DelinquentResidential_current.xlsx  # Monthly tax delinquent (manual upload)
│   ├── ghl_export.csv             # GoHighLevel import CSV
│   ├── cad_cache.json             # CAD address lookup cache (committed)
│   ├── lcad_detail_cache.json     # LCAD property detail cache (committed — expensive)
│   └── mls_cache.json             # MLS listing cache (gitignored — regenerated)
├── scraper/
│   ├── fetch.py                   # Clerk scraper (main)
│   ├── tax_delinquent.py          # Tax delinquent processor
│   ├── fire_damage.py             # Fire damage processor
│   ├── lcad_lookup.py             # LCAD address enrichment
│   ├── lcad_enrich_incomplete.py  # Incremental fill for missing addresses
│   ├── build_unified.py           # Merges all sources, scoring
│   ├── scoring.py                 # Scoring weights and logic
│   ├── mls_lookup.py              # FlexMLS Spark API client
│   ├── mls_enrich.py              # MLS cross-reference
│   ├── requirements.txt
│   └── .env.example               # Credential template (safe to commit)
├── FILL_IN_SECRETS.txt            # Credential entry form — DELETE after use, never commit
├── push_secrets.py                # Pushes secrets to GitHub via gh CLI
├── .gitignore
├── CLAUDE.md                      # ← this file
└── README.md
```

---

## Monetization Plan (not yet launched)

### Tiers
| Tier | Price | Description |
|---|---|---|
| One-time pull | $197 | Full current dataset — every scored lead in Lubbock County right now |
| Pro subscription | $97–$147/mo | Weekly updated scored lead list, CSV export, GHL-ready format |
| Bespoke county build | $2,500–$5,000 build + $297–$497/mo retainer | Custom deployment for another investor's county |

### Target Communities
- SubTo (Subject To investing community)
- TTP (Talk To People / Pace Morby community)
- Lubbock Facebook real estate investor groups

### 30-Day Launch Plan
- Week 1 — Build proof assets: one-page PDF sample report, 60-sec screen-record demo, simple landing page
- Week 2 — Seed communities: post value/data/insights, no pitch, reply to every comment
- Week 3 — Direct outreach: DM 20 engaged people, send sample PDF, offer $197 one-time pull, close 3–5
- Week 4 — Convert to recurring: follow up, get case study, push upgrade to monthly

### Key Principle
Sell manually first. Google Form → Venmo/Stripe → email delivery. Close first $1,000 before automating anything.

---

## Pending Work (priority order)

1. **MLS credentials** — fill in `FILL_IN_SECRETS.txt`, run `push_secrets.py` (needs `gh` CLI)
2. **Scoring rebalance** — logistic-style cap so 100 isn't so easy to hit
3. **MLS scoring bonuses** — implement expired/withdrawn/DOM bonuses (see table above)
4. **Incomplete data workflow** — UI/logic for manual lookup queue
5. **Divorce records** — add as new source via Lubbock County District Clerk
6. **Gmail API auto-pull** — auto-fetch monthly tax delinquent email
7. **Monetization launch** — proof assets → community seeding → direct outreach
8. **Multi-market expansion** — Midland, Abilene, Amarillo, Stephenville TX; Chattanooga TN

---

## Key Technical Notes

- All credential values live in **GitHub Secrets only** — never in files committed to the repo
- `data/mls_cache.json` and `data/cad_cache.json` are gitignored (regenerated each run)
- `data/lcad_detail_cache.json` IS committed — expensive to regenerate, each R-number fetched once
- Dashboard served as static site via GitHub Pages from the `dashboard/` folder
- LCAD enrichment rate-limits: lubbockcad.org throttles after ~30 requests; cache mitigates this
- 90-day lookback pulls work via chunked search but take 20–30 minutes
- FlexMLS Spark API base: `https://sparkapi.com/v1`
- Spark token refresh endpoint: `https://sparkplatform.com/oauth2/grant`
- Spark docs: https://sparkplatform.com/docs
- LAR contact: mls@lubbockrealtors.com (Tonya)

## Jarrod's Tools Stack (for context)
- CRM: GoHighLevel (GHL) — leads import via GHL CSV export
- Lead gen: Closer Control, Batch Leads, Batch Dialer, SMRTPHONE.IO
- Data: InvestorLift, PropStream (free account), Appraiva DFD, Rehab Estimator Pro
