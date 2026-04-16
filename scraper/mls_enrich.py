"""
mls_enrich.py
Cross-reference existing lead records (from fetch.py, tax_delinquent.py, etc.)
with live FlexMLS data to flag which distressed properties are already listed.

Why this matters:
  - If a motivated seller lead is already on MLS, they've engaged an agent.
    That's a signal — but also competition. You can see their ask price.
  - If a high-score lead has NO MLS listing, it's likely off-market.
    That's your best opportunity window.

Run standalone:
  cd <project root>
  python scraper/mls_enrich.py

Or import:
  from mls_enrich import enrich_leads_with_mls
"""

import json
import logging
import os
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from mls_lookup import MLSClient

log = logging.getLogger("mls_enrich")

# Input/output paths
CLERK_RECORDS_PATH  = Path("dashboard/records.json")
TAX_RECORDS_PATH    = Path("dashboard/tax_delinquent.json")
UNIFIED_PATH        = Path("dashboard/unified_leads.json")
MLS_OUT_PATH        = Path("dashboard/mls_enriched.json")


def enrich_leads_with_mls(leads: list[dict], client: MLSClient = None) -> list[dict]:
    """
    Attach MLS data to a list of lead records.

    Fetches the full area listing set once, builds an address index,
    then runs each lead through it — so this is one API batch call,
    not one call per lead.

    Args:
      leads:   List of lead dicts (from records.json, unified_leads.json, etc.)
      client:  Optional pre-initialized MLSClient. Created if not provided.

    Returns:
      Same list with mls_* fields added to each record.
    """
    if not leads:
        return leads

    if client is None:
        client = MLSClient()

    log.info(f"Fetching MLS area listings to build address index...")
    # Fetch active + pending listings
    # Note: ActiveUnderContract was removed — it hit the 5,000 record cap and
    # appeared to be returning historical data rather than current listings.
    listings = []
    for status in ("Active", "Pending"):
        try:
            batch = client.fetch_area_listings(status=status, max_records=5000)
            listings.extend(batch)
            log.info(f"  {status}: {len(batch)} listings")
        except Exception as e:
            log.warning(f"  Could not fetch {status} listings: {e}")

    if not listings:
        log.warning("No MLS listings retrieved — skipping enrichment")
        return leads

    index = client.build_address_index(listings)
    log.info(f"Address index built: {len(index)} entries")

    enriched_count = 0
    for lead in leads:
        lead = client.enrich_lead(lead, index)
        if lead.get("mls_found"):
            enriched_count += 1

    log.info(f"MLS enrichment complete: {enriched_count}/{len(leads)} leads matched")
    return leads


def load_clerk_records() -> list[dict]:
    """Load records from the clerk scraper (fetch.py output)."""
    if not CLERK_RECORDS_PATH.exists():
        return []
    data = json.loads(CLERK_RECORDS_PATH.read_text())
    return data.get("records", []) if isinstance(data, dict) else data


def load_unified_leads() -> list[dict]:
    """Load the unified leads file (tax + fire combined)."""
    if not UNIFIED_PATH.exists():
        return []
    data = json.loads(UNIFIED_PATH.read_text())
    return data if isinstance(data, list) else []


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    log.info("Loading lead sources...")
    clerk_leads   = load_clerk_records()
    unified_leads = load_unified_leads()

    log.info(f"  Clerk records:  {len(clerk_leads)}")
    log.info(f"  Unified leads:  {len(unified_leads)}")

    all_leads = clerk_leads + unified_leads
    if not all_leads:
        log.warning("No leads found. Run fetch.py and/or build_unified.py first.")
        return

    client = MLSClient()
    enriched = enrich_leads_with_mls(all_leads, client=client)

    # Split back into sources
    n_clerk   = len(clerk_leads)
    e_clerk   = enriched[:n_clerk]
    e_unified = enriched[n_clerk:]

    # Stats
    mls_found    = sum(1 for r in enriched if r.get("mls_found"))
    off_market   = sum(1 for r in enriched if not r.get("mls_found") and r.get("prop_address") or r.get("situs_address"))
    high_score_off = sum(
        1 for r in enriched
        if not r.get("mls_found") and r.get("score", 0) >= 70
    )

    log.info(f"\n{'='*55}")
    log.info(f"Total leads enriched : {len(enriched)}")
    log.info(f"Already on MLS       : {mls_found}")
    log.info(f"Off-market           : {off_market}")
    log.info(f"High-score off-market: {high_score_off}  ← best opportunities")
    log.info(f"{'='*55}")

    # Save combined output
    output = {
        "enriched_at":       datetime.utcnow().isoformat() + "Z",
        "total":             len(enriched),
        "mls_found":         mls_found,
        "off_market":        off_market,
        "high_score_off_market": high_score_off,
        "clerk_records":     e_clerk,
        "unified_leads":     e_unified,
    }

    MLS_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    MLS_OUT_PATH.write_text(json.dumps(output, indent=2, default=str))
    log.info(f"Saved → {MLS_OUT_PATH}")

    # Also update unified_leads.json in place with mls fields
    if e_unified:
        UNIFIED_PATH.write_text(json.dumps(e_unified, separators=(",", ":"), default=str))
        log.info(f"Updated unified_leads.json with MLS fields")

    # Print top off-market opportunities
    top_off = sorted(
        [r for r in enriched if not r.get("mls_found") and r.get("score", 0) >= 60],
        key=lambda x: x.get("score", 0),
        reverse=True,
    )[:10]

    if top_off:
        log.info("\nTop off-market opportunities:")
        for r in top_off:
            addr = r.get("prop_address") or r.get("situs_address") or "unknown"
            owner = r.get("owner_name") or r.get("owner") or ""
            score = r.get("score", 0)
            sources = r.get("sources") or ([r.get("doc_type")] if r.get("doc_type") else [])
            log.info(f"  [{score}] {addr} | {owner} | {sources}")


if __name__ == "__main__":
    main()
