#!/usr/bin/env python3
"""
hunter_ingest.py — RAIS Advisory weekly lead ingestion via Hunter.io

Strategy: Rotate through a curated list of ICP-matched company domains, pull
contacts via Hunter.io Domain Search, filter by title keywords, deduplicate
against the CRM, auto-score, and POST new leads.

Run manually : python3 hunter_ingest.py
GitHub Action: runs every Monday 9 AM UTC

Required env vars:
  HUNTER_API_KEY  — Hunter.io API key (from hunter.io/api-keys)
  CRM_API_URL     — (optional) defaults to https://rais-crm-api.onrender.com
  LEADS_PER_RUN   — (optional) max new leads per execution, default 25
  DOMAIN_BATCH    — (optional) domains to search per run, default 10
"""

import os, sys, time, logging, hashlib
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("hunter_ingest")

# ── Config ────────────────────────────────────────────────────────────────────

HUNTER_API_KEY  = os.environ.get("HUNTER_API_KEY", "")
CRM_API_URL     = os.environ.get("CRM_API_URL", "https://rais-crm-api.onrender.com").rstrip("/")
LEADS_PER_RUN   = int(os.environ.get("LEADS_PER_RUN", "25"))
DOMAIN_BATCH    = int(os.environ.get("DOMAIN_BATCH", "10"))
MAX_PER_DOMAIN  = int(os.environ.get("MAX_PER_DOMAIN", "2"))   # max leads ingested per company
MIN_ICP_SCORE   = int(os.environ.get("MIN_ICP_SCORE", "35"))   # skip leads below this score

if not HUNTER_API_KEY:
    log.error("HUNTER_API_KEY is not set. Exiting.")
    sys.exit(1)

# ── ICP Target Domains ────────────────────────────────────────────────────────
# Curated list of companies matching RAIS ICP:
# Financial Services | CPG | Healthcare | Manufacturing | Small Business
# Rotates weekly so we cover the full list over time without burning credits.

ICP_DOMAINS = [
    # ── Financial Services ────────────────────────────────────────────────────
    "jpmorgan.com", "goldmansachs.com", "morganstanley.com", "blackrock.com",
    "vanguard.com", "fidelity.com", "schwab.com", "ubs.com",
    "aig.com", "prudential.com", "metlife.com", "principal.com",
    "nuveen.com", "tiaa.org", "lincoln.com", "nationwide.com",
    "usbank.com", "pnc.com", "truist.com", "regions.com",
    "comerica.com", "huntington.com", "synovus.com", "firsthorizon.com",

    # ── Consumer Packaged Goods / Retail ─────────────────────────────────────
    "pg.com", "unilever.com", "nestle.com", "kraftheinz.com",
    "generalmills.com", "kelloggs.com", "campbellsoup.com", "conagra.com",
    "mondelez.com", "hersheys.com", "colgate.com", "reckitt.com",
    "jnj.com", "churchdwight.com", "edgewell.com", "revlon.com",
    "target.com", "kroger.com", "albertsons.com", "supervalu.com",

    # ── Healthcare / Life Sciences ────────────────────────────────────────────
    "unitedhealthgroup.com", "cigna.com", "aetna.com", "humana.com",
    "elevancehealth.com", "centene.com", "molina.com", "magellanhealth.com",
    "cardinalhealth.com", "mckesson.com", "amerisourcebergen.com",
    "bectondickinson.com", "baxter.com", "medtronic.com", "stryker.com",
    "zimmer.com", "hologic.com", "varian.com", "illumina.com",

    # ── Manufacturing / Industrial ────────────────────────────────────────────
    "ge.com", "siemens.com", "honeywell.com", "emerson.com",
    "parker.com", "eaton.com", "rockwellautomation.com", "abb.com",
    "3m.com", "illinois.tool.works", "dover.com", "roper.com",
    "fortive.com", "xylem.com", "watts.com", "moog.com",
    "cummins.com", "paccar.com", "trane.com", "carrier.com",

    # ── Small / Mid-Market across sectors ────────────────────────────────────
    "verint.com", "opentext.com", "tibco.com", "talend.com",
    "informatica.com", "alteryx.com", "qlik.com", "microstrategy.com",
    "dataiku.com", "collibra.com", "alation.com", "ataccama.com",
]

# ── ICP Title Keywords (mirrors scoring in api.py) ────────────────────────────

TITLE_KEYWORDS = [
    # Tier-1 (35 pts)
    "chief risk", "chief data", "chief ai", "head of ai",
    "director of ai governance", "cro", "cdo", "caio",
    # Tier-2 (30 pts)
    "chief digital", "chief compliance", "chief analytics",
    # Tier-3 (28 pts)
    "vp data", "vp analytics", "vp ai", "vice president data",
    "vice president analytics", "director of ai", "director of data science",
    # Tier-4 (22 pts)
    "director of data", "director analytics", "vp strategy", "vp digital",
    # Tier-5 (20 pts)
    "general counsel", "vp legal", "head of legal",
    # Tier-6 (18 pts)
    "ciso", "vp risk", "head of risk",
]

def title_matches_icp(title: str) -> bool:
    t = (title or "").lower()
    return any(kw in t for kw in TITLE_KEYWORDS)

# ── Domain rotation (deterministic by ISO week number) ────────────────────────

def get_this_weeks_domains() -> list[str]:
    """Pick a fresh batch of domains each week, cycling through the full list."""
    import datetime
    week_num = datetime.date.today().isocalendar()[1]
    start = (week_num * DOMAIN_BATCH) % len(ICP_DOMAINS)
    batch = (ICP_DOMAINS + ICP_DOMAINS)[start: start + DOMAIN_BATCH]
    log.info(f"Week {week_num}: searching {len(batch)} domains starting at index {start}")
    return batch

# ── Hunter.io Domain Search ───────────────────────────────────────────────────

def search_domain(domain: str, limit: int = 10) -> list[dict]:
    """
    Call Hunter.io /v2/domain-search for one domain.
    Returns list of email-bearing contacts (with name & title).
    """
    try:
        resp = requests.get(
            "https://api.hunter.io/v2/domain-search",
            params={
                "domain":   domain,
                "api_key":  HUNTER_API_KEY,
                "limit":    limit,
                "type":     "personal",   # personal emails more likely to be decision-makers
            },
            timeout=20,
        )
    except requests.RequestException as exc:
        log.warning(f"  Hunter request failed for {domain}: {exc}")
        return []

    if resp.status_code == 429:
        log.warning("  Hunter.io rate limit hit — pausing 60s")
        time.sleep(60)
        return []
    if resp.status_code != 200:
        log.warning(f"  Hunter returned {resp.status_code} for {domain}: {resp.text[:120]}")
        return []

    data   = resp.json().get("data", {})
    emails = data.get("emails", [])
    org    = data.get("organization") or data.get("company") or domain.split(".")[0].title()
    log.info(f"  {domain}: {len(emails)} contacts found")
    return [{"contact": e, "org": org, "domain": domain} for e in emails]

# ── Map Hunter contact → CRM lead ─────────────────────────────────────────────

def hunter_to_lead(item: dict) -> dict:
    c   = item["contact"]
    org = item["org"]

    first = c.get("first_name", "")
    last  = c.get("last_name", "")
    name  = f"{first} {last}".strip() or c.get("value", "").split("@")[0]

    # Infer company size bucket from Hunter's seniority / department data
    # (Hunter doesn't give headcount; leave blank for manual update)
    return {
        "name":         name,
        "title":        c.get("position", ""),
        "company":      org,
        "email":        c.get("value", ""),
        "linkedin_url": c.get("linkedin", "") or "",
        "phone":        "",
        "source":       "hunter",
        "industry":     "",          # Hunter doesn't return industry; scored from company context
        "company_size": "",
        "ai_maturity":  "",
        "notes":        f"Auto-ingested via Hunter.io domain search ({item['domain']}). "
                        f"Email confidence: {c.get('confidence', '?')}%",
        "status":       "prospect",
    }

# ── Fetch existing CRM leads for deduplication ────────────────────────────────

def get_existing_leads() -> tuple[set, set]:
    try:
        resp = requests.get(f"{CRM_API_URL}/api/leads", timeout=60)
        resp.raise_for_status()
        leads  = resp.json()
        emails = {l.get("email", "").lower().strip() for l in leads if l.get("email")}
        combos = {(l.get("name", "").lower().strip(), l.get("company", "").lower().strip()) for l in leads}
        log.info(f"Loaded {len(leads)} existing CRM leads for deduplication")
        return emails, combos
    except Exception as exc:
        log.warning(f"Could not fetch existing leads ({exc}). Skipping dedup.")
        return set(), set()

# ── Post a lead to the CRM ────────────────────────────────────────────────────

def post_lead(lead: dict):
    try:
        resp = requests.post(
            f"{CRM_API_URL}/api/leads",
            json=lead,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code == 201:
            d = resp.json()
            return d.get("id"), d.get("icp_score"), d.get("icp_tier")
        log.warning(f"  CRM rejected '{lead.get('name')}': HTTP {resp.status_code}")
        return None
    except Exception as exc:
        log.error(f"  Error posting '{lead.get('name')}': {exc}")
        return None

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info(f"=== Hunter.io Ingestion Start | target={LEADS_PER_RUN} leads ===")

    existing_emails, existing_combos = get_existing_leads()
    domains  = get_this_weeks_domains()
    ingested = 0
    skipped  = 0

    for domain in domains:
        if ingested >= LEADS_PER_RUN:
            break

        log.info(f"Searching: {domain}")
        contacts = search_domain(domain, limit=10)

        domain_count = 0   # track how many leads we take from this domain

        for item in contacts:
            if ingested >= LEADS_PER_RUN:
                break
            if domain_count >= MAX_PER_DOMAIN:
                log.debug(f"  Domain cap ({MAX_PER_DOMAIN}) reached for {domain}")
                break

            c     = item["contact"]
            title = c.get("position", "")

            # Only import contacts whose title matches our ICP
            if not title_matches_icp(title):
                log.debug(f"  Skip (non-ICP title): {title!r}")
                skipped += 1
                continue

            lead = hunter_to_lead(item)

            if not lead["name"] or not lead["email"]:
                skipped += 1
                continue

            # Skip low-confidence emails (< 50%)
            if c.get("confidence", 100) < 50:
                log.debug(f"  Skip (low confidence {c.get('confidence')}%): {lead['email']}")
                skipped += 1
                continue

            # Deduplicate
            email_key = lead["email"].lower().strip()
            if email_key in existing_emails:
                log.debug(f"  Skip (dup email): {email_key}")
                skipped += 1
                continue

            combo_key = (lead["name"].lower().strip(), lead["company"].lower().strip())
            if combo_key in existing_combos:
                log.debug(f"  Skip (dup name+company): {combo_key}")
                skipped += 1
                continue

            result = post_lead(lead)
            if result and result[0]:
                lid, score, tier = result
                # Enforce minimum ICP score — skip low-quality leads
                if score < MIN_ICP_SCORE:
                    log.debug(f"  Skip (score {score} < min {MIN_ICP_SCORE}): {lead['name']}")
                    # Remove from DB since we already posted — delete it back
                    try:
                        requests.delete(f"{CRM_API_URL}/api/leads/{lid}", timeout=10)
                    except Exception:
                        pass
                    skipped += 1
                    continue
                log.info(
                    f"  [{ingested + 1}/{LEADS_PER_RUN}] {lead['name']} @ {lead['company']} "
                    f"| {tier} ({score}/100) | {lead['email']} | id={lid}"
                )
                ingested += 1
                domain_count += 1
                existing_emails.add(email_key)
                existing_combos.add(combo_key)
                time.sleep(0.1)

        time.sleep(1.5)   # be kind to Hunter.io rate limits between domains

    log.info(
        f"=== Hunter.io Ingestion Complete | "
        f"ingested={ingested} | skipped={skipped} ==="
    )
    if ingested == 0 and skipped == 0:
        log.warning(
            "Zero leads processed. Check HUNTER_API_KEY and ensure the "
            "target domains return contacts with matching ICP titles."
        )


if __name__ == "__main__":
    main()
