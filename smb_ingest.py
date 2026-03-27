#!/usr/bin/env python3
"""
smb_ingest.py — RAIS Advisory: Small & Mid-Market Business lead ingestion

Targets SMB decision-makers (CEO, Owner, Founder, COO, President) at small
and mid-sized companies across Financial Services, Healthcare, Manufacturing,
and Retail who likely don't yet have formal AI/data governance in place.

Uses Hunter.io Domain Search to find contacts, filters by SMB decision-maker
titles, auto-scores, and POSTs to the RAIS CRM.

Run manually : python3 smb_ingest.py
GitHub Action: runs every Thursday 9 AM UTC (separate cadence from enterprise)

Required env vars:
  HUNTER_API_KEY  — Hunter.io API key
  CRM_API_URL     — (optional) defaults to https://rais-crm-api.onrender.com
  LEADS_PER_RUN   — (optional) max new leads per run, default 20
  DOMAIN_BATCH    — (optional) domains per run, default 12
"""

import os, sys, time, logging
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("smb_ingest")

# ── Config ────────────────────────────────────────────────────────────────────

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
CRM_API_URL    = os.environ.get("CRM_API_URL", "https://rais-crm-api.onrender.com").rstrip("/")
LEADS_PER_RUN  = int(os.environ.get("LEADS_PER_RUN", "20"))
DOMAIN_BATCH   = int(os.environ.get("DOMAIN_BATCH", "12"))

if not HUNTER_API_KEY:
    log.error("HUNTER_API_KEY is not set. Exiting.")
    sys.exit(1)

# ── SMB ICP Title Keywords ────────────────────────────────────────────────────
# Small businesses = CEO/Owner IS the buyer. No need to find a CDO — there isn't one.
# These are people who make ALL technology and strategy decisions.

SMB_TITLE_KEYWORDS = [
    # Primary decision-makers
    "owner", "founder", "co-founder", "cofounder",
    "chief executive", "ceo", "president", "principal",
    "managing director", "managing partner", "managing member",
    "proprietor", "partner",
    # Operational decision-makers (often handle tech at SMBs)
    "chief operating officer", "coo",
    "chief technology officer", "cto",
    "chief financial officer", "cfo",
    "chief information officer", "cio",
    "director of operations", "director of technology",
    "director of finance", "director of it",
    "vp of operations", "vp operations",
    "general manager", "operations manager",
    "it manager", "technology manager",
]

def title_matches_smb(title: str) -> bool:
    t = (title or "").lower()
    return any(kw in t for kw in SMB_TITLE_KEYWORDS)

# ── Curated SMB / Mid-Market Domain List ─────────────────────────────────────
# Small-to-mid market companies across RAIS ICP sectors.
# These are companies with 10–500 employees that likely lack formal
# AI/data governance — exactly the gap RAIS Advisory fills.

SMB_DOMAINS = [
    # ── Regional / Community Banks & Credit Unions ────────────────────────────
    "bankatfirst.com", "firstbankpr.com", "glacierbancorp.com",
    "heartlandfinancialusa.com", "independentbank.com", "lakeshorebank.com",
    "mainstreetbank.com", "midlandstatesbank.com", "nbsc.com",
    "oldnationalbank.com", "pinnaclebank.com", "prosperitybanktx.com",
    "renewalfinancial.com", "seacoastbanking.com", "southstatebank.com",
    "townebank.com", "tristatecapitalbank.com", "unitedcommunitybank.com",
    "veritasfinancialgroup.com", "westernalliancebancorporation.com",
    "firstfinancial.com", "centerstatebk.com", "enterprisebanking.com",
    "bankofnewglarus.com", "firstmerchants.com", "midpeninsulabk.com",
    "capitalcitybank.com", "citizenscommunitybancorp.com",

    # ── Independent Financial Advisors & Insurance ────────────────────────────
    "ameritaslife.com", "arrowheadresearch.com", "balentine.com",
    "bairdwealth.com", "brownandwilliamson.com", "captrust.com",
    "crestwoodadvisors.com", "delanceywealth.com", "edwardjones.com",
    "firstwesterntrust.com", "forvismazars.com", "gradientadvisors.com",
    "integritymarketing.com", "janney.com", "keenanfin.com",
    "lpl.com", "magnififinancial.com", "marinerwealth.com",
    "merceradvisors.com", "nfp.com", "periscopegroup.com",
    "plancorp.com", "quartermaine.com", "royalridgeadvisors.com",
    "savantwealth.com", "stewardpartners.com", "tcfinancialgroup.com",
    "towerpointwealth.com", "univestfinancial.com", "usadvisors.com",

    # ── Regional Healthcare Practices & Groups ────────────────────────────────
    "alliedphysicians.com", "atlanticmedical.com", "baycare.org",
    "caremount.com", "carolinasmedicalcenter.com", "clintonhospital.org",
    "communityhospital.com", "crystalrunhealthcare.com", "deancare.com",
    "eastpennmedical.com", "firstchoicehealth.com", "glencoeregional.com",
    "hendricks.org", "hillsidehealthcenter.com", "impclinics.com",
    "jerseyshoreuniversitymedicalcenter.org", "karmanos.org", "lakewoodhealth.com",
    "midlandshealthcare.com", "northsidehospital.com", "ohiohealth.com",
    "phoenixchildrens.org", "pottersmedical.com", "quantumhealth.com",
    "rockfordmemorialhospital.com", "scripps.org", "southwestgeneral.net",
    "trinityhealth.org", "umassmed.edu", "valleyhealth.com",

    # ── Small / Mid-Market Manufacturers ─────────────────────────────────────
    "acuitybrands.com", "belden.com", "circo-craft.com",
    "clearfieldco.com", "donaldson.com", "easternsealings.com",
    "fabrinet.com", "generalcable.com", "harsco.com",
    "insteel.com", "jadow.com", "kaydon.com",
    "lydall.com", "midwayindustries.com", "novatel.com",
    "odysseymfg.com", "patricksolutions.com", "quadrant.com",
    "roperind.com", "salcosystems.com", "thecrosby.com",
    "ultrafabrics.com", "vermeerequipment.com", "westernmfg.com",
    "xtechinc.com", "ynvisible.com", "zurn.com",
    "alphametals.com", "bauerfeind.com", "cirtransformers.com",
    "deltacooling.com", "edgewater.com", "fabricatedmetals.com",

    # ── Regional Retail / CPG Brands ──────────────────────────────────────────
    "aheadinc.com", "bellring.com", "calavo.com",
    "clearfield.com", "daves-killer-bread.com", "energizer.com",
    "farmersupply.com", "freshpetfood.com", "gronkslimejuice.com",
    "harvestnatural.com", "ihsfoods.com", "jakes-mints.com",
    "kalahari.com", "lagunita.com", "midwestfoods.com",
    "naturalizeddeli.com", "offthecob.com", "patinagroup.com",
    "qualityfoods.com", "rockymountainchocolate.com", "simplymandarin.com",
    "terrissimo.com", "unionbeverage.com", "villabrands.com",
    "wilsonnutrients.com", "xochitl.com", "youorganic.com",
    "zevia.com", "bolthousebrands.com", "croftersorganic.com",

    # ── Professional Services (Accounting, Law, Consulting) ───────────────────
    "abbottdavis.com", "bkd.com", "cbiz.com",
    "cliftonlarsonallen.com", "cohencpa.com", "dixonhughes.com",
    "eisneramper.com", "forvismazars.com", "grassigroup.com",
    "heidrick.com", "insightcfs.com", "joneswalker.com",
    "kleinfelder.com", "larsonallen.com", "marcumllp.com",
    "neimanmarcusgroup.com", "opscheck.com", "pensylvaniaadvisors.com",
    "quintiles.com", "rsmus.com", "sikich.com",
    "tbmconsulting.com", "uhy-us.com", "vanderhouwen.com",
    "webbercpa.com", "xinoconsulting.com", "yaskawa.com",

    # ── Staffing & HR Firms (often data-intensive, under-governed) ────────────
    "acsontarget.com", "bartonassociates.com", "cdistaff.com",
    "drivenstaffing.com", "execusearch.com", "fiverings.com",
    "globalhirex.com", "highmark.com", "infinityhcm.com",
    "joulehr.com", "kforce.com", "linkedstaffing.com",
    "manpowergroup.com", "northlandgroup.com", "opusrecruiting.com",
    "primestaffing.com", "quickstaffing.com", "recruitics.com",
    "staffmark.com", "talentbridge.com", "unique-hr.com",
]

# ── Domain rotation (offset from enterprise rotation to avoid credit overlap) ─

def get_this_weeks_domains() -> list[str]:
    import datetime
    week_num = datetime.date.today().isocalendar()[1]
    # Offset by 50 to not clash with hunter_ingest.py rotation
    start = ((week_num + 5) * DOMAIN_BATCH) % len(SMB_DOMAINS)
    batch = (SMB_DOMAINS + SMB_DOMAINS)[start: start + DOMAIN_BATCH]
    log.info(f"Week {week_num}: SMB batch of {len(batch)} domains starting at index {start}")
    return batch

# ── Hunter.io Domain Search ───────────────────────────────────────────────────

def search_domain(domain: str) -> list[dict]:
    try:
        resp = requests.get(
            "https://api.hunter.io/v2/domain-search",
            params={"domain": domain, "api_key": HUNTER_API_KEY, "limit": 10},
            timeout=20,
        )
    except requests.RequestException as exc:
        log.warning(f"  Hunter request failed for {domain}: {exc}")
        return []

    if resp.status_code == 429:
        log.warning("  Rate limited — pausing 60s")
        time.sleep(60)
        return []
    if resp.status_code != 200:
        log.warning(f"  Hunter {resp.status_code} for {domain}: {resp.text[:100]}")
        return []

    data   = resp.json().get("data", {})
    emails = data.get("emails", [])
    org    = data.get("organization") or domain.split(".")[0].title()
    log.info(f"  {domain}: {len(emails)} contacts")
    return [{"contact": e, "org": org, "domain": domain} for e in emails]

# ── Map Hunter contact → CRM lead ─────────────────────────────────────────────

def hunter_to_lead(item: dict) -> dict:
    c    = item["contact"]
    name = f"{c.get('first_name','')} {c.get('last_name','')}".strip()
    if not name:
        name = c.get("value", "").split("@")[0].replace(".", " ").title()
    return {
        "name":         name,
        "title":        c.get("position", ""),
        "company":      item["org"],
        "email":        c.get("value", ""),
        "linkedin_url": c.get("linkedin", "") or "",
        "phone":        "",
        "source":       "hunter-smb",
        "industry":     "",
        "company_size": "<100",   # assume SMB until proven otherwise
        "ai_maturity":  "exploring",  # SMBs rarely have formal AI programs
        "notes":        (
            f"SMB auto-ingest via Hunter.io ({item['domain']}). "
            f"Email confidence: {c.get('confidence','?')}%. "
            "Likely unaware of AI governance needs — high education opportunity."
        ),
        "status":       "prospect",
    }

# ── Fetch existing leads for dedup ────────────────────────────────────────────

def get_existing_leads() -> tuple[set, set]:
    try:
        resp = requests.get(f"{CRM_API_URL}/api/leads", timeout=60)
        resp.raise_for_status()
        leads  = resp.json()
        emails = {l.get("email","").lower().strip() for l in leads if l.get("email")}
        combos = {(l.get("name","").lower().strip(), l.get("company","").lower().strip()) for l in leads}
        log.info(f"Loaded {len(leads)} existing CRM leads for dedup")
        return emails, combos
    except Exception as exc:
        log.warning(f"Could not fetch existing leads ({exc}). Skipping dedup.")
        return set(), set()

# ── Post lead to CRM ──────────────────────────────────────────────────────────

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
        log.warning(f"  CRM rejected '{lead.get('name')}': {resp.status_code}")
        return None
    except Exception as exc:
        log.error(f"  Error posting '{lead.get('name')}': {exc}")
        return None

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info(f"=== SMB Ingestion Start | target={LEADS_PER_RUN} leads ===")

    existing_emails, existing_combos = get_existing_leads()
    domains  = get_this_weeks_domains()
    ingested = 0
    skipped  = 0

    for domain in domains:
        if ingested >= LEADS_PER_RUN:
            break

        log.info(f"Searching: {domain}")
        contacts = search_domain(domain)

        for item in contacts:
            if ingested >= LEADS_PER_RUN:
                break

            c     = item["contact"]
            title = c.get("position", "")

            if not title_matches_smb(title):
                log.debug(f"  Skip (non-SMB title): {title!r}")
                skipped += 1
                continue

            lead = hunter_to_lead(item)

            if not lead["name"] or not lead["email"]:
                skipped += 1
                continue

            if c.get("confidence", 100) < 40:
                skipped += 1
                continue

            email_key = lead["email"].lower().strip()
            if email_key in existing_emails:
                skipped += 1
                continue

            combo_key = (lead["name"].lower().strip(), lead["company"].lower().strip())
            if combo_key in existing_combos:
                skipped += 1
                continue

            result = post_lead(lead)
            if result and result[0]:
                lid, score, tier = result
                log.info(
                    f"  [{ingested+1}/{LEADS_PER_RUN}] {lead['name']} @ {lead['company']} "
                    f"| {lead['title']} | {tier} ({score}/100) | id={lid}"
                )
                ingested += 1
                existing_emails.add(email_key)
                existing_combos.add(combo_key)
                time.sleep(0.1)

        time.sleep(1.5)

    log.info(f"=== SMB Ingestion Complete | ingested={ingested} | skipped={skipped} ===")


if __name__ == "__main__":
    main()
