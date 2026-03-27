#!/usr/bin/env python3
"""
apollo_ingest.py — RAIS Advisory weekly lead ingestion from Apollo.io

Queries Apollo People Search API for contacts matching our ICP, deduplicates
against existing CRM leads, auto-scores each lead, and POSTs new ones to the
RAIS CRM API.

Run manually : python3 apollo_ingest.py
Render Cron  : python3 apollo_ingest.py   (weekly schedule in Render dashboard)

Required env vars:
  APOLLO_API_KEY  — Apollo.io API key (from Settings → Integrations → API)
  CRM_API_URL     — (optional) defaults to https://rais-crm-api.onrender.com
  LEADS_PER_RUN   — (optional) max new leads per execution, default 50
"""

import os, sys, time, logging
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("apollo_ingest")

# ── Config ────────────────────────────────────────────────────────────────────

APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
CRM_API_URL    = os.environ.get("CRM_API_URL", "https://rais-crm-api.onrender.com").rstrip("/")
LEADS_PER_RUN  = int(os.environ.get("LEADS_PER_RUN", "50"))

if not APOLLO_API_KEY:
    log.error("APOLLO_API_KEY environment variable is not set. Exiting.")
    sys.exit(1)

# ── ICP Targeting (mirrors scoring weights in api.py) ────────────────────────

# Job titles — ordered by ICP priority (highest-scoring first)
TARGET_TITLES = [
    # 35-pt tier: direct AI/data governance buyers
    "Chief Risk Officer", "CRO", "Chief Data Officer", "CDO",
    "Chief AI Officer", "CAIO", "Head of AI Governance", "Head of AI",
    "Director of AI Governance",
    # 30-pt tier: adjacent C-suite
    "Chief Digital Officer", "Chief Compliance Officer", "CCO",
    "Chief Analytics Officer",
    # 28-pt tier: senior VP/Director
    "VP Data", "VP Analytics", "VP AI",
    "Vice President Data", "Vice President Analytics", "Vice President AI",
    "Director of AI", "Director of Data Science",
    # 22-pt tier: data/analytics directors
    "Director of Data", "Director of Analytics",
    "VP Strategy", "VP Digital",
    # 20-pt tier: legal/compliance
    "General Counsel", "Deputy General Counsel", "VP Legal", "Head of Legal",
    # 18-pt tier: security/risk
    "CISO", "Chief Information Security Officer", "VP Risk", "Head of Risk",
]

TARGET_SENIORITIES = ["c_suite", "vp", "director", "head"]

# Industries that score highest in our ICP model
TARGET_INDUSTRIES = [
    "Consumer Packaged Goods",
    "Retail",
    "Financial Services",
    "Banking",
    "Insurance",
    "Asset Management",
    "Investment Management",
    "Healthcare",
    "Pharmaceuticals",
    "Life Sciences",
    "Manufacturing",
    "Logistics and Supply Chain",
]

# ── Apollo People Search ──────────────────────────────────────────────────────

def search_apollo(page: int = 1, per_page: int = 50) -> list:
    """
    Query Apollo.io /v1/mixed_people/search and return a list of person dicts.
    Returns [] on any error.
    """
    url = "https://api.apollo.io/v1/mixed_people/search"
    payload = {
        "per_page":                    per_page,
        "page":                        page,
        "person_titles":               TARGET_TITLES,
        "person_seniorities":          TARGET_SENIORITIES,
        "organization_industry_tags":  TARGET_INDUSTRIES,
        # Only people not yet prospected by our team
        "prospected_by_current_team":  ["no"],
        # Prefer verified emails
        "contact_email_status":        ["verified", "likely_to_engage"],
    }
    try:
        resp = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "X-Api-Key": APOLLO_API_KEY,   # key must be in header per Apollo docs
            },
            json=payload,
            timeout=30,
        )
    except requests.RequestException as exc:
        log.error(f"Apollo request failed: {exc}")
        return []

    if resp.status_code != 200:
        log.error(f"Apollo API returned {resp.status_code}: {resp.text[:400]}")
        return []

    data = resp.json()
    people = data.get("people") or data.get("contacts") or []
    log.info(f"Apollo page {page}: {len(people)} contacts received")
    return people


# ── Fetch existing CRM leads for deduplication ────────────────────────────────

def get_existing_leads() -> tuple[set, set]:
    """
    Returns (email_set, name_company_set) loaded from the CRM so we can skip
    leads that are already in the database.
    """
    try:
        resp = requests.get(f"{CRM_API_URL}/api/leads", timeout=60)  # 60s for Render cold starts
        resp.raise_for_status()
        leads = resp.json()
        emails  = {l.get("email", "").lower().strip() for l in leads if l.get("email")}
        combos  = {
            (l.get("name", "").lower().strip(), l.get("company", "").lower().strip())
            for l in leads
        }
        log.info(f"Loaded {len(leads)} existing CRM leads for deduplication")
        return emails, combos
    except Exception as exc:
        log.warning(f"Could not fetch existing leads ({exc}). Skipping dedup.")
        return set(), set()


# ── Apollo → CRM mapping ──────────────────────────────────────────────────────

def _map_size(employee_count) -> str:
    """Map a raw employee count to the CRM size bucket."""
    try:
        n = int(employee_count)
    except (TypeError, ValueError):
        return ""
    if n < 100:    return "<100"
    if n < 500:    return "100-500"
    if n < 1000:   return "500-1000"
    if n < 10000:  return "1000-10000"
    return "10000+"


def apollo_to_lead(person: dict) -> dict:
    """Convert an Apollo People API person dict to a CRM lead payload."""
    org  = person.get("organization") or {}
    name = f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()

    # Best available email
    email = person.get("email", "")
    if not email:
        for e in (person.get("email_addresses") or []):
            if isinstance(e, dict) and e.get("email"):
                email = e["email"]
                break

    # Employee count comes from org or top-level
    emp_count = (
        org.get("num_employees")
        or org.get("estimated_num_employees")
        or person.get("organization_num_employees")
    )

    industry = (
        org.get("industry")
        or org.get("short_description")
        or person.get("organization_industry")
        or ""
    ).title()

    location_parts = [
        person.get("city", ""),
        person.get("state", ""),
        person.get("country", ""),
    ]
    location = ", ".join(p for p in location_parts if p).strip(", ")

    return {
        "name":         name,
        "title":        person.get("title", ""),
        "company":      org.get("name", "") or person.get("organization_name", ""),
        "email":        email,
        "linkedin_url": person.get("linkedin_url", ""),
        "phone":        person.get("phone", "") or person.get("sanitized_phone", "") or "",
        "source":       "apollo",
        "industry":     industry,
        "company_size": _map_size(emp_count),
        "ai_maturity":  "",        # unknown at ingest time; update manually after call
        "notes":        f"Auto-ingested via Apollo.io weekly feed.{' Location: ' + location if location else ''}",
        "status":       "prospect",
    }


# ── Post a lead to the CRM ────────────────────────────────────────────────────

def post_lead(lead: dict):
    """
    POST a lead payload to the CRM API.
    Returns (id, icp_score, icp_tier) on success, or None on failure.
    """
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
        log.warning(
            f"CRM rejected '{lead.get('name')}': "
            f"HTTP {resp.status_code} — {resp.text[:120]}"
        )
        return None
    except Exception as exc:
        log.error(f"Error posting '{lead.get('name')}': {exc}")
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info(f"=== Apollo Ingestion Start | target={LEADS_PER_RUN} leads ===")

    existing_emails, existing_combos = get_existing_leads()

    ingested = 0
    skipped  = 0
    page     = 1

    while ingested < LEADS_PER_RUN:
        batch = search_apollo(page=page, per_page=50)
        if not batch:
            log.info("No more contacts returned from Apollo.")
            break

        for person in batch:
            if ingested >= LEADS_PER_RUN:
                break

            lead = apollo_to_lead(person)

            if not lead["name"]:
                skipped += 1
                continue

            # ── Deduplication ──────────────────────────────────────────────
            email_key = lead["email"].lower().strip()
            if email_key and email_key in existing_emails:
                log.debug(f"  Skip (dup email): {email_key}")
                skipped += 1
                continue

            combo_key = (lead["name"].lower().strip(), lead["company"].lower().strip())
            if combo_key in existing_combos:
                log.debug(f"  Skip (dup name+company): {combo_key}")
                skipped += 1
                continue

            # ── Ingest ────────────────────────────────────────────────────
            result = post_lead(lead)
            if result and result[0]:
                lid, score, tier = result
                log.info(
                    f"  [{ingested + 1}/{LEADS_PER_RUN}] "
                    f"{lead['name']} @ {lead['company']} | "
                    f"{tier} ({score}/100) | id={lid}"
                )
                ingested += 1
                # Track in-run additions so we don't double-post
                if email_key:
                    existing_emails.add(email_key)
                existing_combos.add(combo_key)
                time.sleep(0.1)   # gentle pacing

        page += 1
        if page > 10:             # safety ceiling: 10 pages × 50 = 500 candidates max
            log.info("Reached page limit (10). Stopping.")
            break
        time.sleep(1.0)           # rate-limit courtesy to Apollo

    log.info(
        f"=== Apollo Ingestion Complete | "
        f"ingested={ingested} | skipped_dups={skipped} ==="
    )
    if ingested == 0 and skipped == 0:
        log.warning(
            "Zero leads processed. Verify APOLLO_API_KEY is valid and "
            "the account has People Search credits."
        )


if __name__ == "__main__":
    main()
