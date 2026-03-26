#!/usr/bin/env python3
"""
RAIS Advisory — CRM API (Render + Supabase PostgreSQL)
Start: gunicorn api:app   |   Local: python3 api.py
Requires: DATABASE_URL env var (PostgreSQL connection string)
"""
import os, sys
from datetime import datetime
from contextlib import contextmanager
from flask import Flask, jsonify, request

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL environment variable.", file=sys.stderr)
    sys.exit(1)

try:
    import psycopg2, psycopg2.extras
except ImportError:
    print("Run: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

app = Flask(__name__)

# ── DB helpers ────────────────────────────────────────────────────────────────

@contextmanager
def db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def fetchall(conn, sql, params=()):
    cur = conn.cursor(); cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]

def fetchone(conn, sql, params=()):
    cur = conn.cursor(); cur.execute(sql, params)
    r = cur.fetchone(); return dict(r) if r else None

def scalar(conn, sql, params=()):
    cur = conn.cursor(); cur.execute(sql, params)
    r = cur.fetchone(); return list(r.values())[0] if r else None

def run(conn, sql, params=()):
    cur = conn.cursor(); cur.execute(sql, params); return cur

def insert(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql + " RETURNING id", params)
    r = cur.fetchone(); return r["id"] if r else None

# ── Init ──────────────────────────────────────────────────────────────────────

def init_db():
    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id           BIGSERIAL PRIMARY KEY,
            name         TEXT    NOT NULL,
            title        TEXT,   company      TEXT,
            email        TEXT,   linkedin_url TEXT,
            phone        TEXT,   source       TEXT,
            industry     TEXT,   company_size TEXT,
            ai_maturity  TEXT,
            icp_score    INTEGER DEFAULT 0,
            icp_tier     TEXT    DEFAULT 'nurture',
            notes        TEXT,
            status       TEXT    DEFAULT 'prospect',
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            updated_at   TIMESTAMPTZ DEFAULT NOW()
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline (
            id               BIGSERIAL PRIMARY KEY,
            lead_id          BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
            stage            TEXT    DEFAULT 'prospect',
            service_type     TEXT,
            deal_value       NUMERIC DEFAULT 0,
            probability      INTEGER DEFAULT 20,
            next_action      TEXT,
            next_action_date TEXT,
            notes            TEXT,
            created_at       TIMESTAMPTZ DEFAULT NOW(),
            updated_at       TIMESTAMPTZ DEFAULT NOW()
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS outreach (
            id             BIGSERIAL PRIMARY KEY,
            lead_id        BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
            sequence_step  INTEGER DEFAULT 1,
            template_used  TEXT,
            channel        TEXT DEFAULT 'email',
            sent_at        TIMESTAMPTZ,
            status         TEXT DEFAULT 'pending',
            follow_up_date TEXT,
            response_notes TEXT,
            created_at     TIMESTAMPTZ DEFAULT NOW()
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS activities (
            id         BIGSERIAL PRIMARY KEY,
            lead_id    BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
            type       TEXT,
            content    TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""")
    print("Database initialized.")

# ── ICP Scoring ───────────────────────────────────────────────────────────────

TITLE_SCORES = [
    (35, ["chief risk officer","cro","chief data officer","cdo","chief ai officer",
          "caio","head of ai governance","head of ai","director of ai governance"]),
    (30, ["chief digital officer","chief compliance officer","cco","chief analytics officer"]),
    (28, ["vp data","vp analytics","vp ai","vice president data","vice president analytics",
          "vice president ai","director of ai","director of data science"]),
    (22, ["director of data","director analytics","vp strategy","vp digital"]),
    (20, ["general counsel","deputy general counsel","vp legal","head of legal"]),
    (18, ["ciso","chief information security","vp risk","head of risk"]),
    (14, ["director","vp","vice president"]),
    (8,  ["manager","head of","lead"]),
]
INDUSTRY_SCORES = [
    (30, ["cpg","consumer packaged goods","consumer goods","fmcg","retail","e-commerce","ecommerce"]),
    (25, ["financial services","banking","insurance","asset management","wealth management",
          "investment","private equity","hedge fund","fintech"]),
    (22, ["healthcare","pharma","pharmaceutical","life sciences","biotech"]),
    (20, ["manufacturing","industrial","supply chain","logistics"]),
    (15, ["technology","software","saas","media","telecom"]),
    (10, ["government","nonprofit","education","real estate"]),
]
SIZE_SCORES = {"1000-10000":20,"10000+":18,"500-1000":15,"100-500":10,"<100":5}
MATURITY_SCORES = {"deployed-no-governance":15,"building":14,"exploring":12,"mature-with-gaps":10,"mature":5}

def score_icp(title, industry, size, maturity):
    t = (title or "").lower(); i = (industry or "").lower(); pts = 0
    for score, kws in TITLE_SCORES:
        if any(k in t for k in kws): pts += score; break
    for score, kws in INDUSTRY_SCORES:
        if any(k in i for k in kws): pts += score; break
    pts += SIZE_SCORES.get(size or "", 0)
    pts += MATURITY_SCORES.get(maturity or "", 0)
    total = min(pts, 100)
    tier = "tier1" if total>=80 else "tier2" if total>=60 else "tier3" if total>=40 else "nurture"
    return total, tier

# ── CORS ──────────────────────────────────────────────────────────────────────

@app.after_request
def cors(r):
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type"
    r.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    return r

@app.route("/api/<path:p>", methods=["OPTIONS"])
def preflight(p): return "", 200

@app.route("/health")
def health(): return jsonify({"status":"ok"})

# ── Leads ─────────────────────────────────────────────────────────────────────

@app.route("/api/leads", methods=["GET"])
def list_leads():
    with db() as conn:
        data = fetchall(conn, """
            SELECT l.*, p.id AS pid, p.stage, p.service_type, p.deal_value,
                   p.probability, p.next_action, p.next_action_date
            FROM leads l LEFT JOIN pipeline p ON p.lead_id = l.id
            ORDER BY l.icp_score DESC, l.created_at DESC
        """)
    return jsonify(data)

@app.route("/api/leads", methods=["POST"])
def add_lead():
    d = request.json or {}
    score, tier = score_icp(d.get("title"), d.get("industry"), d.get("company_size"), d.get("ai_maturity"))
    with db() as conn:
        lid = insert(conn, """
            INSERT INTO leads (name,title,company,email,linkedin_url,phone,source,
              industry,company_size,ai_maturity,icp_score,icp_tier,notes,status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (d.get("name",""),d.get("title",""),d.get("company",""),d.get("email",""),
              d.get("linkedin_url",""),d.get("phone",""),d.get("source",""),d.get("industry",""),
              d.get("company_size",""),d.get("ai_maturity",""),score,tier,
              d.get("notes",""),d.get("status","prospect")))
        run(conn, "INSERT INTO pipeline (lead_id,stage,service_type,deal_value,probability) VALUES (%s,%s,%s,%s,%s)",
            (lid,"prospect",d.get("service_type",""),d.get("deal_value") or 0,20))
        run(conn, "INSERT INTO activities (lead_id,type,content) VALUES (%s,%s,%s)",
            (lid,"created",f"Lead added · Source: {d.get('source','—')}"))
    return jsonify({"id":lid,"icp_score":score,"icp_tier":tier}), 201

@app.route("/api/leads/<int:lid>", methods=["GET"])
def get_lead(lid):
    with db() as conn:
        l = fetchone(conn, "SELECT * FROM leads WHERE id=%s", (lid,))
        if not l: return jsonify({"error":"not found"}), 404
        p  = fetchone(conn, "SELECT * FROM pipeline WHERE lead_id=%s", (lid,))
        o  = fetchall(conn, "SELECT * FROM outreach WHERE lead_id=%s ORDER BY sequence_step", (lid,))
        ac = fetchall(conn, "SELECT * FROM activities WHERE lead_id=%s ORDER BY created_at DESC LIMIT 20", (lid,))
    return jsonify({"lead":l,"pipeline":p or {},"outreach":o,"activities":ac})

@app.route("/api/leads/<int:lid>", methods=["PUT"])
def update_lead(lid):
    d = request.json or {}
    score, tier = score_icp(d.get("title"), d.get("industry"), d.get("company_size"), d.get("ai_maturity"))
    with db() as conn:
        run(conn, """
            UPDATE leads SET name=%s,title=%s,company=%s,email=%s,linkedin_url=%s,
            phone=%s,source=%s,industry=%s,company_size=%s,ai_maturity=%s,
            icp_score=%s,icp_tier=%s,notes=%s,status=%s,updated_at=NOW() WHERE id=%s
        """, (d.get("name",""),d.get("title",""),d.get("company",""),d.get("email",""),
              d.get("linkedin_url",""),d.get("phone",""),d.get("source",""),d.get("industry",""),
              d.get("company_size",""),d.get("ai_maturity",""),score,tier,
              d.get("notes",""),d.get("status","prospect"),lid))
    return jsonify({"ok":True,"icp_score":score,"icp_tier":tier})

@app.route("/api/leads/<int:lid>", methods=["DELETE"])
def del_lead(lid):
    with db() as conn: run(conn, "DELETE FROM leads WHERE id=%s", (lid,))
    return jsonify({"ok":True})

# ── Pipeline ──────────────────────────────────────────────────────────────────

@app.route("/api/pipeline", methods=["GET"])
def get_pipeline():
    with db() as conn:
        data = fetchall(conn, """
            SELECT p.*, l.name, l.title, l.company, l.icp_score, l.icp_tier, l.source, l.email
            FROM pipeline p JOIN leads l ON l.id = p.lead_id
            ORDER BY p.updated_at DESC
        """)
    return jsonify(data)

@app.route("/api/pipeline/<int:pid>", methods=["PUT"])
def update_pipeline(pid):
    d = request.json or {}
    with db() as conn:
        old = fetchone(conn, "SELECT stage, lead_id FROM pipeline WHERE id=%s", (pid,))
        run(conn, """
            UPDATE pipeline SET stage=%s,service_type=%s,deal_value=%s,probability=%s,
            next_action=%s,next_action_date=%s,notes=%s,updated_at=NOW() WHERE id=%s
        """, (d.get("stage","prospect"),d.get("service_type",""),d.get("deal_value",0),
              d.get("probability",20),d.get("next_action",""),d.get("next_action_date",""),
              d.get("notes",""),pid))
        if old and old.get("stage") != d.get("stage"):
            run(conn, "INSERT INTO activities (lead_id,type,content) VALUES (%s,%s,%s)",
                (old["lead_id"],"stage-change",
                 f"Pipeline: {old['stage']} → {d.get('stage','?')}"))
    return jsonify({"ok":True})

# ── Outreach ──────────────────────────────────────────────────────────────────

@app.route("/api/outreach", methods=["GET"])
def get_outreach():
    with db() as conn:
        data = fetchall(conn, """
            SELECT o.*, l.name, l.company, l.email, l.title, l.icp_tier
            FROM outreach o JOIN leads l ON l.id = o.lead_id
            ORDER BY CASE o.status
                WHEN 'pending' THEN 1 WHEN 'sent' THEN 2
                WHEN 'opened'  THEN 3 WHEN 'replied' THEN 4 ELSE 5 END,
              o.follow_up_date ASC NULLS LAST
        """)
    return jsonify(data)

@app.route("/api/outreach", methods=["POST"])
def add_outreach():
    d = request.json or {}
    with db() as conn:
        oid = insert(conn, """
            INSERT INTO outreach
              (lead_id,sequence_step,template_used,channel,sent_at,status,follow_up_date,response_notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (d.get("lead_id"),d.get("sequence_step",1),d.get("template_used",""),
              d.get("channel","email"),datetime.now(),d.get("status","sent"),
              d.get("follow_up_date") or None,d.get("response_notes","")))
        run(conn, "INSERT INTO activities (lead_id,type,content) VALUES (%s,%s,%s)",
            (d["lead_id"],"outreach",
             f"Step {d.get('sequence_step',1)} via {d.get('channel','email')} · {d.get('template_used','')}"))
    return jsonify({"id":oid}), 201

@app.route("/api/outreach/<int:oid>", methods=["PUT"])
def update_outreach(oid):
    d = request.json or {}
    with db() as conn:
        run(conn, "UPDATE outreach SET status=%s,follow_up_date=%s,response_notes=%s WHERE id=%s",
            (d.get("status","sent"),d.get("follow_up_date") or None,d.get("response_notes",""),oid))
        if d.get("status") in ("replied","booked"):
            o = fetchone(conn, "SELECT lead_id FROM outreach WHERE id=%s", (oid,))
            if o:
                run(conn, "INSERT INTO activities (lead_id,type,content) VALUES (%s,%s,%s)",
                    (o["lead_id"],"response",f"Outreach status → {d['status']}"))
    return jsonify({"ok":True})

# ── Activities ────────────────────────────────────────────────────────────────

@app.route("/api/activities", methods=["POST"])
def add_activity():
    d = request.json or {}
    with db() as conn:
        run(conn, "INSERT INTO activities (lead_id,type,content) VALUES (%s,%s,%s)",
            (d["lead_id"],d.get("type","note"),d.get("content","")))
    return jsonify({"ok":True}), 201

@app.route("/api/activities/<int:lid>", methods=["GET"])
def get_activities(lid):
    with db() as conn:
        data = fetchall(conn,"SELECT * FROM activities WHERE lead_id=%s ORDER BY created_at DESC LIMIT 30",(lid,))
    return jsonify(data)

# ── Stats ─────────────────────────────────────────────────────────────────────

@app.route("/api/stats", methods=["GET"])
def stats():
    with db() as conn:
        total  = scalar(conn, "SELECT COUNT(*) FROM leads")
        pipe_v = scalar(conn, "SELECT COALESCE(SUM(deal_value),0) FROM pipeline WHERE stage != 'closed-lost'")
        active = scalar(conn, "SELECT COUNT(*) FROM pipeline WHERE stage='active'")
        props  = scalar(conn, "SELECT COUNT(*) FROM pipeline WHERE stage='proposal-sent'")
        won    = fetchone(conn, "SELECT COUNT(*) as cnt, COALESCE(SUM(deal_value),0) as val FROM pipeline WHERE stage='closed-won'")
        due    = scalar(conn, """
            SELECT COUNT(*) FROM outreach
            WHERE status IN ('pending','sent') AND follow_up_date IS NOT NULL
            AND follow_up_date::date <= CURRENT_DATE + INTERVAL '3 days'
        """)
        stages = fetchall(conn, "SELECT stage, COUNT(*) as cnt, COALESCE(SUM(deal_value),0) as val FROM pipeline GROUP BY stage ORDER BY cnt DESC")
        top    = fetchall(conn, """
            SELECT l.name,l.company,l.title,l.icp_score,l.icp_tier,p.stage,p.deal_value
            FROM leads l LEFT JOIN pipeline p ON p.lead_id=l.id
            ORDER BY l.icp_score DESC LIMIT 8
        """)
        recent = fetchall(conn, """
            SELECT a.*, l.name, l.company FROM activities a
            JOIN leads l ON l.id=a.lead_id ORDER BY a.created_at DESC LIMIT 10
        """)
        tiers  = fetchall(conn, "SELECT icp_tier, COUNT(*) as cnt FROM leads GROUP BY icp_tier")
    return jsonify({
        "total_leads":        total        or 0,
        "pipeline_value":     float(pipe_v or 0),
        "active_engagements": active       or 0,
        "proposals_out":      props        or 0,
        "closed_won_count":   (won or {}).get("cnt", 0),
        "closed_won_value":   float((won or {}).get("val", 0)),
        "outreach_due":       due          or 0,
        "by_stage":           stages,
        "top_leads":          top,
        "recent_activity":    recent,
        "by_tier":            tiers,
    })

# ── Start ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5001))
    print(f"RAIS CRM API running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
