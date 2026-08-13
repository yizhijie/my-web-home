import os
import csv
import io
import json
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field

DATABASE_URL = os.environ["DATABASE_URL"]
app = FastAPI(title="US Trend Radar")

MARKETS = {
    "US": {"name": "美国", "name_en": "United States", "group": "North America", "geo": "US"},
    "SG": {"name": "新加坡", "name_en": "Singapore", "group": "Southeast Asia", "geo": "SG"},
    "MY": {"name": "马来西亚", "name_en": "Malaysia", "group": "Southeast Asia", "geo": "MY"},
    "TH": {"name": "泰国", "name_en": "Thailand", "group": "Southeast Asia", "geo": "TH"},
    "VN": {"name": "越南", "name_en": "Vietnam", "group": "Southeast Asia", "geo": "VN"},
    "PH": {"name": "菲律宾", "name_en": "Philippines", "group": "Southeast Asia", "geo": "PH"},
}
MARKET_ORDER = ["US", "SG", "MY", "TH", "VN", "PH"]


def normalize_market(value: str | None) -> str:
    code = str(value or "US").strip().upper()
    return code if code in MARKETS else "US"


def enabled_markets() -> set[str]:
    configured = os.environ.get("COLLECTION_MARKETS", "US")
    markets = {normalize_market(x) for x in configured.split(",") if x.strip()}
    return markets or {"US"}


@contextmanager
def db():
    with psycopg.connect(DATABASE_URL) as conn:
        yield conn


def init_db():
    with db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS opportunities (
          id SERIAL PRIMARY KEY, product TEXT NOT NULL, category TEXT NOT NULL,
          platform TEXT NOT NULL, score INTEGER NOT NULL, momentum INTEGER NOT NULL,
          cross_platform INTEGER NOT NULL, demand INTEGER NOT NULL,
          gap TEXT NOT NULL, risk TEXT NOT NULL, source_url TEXT,
          comment_count INTEGER NOT NULL DEFAULT 0,
          pain_points TEXT NOT NULL DEFAULT '',
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""")
        conn.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS comment_count INTEGER NOT NULL DEFAULT 0")
        conn.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS pain_points TEXT NOT NULL DEFAULT ''")
        conn.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS comments_checked_at TIMESTAMPTZ")
        conn.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS market TEXT NOT NULL DEFAULT 'US'")
        conn.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ")
        conn.execute("""CREATE TABLE IF NOT EXISTS validation_checks (
          opportunity_id INTEGER PRIMARY KEY REFERENCES opportunities(id) ON DELETE CASCADE,
          target_price NUMERIC(12,2) NOT NULL DEFAULT 0,
          product_cost NUMERIC(12,2) NOT NULL DEFAULT 0,
          inbound_shipping NUMERIC(12,2) NOT NULL DEFAULT 0,
          platform_fee_pct NUMERIC(5,2) NOT NULL DEFAULT 0,
          fulfillment_fee NUMERIC(12,2) NOT NULL DEFAULT 0,
          ad_cost_pct NUMERIC(5,2) NOT NULL DEFAULT 0,
          return_rate_pct NUMERIC(5,2) NOT NULL DEFAULT 0,
          other_cost NUMERIC(12,2) NOT NULL DEFAULT 0,
          competitor_count INTEGER NOT NULL DEFAULT 0,
          competitor_avg_price NUMERIC(12,2) NOT NULL DEFAULT 0,
          compliance_risk TEXT NOT NULL DEFAULT 'unknown',
          seasonality TEXT NOT NULL DEFAULT '',
          notes TEXT NOT NULL DEFAULT '',
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS watchlists (
          id SERIAL PRIMARY KEY,
          user_key TEXT NOT NULL DEFAULT 'local',
          market TEXT NOT NULL DEFAULT 'US',
          opportunity_id INTEGER NOT NULL REFERENCES opportunities(id) ON DELETE CASCADE,
          note TEXT NOT NULL DEFAULT '',
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE(user_key, market, opportunity_id)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS comments (
          id SERIAL PRIMARY KEY,
          opportunity_id INTEGER NOT NULL REFERENCES opportunities(id) ON DELETE CASCADE,
          platform TEXT NOT NULL,
          comment_text TEXT NOT NULL,
          likes INTEGER NOT NULL DEFAULT 0,
          replies INTEGER NOT NULL DEFAULT 0,
          pain_label TEXT NOT NULL DEFAULT 'other',
          comment_url TEXT UNIQUE,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          published_at TIMESTAMPTZ
        )""")
        conn.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ")
        conn.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS sentiment TEXT NOT NULL DEFAULT 'unknown'")
        conn.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS sentiment_score NUMERIC(6,3)")
        conn.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS aspect TEXT NOT NULL DEFAULT 'other'")
        conn.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS classification_confidence NUMERIC(5,4)")
        conn.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS review_status TEXT NOT NULL DEFAULT 'machine_labeled'")
        conn.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS language TEXT NOT NULL DEFAULT 'en'")
        conn.execute("ALTER TABLE comments ADD COLUMN IF NOT EXISTS source_post_id TEXT NOT NULL DEFAULT ''")
        conn.execute("""CREATE TABLE IF NOT EXISTS pain_snapshots (
          id SERIAL PRIMARY KEY,
          snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
          market TEXT NOT NULL DEFAULT 'US',
          platform TEXT NOT NULL,
          pain_label TEXT NOT NULL,
          sentiment TEXT NOT NULL DEFAULT 'unknown',
          comment_count INTEGER NOT NULL DEFAULT 0,
          total_comments INTEGER NOT NULL DEFAULT 0,
          UNIQUE(snapshot_date, market, platform, pain_label, sentiment)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS trend_snapshots (
          id SERIAL PRIMARY KEY,
          snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
          market TEXT NOT NULL DEFAULT 'US',
          category TEXT NOT NULL,
          platform TEXT NOT NULL,
          opportunity_count INTEGER NOT NULL,
          avg_score INTEGER NOT NULL,
          avg_momentum INTEGER NOT NULL,
          avg_demand INTEGER NOT NULL,
          comment_count INTEGER NOT NULL DEFAULT 0,
          UNIQUE(snapshot_date, category, platform)
        )""")
        conn.execute("ALTER TABLE trend_snapshots ADD COLUMN IF NOT EXISTS market TEXT NOT NULL DEFAULT 'US'")
        conn.execute("ALTER TABLE trend_snapshots DROP CONSTRAINT IF EXISTS trend_snapshots_snapshot_date_category_platform_key")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS trend_snapshots_market_key ON trend_snapshots(snapshot_date, market, category, platform)")
        conn.execute("""CREATE TABLE IF NOT EXISTS collector_runs (
          id SERIAL PRIMARY KEY,
          keyword TEXT NOT NULL,
          status TEXT NOT NULL,
          new_items INTEGER NOT NULL DEFAULT 0,
          error_message TEXT NOT NULL DEFAULT '',
          started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          finished_at TIMESTAMPTZ
        )""")
        conn.execute("ALTER TABLE collector_runs ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'TikTok'")
        conn.execute("ALTER TABLE collector_runs ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'social'")
        conn.execute("ALTER TABLE collector_runs ADD COLUMN IF NOT EXISTS market TEXT NOT NULL DEFAULT 'US'")
        conn.execute("""CREATE TABLE IF NOT EXISTS source_status (
          source_key TEXT PRIMARY KEY,
          market TEXT NOT NULL DEFAULT 'US',
          display_name TEXT NOT NULL,
          kind TEXT NOT NULL,
          enabled BOOLEAN NOT NULL DEFAULT FALSE,
          configured BOOLEAN NOT NULL DEFAULT FALSE,
          status TEXT NOT NULL DEFAULT 'pending',
          rows_collected INTEGER NOT NULL DEFAULT 0,
          error_message TEXT NOT NULL DEFAULT '',
          last_run_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""")
        conn.execute("ALTER TABLE source_status ADD COLUMN IF NOT EXISTS market TEXT NOT NULL DEFAULT 'US'")
        conn.execute("ALTER TABLE source_status DROP CONSTRAINT IF EXISTS source_status_pkey")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS source_status_market_key ON source_status(source_key, market)")
        conn.execute("""CREATE TABLE IF NOT EXISTS google_trends (
          id SERIAL PRIMARY KEY,
          keyword TEXT NOT NULL,
          region TEXT NOT NULL DEFAULT 'US',
          trend_date DATE NOT NULL,
          interest INTEGER NOT NULL DEFAULT 0,
          related_query TEXT NOT NULL DEFAULT '',
          related_value INTEGER NOT NULL DEFAULT 0,
          source_url TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE(keyword, trend_date, related_query)
        )""")
        conn.execute("ALTER TABLE google_trends ADD COLUMN IF NOT EXISTS region TEXT NOT NULL DEFAULT 'US'")
        conn.execute("ALTER TABLE google_trends DROP CONSTRAINT IF EXISTS google_trends_keyword_trend_date_related_query_key")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS google_trends_region_key ON google_trends(keyword, region, trend_date, related_query)")
        # Remove only the three known seed rows. Live Bright Data rows have a source URL.
        conn.execute("""DELETE FROM opportunities
          WHERE source_url = '' AND product IN (
            'Portable pet cooling mat', 'Under-desk walking pad accessories',
            'Modular pantry organizer'
          )""")


@app.on_event("startup")
def startup():
    init_db()


class Opportunity(BaseModel):
    product: str = Field(min_length=2, max_length=160)
    category: str
    platform: str
    momentum: int = Field(ge=0, le=100)
    cross_platform: int = Field(ge=0, le=100)
    demand: int = Field(ge=0, le=100)
    gap: str
    risk: str
    source_url: str = ""


class ValidationInput(BaseModel):
    target_price: float = Field(ge=0, le=100000)
    product_cost: float = Field(ge=0, le=100000)
    inbound_shipping: float = Field(ge=0, le=100000)
    platform_fee_pct: float = Field(ge=0, le=100)
    fulfillment_fee: float = Field(ge=0, le=100000)
    ad_cost_pct: float = Field(ge=0, le=100)
    return_rate_pct: float = Field(ge=0, le=100)
    other_cost: float = Field(ge=0, le=100000)
    competitor_count: int = Field(ge=0, le=1000000)
    competitor_avg_price: float = Field(ge=0, le=100000)
    compliance_risk: str = Field(default="unknown", max_length=20)
    seasonality: str = Field(default="", max_length=200)
    notes: str = Field(default="", max_length=2000)


class WatchlistInput(BaseModel):
    note: str = Field(default="", max_length=500)


@app.get("/api/opportunities")
def list_opportunities(
    market: str = "US",
    category: str = "",
    platform: str = "",
    q: str = "",
    min_score: int = Query(0, ge=0, le=100),
    min_comments: int = Query(0, ge=0, le=1000000),
    trend: str = "",
    risk: str = "",
    sort: str = "score",
):
    market = normalize_market(market)
    clauses = ["market = %s", "score >= %s"]
    params: list[object] = [market, min_score]
    if category:
        clauses.append("category = %s")
        params.append(category)
    if platform:
        clauses.append("platform = %s")
        params.append(platform)
    if q:
        clauses.append("(product ILIKE %s OR gap ILIKE %s OR pain_points ILIKE %s)")
        needle = f"%{q}%"
        params.extend([needle, needle, needle])
    if min_comments:
        clauses.append("comment_count >= %s")
        params.append(min_comments)
    if trend == "rising":
        clauses.append("momentum >= 70")
    elif trend == "stable":
        clauses.append("momentum >= 40 AND momentum < 70")
    elif trend == "cooling":
        clauses.append("momentum < 40")
    if risk:
        clauses.append("risk ILIKE %s")
        params.append(f"%{risk}%")
    order = {"score": "score DESC, created_at DESC", "momentum": "momentum DESC, score DESC",
             "demand": "demand DESC, score DESC", "comments": "comment_count DESC, score DESC",
             "recent": "created_at DESC"}.get(sort, "score DESC, created_at DESC")
    query = f"""SELECT o.id,o.product,o.category,o.platform,o.score,o.momentum,o.cross_platform,
      o.demand,o.gap,o.risk,o.source_url,o.comment_count,o.pain_points,o.created_at,o.published_at,o.market,
      EXISTS(SELECT 1 FROM watchlists w WHERE w.opportunity_id=o.id AND w.market=o.market AND w.user_key='local') AS watchlisted
      FROM opportunities o WHERE {' AND '.join('o.' + x if x.startswith(('market','category','platform','score','product','gap','pain_points','comment_count','risk','momentum')) else x for x in clauses)}
      ORDER BY {order} LIMIT 200"""
    with db() as conn:
        rows = conn.execute(query, params).fetchall()
    keys = ["id", "product", "category", "platform", "score", "momentum", "cross_platform",
            "demand", "gap", "risk", "source_url", "comment_count", "pain_points", "created_at", "published_at", "market", "watchlisted"]
    result = []
    for row in rows:
        item = dict(zip(keys, row))
        item["trend_state"] = "rising" if item["momentum"] >= 70 else ("stable" if item["momentum"] >= 40 else "cooling")
        result.append(item)
    return result


@app.get("/api/summary")
def summary(market: str = "US"):
    market = normalize_market(market)
    with db() as conn:
        total, avg, high, comments, today = conn.execute(
            """SELECT count(*), coalesce(round(avg(score)),0),
            count(*) FILTER (WHERE score>=70), coalesce(sum(comment_count),0),
            count(*) FILTER (WHERE created_at >= CURRENT_DATE)
            FROM opportunities WHERE market=%s""", (market,)
        ).fetchone()
        categories = conn.execute(
            "SELECT category, count(*) FROM opportunities WHERE market=%s GROUP BY category ORDER BY count(*) DESC", (market,)
        ).fetchall()
        platforms = conn.execute(
            "SELECT platform, count(*) FROM opportunities WHERE market=%s GROUP BY platform ORDER BY count(*) DESC", (market,)
        ).fetchall()
    return {
        "total": total, "average_score": avg, "high_priority": high, "today_new": today,
        "comment_count": comments,
        "categories": [{"name": x[0], "count": x[1]} for x in categories],
        "platforms": [{"name": x[0], "count": x[1]} for x in platforms], "market": market,
    }


@app.get("/api/filters")
def filters(market: str = "US"):
    market = normalize_market(market)
    with db() as conn:
        categories = conn.execute("SELECT DISTINCT category FROM opportunities WHERE market=%s ORDER BY category", (market,)).fetchall()
        platforms = conn.execute("SELECT DISTINCT platform FROM opportunities WHERE market=%s ORDER BY platform", (market,)).fetchall()
    return {"categories": [x[0] for x in categories], "platforms": [x[0] for x in platforms]}


@app.get("/api/markets")
def markets():
    enabled = enabled_markets()
    return [{"code": code, **MARKETS[code], "collection_enabled": code in enabled} for code in MARKET_ORDER]


@app.get("/api/trends")
def trends(days: int = Query(30, ge=7, le=180), market: str = "US"):
    market = normalize_market(market)
    with db() as conn:
        rows = conn.execute("""SELECT snapshot_date,category,platform,opportunity_count,
          avg_score,avg_momentum,avg_demand,comment_count
          FROM trend_snapshots WHERE market=%s AND snapshot_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
          ORDER BY snapshot_date ASC, category, platform""", (market, days)).fetchall()
    keys = ["date", "category", "platform", "opportunity_count", "avg_score", "avg_momentum", "avg_demand", "comment_count"]
    return [dict(zip(keys, row)) for row in rows]


@app.get("/api/pains")
def pains(
    limit: int = Query(12, ge=1, le=50),
    market: str = "US",
    platform: str = "",
    sentiment: str = "",
    from_date: str = "",
    to_date: str = "",
):
    market = normalize_market(market)
    clauses = ["o.market=%s"]
    params: list[object] = [market]
    if platform:
        clauses.append("o.platform=%s")
        params.append(platform)
    if sentiment in {"positive", "neutral", "negative", "unknown"}:
        clauses.append("c.sentiment=%s")
        params.append(sentiment)
    if from_date:
        clauses.append("COALESCE(c.published_at, c.created_at) >= %s::date")
        params.append(from_date)
    if to_date:
        clauses.append("COALESCE(c.published_at, c.created_at) < (%s::date + INTERVAL '1 day')")
        params.append(to_date)
    with db() as conn:
        rows = conn.execute(f"""SELECT c.pain_label, count(*), coalesce(sum(c.likes),0),
          max(c.comment_text), min(c.published_at), max(c.published_at),
          count(*) FILTER (WHERE c.sentiment='negative'),
          count(*) FILTER (WHERE c.sentiment='positive'),
          count(*) FILTER (WHERE c.sentiment='neutral'),
          count(*) FILTER (WHERE c.sentiment='unknown')
          FROM comments c JOIN opportunities o ON o.id=c.opportunity_id
          WHERE {' AND '.join(clauses)} GROUP BY c.pain_label ORDER BY count(*) DESC LIMIT %s""", (*params, limit)).fetchall()
        total = conn.execute(f"SELECT count(*) FROM comments c JOIN opportunities o ON o.id=c.opportunity_id WHERE {' AND '.join(clauses)}", params).fetchone()[0]
    return [{"label": x[0], "count": x[1], "share": round((x[1] / total * 100), 1) if total else 0,
             "likes": x[2], "example": x[3], "first_comment_at": x[4], "last_comment_at": x[5],
             "negative_count": x[6], "positive_count": x[7], "neutral_count": x[8], "unknown_count": x[9],
             "negative_ratio": round((x[6] / x[1] * 100), 1) if x[1] else 0} for x in rows]


@app.get("/api/pains/{pain_label}")
def pain_detail(
    pain_label: str,
    market: str = "US",
    platform: str = "",
    sentiment: str = "",
    from_date: str = "",
    to_date: str = "",
    offset: int = Query(0, ge=0, le=100000),
    limit: int = Query(20, ge=1, le=100),
):
    """Return review evidence behind one pain label for interactive drill-down."""
    market = normalize_market(market)
    with db() as conn:
        clauses = ["c.pain_label=%s", "o.market=%s"]
        params: list[object] = [pain_label, market]
        if platform:
            clauses.append("o.platform=%s")
            params.append(platform)
        if sentiment in {"positive", "neutral", "negative", "unknown"}:
            clauses.append("c.sentiment=%s")
            params.append(sentiment)
        if from_date:
            clauses.append("COALESCE(c.published_at, c.created_at) >= %s::date")
            params.append(from_date)
        if to_date:
            clauses.append("COALESCE(c.published_at, c.created_at) < (%s::date + INTERVAL '1 day')")
            params.append(to_date)
        rows = conn.execute(f"""SELECT c.comment_text,c.likes,c.replies,c.pain_label,c.comment_url,
          c.published_at,c.created_at,c.sentiment,c.sentiment_score,c.aspect,c.classification_confidence,
          c.review_status,c.language,c.source_post_id,o.id,o.product,o.category,o.platform,o.score,o.source_url
          FROM comments c JOIN opportunities o ON o.id=c.opportunity_id
          WHERE {' AND '.join(clauses)} ORDER BY c.likes DESC,COALESCE(c.published_at,c.created_at) DESC OFFSET %s LIMIT %s""", (*params, offset, limit)).fetchall()
        total = conn.execute(f"SELECT count(*) FROM comments c JOIN opportunities o ON o.id=c.opportunity_id WHERE {' AND '.join(clauses)}", params).fetchone()[0]
    comments = []
    products = {}
    for row in rows:
        comment = dict(zip(["text", "likes", "replies", "pain_label", "url", "published_at", "created_at", "sentiment", "sentiment_score", "aspect", "confidence", "review_status", "language", "source_post_id"], row[:14]))
        product = dict(zip(["id", "product", "category", "platform", "score", "source_url"], row[14:]))
        comment["opportunity"] = product
        comments.append(comment)
        products[product["id"]] = product
    return {"label": pain_label, "count": total, "offset": offset, "limit": limit,
            "has_more": offset + len(comments) < total, "comments": comments,
            "opportunities": list(products.values())}


@app.get("/api/pain-detail")
def pain_detail_query(
    label: str = Query(..., min_length=1, max_length=100),
    market: str = "US",
    platform: str = "",
    sentiment: str = "",
    from_date: str = "",
    to_date: str = "",
    offset: int = Query(0, ge=0, le=100000),
    limit: int = Query(20, ge=1, le=100),
):
    """Slash-safe pain detail endpoint for labels such as 'comfort / noise'."""
    return pain_detail(label, market, platform, sentiment, from_date, to_date, offset, limit)


@app.get("/api/pains/timeseries")
def pain_timeseries(
    label: str = Query(..., min_length=1, max_length=100),
    days: int = Query(30, ge=7, le=180),
    market: str = "US",
    platform: str = "",
):
    market = normalize_market(market)
    clauses = ["c.pain_label=%s", "o.market=%s", "COALESCE(c.published_at,c.created_at) >= CURRENT_DATE - (%s * INTERVAL '1 day')"]
    params: list[object] = [label, market, days]
    if platform:
        clauses.append("o.platform=%s")
        params.append(platform)
    with db() as conn:
        rows = conn.execute(f"""SELECT COALESCE(c.published_at::date,c.created_at::date) AS day,
          count(*) AS count,
          count(*) FILTER (WHERE c.sentiment='negative') AS negative_count,
          count(*) FILTER (WHERE c.sentiment='positive') AS positive_count
          FROM comments c JOIN opportunities o ON o.id=c.opportunity_id
          WHERE {' AND '.join(clauses)} GROUP BY day ORDER BY day""", params).fetchall()
    return [{"date": x[0], "count": x[1], "negative_count": x[2], "positive_count": x[3]} for x in rows]


@app.get("/api/pain-timeseries")
def pain_timeseries_alias(
    label: str = Query(..., min_length=1, max_length=100),
    days: int = Query(30, ge=7, le=180),
    market: str = "US",
    platform: str = "",
):
    """Stable alias kept separate from /api/pains/{pain_label} route matching."""
    return pain_timeseries(label, days, market, platform)


@app.get("/api/benchmarks")
def benchmarks(market: str = "US"):
    market = normalize_market(market)
    with db() as conn:
        rows = conn.execute("""SELECT category,platform,count(*),round(avg(score)),
          max(score),round(avg(momentum)),round(avg(demand)),coalesce(sum(comment_count),0)
          FROM opportunities WHERE market=%s GROUP BY category,platform ORDER BY avg(score) DESC""", (market,)).fetchall()
    keys = ["category", "platform", "opportunity_count", "avg_score", "top_score", "avg_momentum", "avg_demand", "comments"]
    return [dict(zip(keys, row)) for row in rows]


@app.get("/api/watchlist")
def list_watchlist(market: str = "US"):
    market = normalize_market(market)
    with db() as conn:
        rows = conn.execute("""SELECT w.opportunity_id,w.note,w.created_at,
          o.product,o.category,o.platform,o.score,o.momentum,o.demand,o.comment_count,o.source_url
          FROM watchlists w JOIN opportunities o ON o.id=w.opportunity_id
          WHERE w.user_key='local' AND w.market=%s ORDER BY w.created_at DESC""", (market,)).fetchall()
    keys = ["opportunity_id", "note", "created_at", "product", "category", "platform", "score", "momentum", "demand", "comment_count", "source_url"]
    return [dict(zip(keys, row)) for row in rows]


@app.post("/api/watchlist/{opportunity_id}")
def add_watchlist(opportunity_id: int, payload: WatchlistInput, market: str = "US"):
    market = normalize_market(market)
    with db() as conn:
        exists = conn.execute("SELECT 1 FROM opportunities WHERE id=%s AND market=%s", (opportunity_id, market)).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="Opportunity not found")
        row = conn.execute("""INSERT INTO watchlists(user_key,market,opportunity_id,note)
          VALUES('local',%s,%s,%s)
          ON CONFLICT(user_key,market,opportunity_id) DO UPDATE SET note=EXCLUDED.note
          RETURNING opportunity_id,note,created_at""", (market, opportunity_id, payload.note)).fetchone()
    return {"saved": True, "market": market, "opportunity_id": row[0], "note": row[1], "created_at": row[2]}


@app.delete("/api/watchlist/{opportunity_id}")
def remove_watchlist(opportunity_id: int, market: str = "US"):
    market = normalize_market(market)
    with db() as conn:
        conn.execute("DELETE FROM watchlists WHERE user_key='local' AND market=%s AND opportunity_id=%s", (market, opportunity_id))
    return {"saved": False, "market": market, "opportunity_id": opportunity_id}


@app.get("/api/export-pains.csv")
def export_pains_csv(
    market: str = "US", platform: str = "", sentiment: str = "",
    from_date: str = "", to_date: str = "",
):
    market = normalize_market(market)
    clauses = ["o.market=%s"]
    params: list[object] = [market]
    if platform:
        clauses.append("o.platform=%s")
        params.append(platform)
    if sentiment in {"positive", "neutral", "negative", "unknown"}:
        clauses.append("c.sentiment=%s")
        params.append(sentiment)
    if from_date:
        clauses.append("COALESCE(c.published_at,c.created_at) >= %s::date")
        params.append(from_date)
    if to_date:
        clauses.append("COALESCE(c.published_at,c.created_at) < (%s::date + INTERVAL '1 day')")
        params.append(to_date)
    with db() as conn:
        rows = conn.execute(f"""SELECT c.pain_label,count(*),coalesce(sum(c.likes),0),coalesce(sum(c.replies),0),
          count(*) FILTER (WHERE c.sentiment='negative'),count(*) FILTER (WHERE c.sentiment='positive'),
          count(*) FILTER (WHERE c.sentiment='neutral'),count(*) FILTER (WHERE c.sentiment='unknown'),
          min(COALESCE(c.published_at,c.created_at)),max(COALESCE(c.published_at,c.created_at)),max(c.comment_text)
          FROM comments c JOIN opportunities o ON o.id=c.opportunity_id
          WHERE {' AND '.join(clauses)} GROUP BY c.pain_label ORDER BY count(*) DESC""", params).fetchall()
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["pain_label", "comment_count", "likes", "replies", "negative_count", "positive_count", "neutral_count", "unknown_count", "first_comment_at", "last_comment_at", "example_comment", "market", "platform_filter"])
    writer.writerows([(*row, market, platform) for row in rows])
    return Response(out.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=trend-radar-pains.csv"})


@app.get("/api/opportunities/{opportunity_id}")
def opportunity_detail(opportunity_id: int):
    with db() as conn:
        row = conn.execute("""SELECT id,product,category,platform,score,momentum,cross_platform,
          demand,gap,risk,source_url,comment_count,pain_points,created_at,published_at,market,
          EXISTS(SELECT 1 FROM watchlists w WHERE w.opportunity_id=opportunities.id AND w.market=opportunities.market AND w.user_key='local')
          FROM opportunities WHERE id=%s""", (opportunity_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Opportunity not found")
        comments = conn.execute("""SELECT comment_text,likes,replies,pain_label,comment_url,published_at,created_at
          FROM comments WHERE opportunity_id=%s ORDER BY likes DESC, created_at DESC LIMIT 30""", (opportunity_id,)).fetchall()
    keys = ["id", "product", "category", "platform", "score", "momentum", "cross_platform", "demand",
            "gap", "risk", "source_url", "comment_count", "pain_points", "created_at", "published_at", "market", "watchlisted"]
    comment_keys = ["text", "likes", "replies", "pain_label", "url", "published_at", "created_at"]
    result = dict(zip(keys, row))
    result["comments"] = [dict(zip(comment_keys, x)) for x in comments]
    return result


@app.get("/api/opportunities/{opportunity_id}/benchmark")
def opportunity_benchmark(opportunity_id: int):
    with db() as conn:
        product = conn.execute("""SELECT id,product,category,platform,market,score,momentum,demand,comment_count
          FROM opportunities WHERE id=%s""", (opportunity_id,)).fetchone()
        if not product:
            raise HTTPException(status_code=404, detail="Opportunity not found")
        base = conn.execute("""SELECT count(*),round(avg(score)),max(score),round(avg(momentum)),round(avg(demand)),coalesce(sum(comment_count),0)
          FROM opportunities WHERE market=%s AND category=%s AND platform=%s""", (product[4], product[2], product[3])).fetchone()
        category = conn.execute("""SELECT count(*),round(avg(score)),round(avg(momentum)),round(avg(demand))
          FROM opportunities WHERE market=%s AND category=%s""", (product[4], product[2])).fetchone()
    keys = ["id", "product", "category", "platform", "market", "score", "momentum", "demand", "comment_count"]
    item = dict(zip(keys, product))
    benchmark = dict(zip(["opportunity_count", "avg_score", "top_score", "avg_momentum", "avg_demand", "comments"], base))
    category_base = dict(zip(["opportunity_count", "avg_score", "avg_momentum", "avg_demand"], category))
    return {"opportunity": item, "same_category_platform": benchmark, "same_category": category_base,
            "score_delta": (item["score"] - (benchmark["avg_score"] or 0)) if benchmark["opportunity_count"] else None,
            "note": "对标值来自已采集信号，不代表销量、市场份额或利润。"}


def _validation_result(opportunity_id: int, values: dict):
    with db() as conn:
        opportunity = conn.execute("SELECT score,momentum,demand,risk FROM opportunities WHERE id=%s", (opportunity_id,)).fetchone()
        if not opportunity:
            raise HTTPException(status_code=404, detail="Opportunity not found")
        conn.execute("""INSERT INTO validation_checks
          (opportunity_id,target_price,product_cost,inbound_shipping,platform_fee_pct,fulfillment_fee,
           ad_cost_pct,return_rate_pct,other_cost,competitor_count,competitor_avg_price,compliance_risk,seasonality,notes,updated_at)
          VALUES(%(opportunity_id)s,%(target_price)s,%(product_cost)s,%(inbound_shipping)s,%(platform_fee_pct)s,%(fulfillment_fee)s,
           %(ad_cost_pct)s,%(return_rate_pct)s,%(other_cost)s,%(competitor_count)s,%(competitor_avg_price)s,%(compliance_risk)s,%(seasonality)s,%(notes)s,now())
          ON CONFLICT(opportunity_id) DO UPDATE SET target_price=EXCLUDED.target_price,product_cost=EXCLUDED.product_cost,
           inbound_shipping=EXCLUDED.inbound_shipping,platform_fee_pct=EXCLUDED.platform_fee_pct,
           fulfillment_fee=EXCLUDED.fulfillment_fee,ad_cost_pct=EXCLUDED.ad_cost_pct,return_rate_pct=EXCLUDED.return_rate_pct,
           other_cost=EXCLUDED.other_cost,competitor_count=EXCLUDED.competitor_count,competitor_avg_price=EXCLUDED.competitor_avg_price,
           compliance_risk=EXCLUDED.compliance_risk,seasonality=EXCLUDED.seasonality,notes=EXCLUDED.notes,updated_at=now()""", {**values, "opportunity_id": opportunity_id})
    price = float(values["target_price"])
    fixed_cost = sum(float(values[k]) for k in ("product_cost", "inbound_shipping", "fulfillment_fee", "other_cost"))
    variable_rate = (float(values["platform_fee_pct"]) + float(values["ad_cost_pct"]) + float(values["return_rate_pct"])) / 100
    variable_cost = price * variable_rate
    total_cost = fixed_cost + variable_cost
    profit = price - total_cost
    margin = (profit / price * 100) if price else 0
    break_even = (fixed_cost / (1 - variable_rate)) if price and variable_rate < 1 else 0
    score, momentum, demand, opportunity_risk = [int(x or 0) if i < 3 else str(x or "") for i, x in enumerate(opportunity)]
    compliance = str(values["compliance_risk"] or "unknown").lower()
    if not price:
        recommendation = "待填写"
    elif compliance == "high" or margin < 15 or (score < 55 and margin < 25):
        recommendation = "暂缓"
    elif score >= 70 and momentum >= 60 and margin >= 25 and compliance != "high":
        recommendation = "推荐测试"
    else:
        recommendation = "继续观察"
    reasons = []
    if score >= 70:
        reasons.append("社媒信号达到候选线")
    elif score:
        reasons.append("社媒信号仍需积累")
    if margin >= 25:
        reasons.append("预计单位毛利率达到 25%")
    elif price:
        reasons.append("预计毛利率低于 25%，需优化成本或售价")
    if values["competitor_count"]:
        reasons.append(f"已记录 {values['competitor_count']} 个竞品")
    if compliance == "high":
        reasons.append("合规风险较高，先完成人工核验")
    return {"input": values, "economics": {"fixed_cost": round(fixed_cost, 2), "variable_rate_pct": round(variable_rate * 100, 2),
      "variable_cost": round(variable_cost, 2), "total_cost": round(total_cost, 2), "profit": round(profit, 2),
      "margin_pct": round(margin, 2), "break_even_price": round(break_even, 2)},
      "signal": {"score": score, "momentum": momentum, "demand": demand, "risk": opportunity_risk},
      "recommendation": recommendation, "reasons": reasons}


@app.get("/api/opportunities/{opportunity_id}/validation")
def get_validation(opportunity_id: int):
    defaults = {"target_price": 0, "product_cost": 0, "inbound_shipping": 0, "platform_fee_pct": 0,
                "fulfillment_fee": 0, "ad_cost_pct": 0, "return_rate_pct": 0, "other_cost": 0,
                "competitor_count": 0, "competitor_avg_price": 0, "compliance_risk": "unknown", "seasonality": "", "notes": ""}
    with db() as conn:
        row = conn.execute("""SELECT target_price,product_cost,inbound_shipping,platform_fee_pct,fulfillment_fee,
          ad_cost_pct,return_rate_pct,other_cost,competitor_count,competitor_avg_price,compliance_risk,seasonality,notes
          FROM validation_checks WHERE opportunity_id=%s""", (opportunity_id,)).fetchone()
    values = defaults if not row else dict(zip(defaults, row))
    return _validation_result(opportunity_id, values)


@app.put("/api/opportunities/{opportunity_id}/validation")
def save_validation(opportunity_id: int, payload: ValidationInput):
    return _validation_result(opportunity_id, payload.model_dump())


@app.get("/api/status")
def status(market: str = "US"):
    market = normalize_market(market)
    with db() as conn:
        last = conn.execute("""SELECT keyword,status,new_items,error_message,started_at,finished_at,platform,source_kind
          FROM collector_runs WHERE market=%s ORDER BY started_at DESC LIMIT 1""", (market,)).fetchone()
        sources = conn.execute("""SELECT source_key,market,display_name,kind,enabled,configured,status,
          rows_collected,error_message,last_run_at,updated_at
          FROM source_status WHERE market=%s
          ORDER BY CASE kind WHEN 'social' THEN 0 WHEN 'search' THEN 1 ELSE 2 END, display_name""", (market,)).fetchall()
    result = {"app": "ok", "brightdata_configured": bool(os.environ.get("BRIGHTDATA_API_TOKEN")),
              "google_trends_enabled": os.environ.get("GOOGLE_TRENDS_ENABLED", "false").lower() in {"1", "true", "yes"},
              "last_run": None, "sources": [], "market": market,
              "market_collection_enabled": market in enabled_markets()}
    if last:
        result["last_run"] = dict(zip(["keyword", "status", "new_items", "error", "started_at", "finished_at", "platform", "source_kind"], last))
    for row in sources:
        result["sources"].append(dict(zip(["key", "market", "name", "kind", "enabled", "configured", "status", "rows_collected", "error", "last_run_at", "updated_at"], row)))
    return result


@app.get("/api/google-trends")
def google_trends(days: int = Query(30, ge=1, le=90), market: str = "US"):
    market = normalize_market(market)
    with db() as conn:
        rows = conn.execute("""SELECT keyword,region,trend_date,interest,related_query,related_value,source_url
          FROM google_trends WHERE region=%s AND trend_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
          ORDER BY trend_date DESC, interest DESC, related_value DESC LIMIT 300""", (market, days)).fetchall()
    keys = ["keyword", "region", "date", "interest", "related_query", "related_value", "source_url"]
    return [dict(zip(keys, row)) for row in rows]


@app.get("/api/reports/{report_type}")
def report(report_type: str, market: str = "US"):
    valid = {"today", "weekly", "high", "pains", "benchmarks"}
    if report_type not in valid:
        raise HTTPException(status_code=400, detail="Unknown report type")
    market = normalize_market(market)
    with db() as conn:
        if report_type == "pains":
            rows = conn.execute("""SELECT pain_label,count(*),coalesce(sum(likes),0),max(comment_text)
              FROM comments c JOIN opportunities o ON o.id=c.opportunity_id
              WHERE o.market=%s GROUP BY pain_label ORDER BY count(*) DESC""", (market,)).fetchall()
            lines = ["# 评论痛点报告", "", "> 机器关键词归类，需人工复核。"]
            lines += [f"- {x[0]}：{x[1]} 条，点赞 {x[2]}；示例：{x[3]}" for x in rows]
        elif report_type == "benchmarks":
            rows = conn.execute("""SELECT category,platform,count(*),round(avg(score)),max(score),round(avg(momentum)),round(avg(demand))
              FROM opportunities WHERE market=%s GROUP BY category,platform ORDER BY avg(score) DESC""", (market,)).fetchall()
            lines = ["# 类目与平台基准报告", "", "| 类目 | 平台 | 信号数 | 平均分 | 最高分 | 平均动能 | 平均需求 |", "|---|---|---:|---:|---:|---:|---:|"]
            lines += [f"| {x[0]} | {x[1]} | {x[2]} | {x[3]} | {x[4]} | {x[5]} | {x[6]} |" for x in rows]
        else:
            condition = "score >= 70" if report_type == "high" else ("created_at >= CURRENT_DATE" if report_type == "today" else "created_at >= CURRENT_DATE - INTERVAL '7 days'")
            condition = f"market = '{market}' AND {condition}"
            rows = conn.execute(f"""SELECT product,category,platform,score,momentum,demand,comment_count,gap,risk,source_url
              FROM opportunities WHERE {condition} ORDER BY score DESC LIMIT 100""").fetchall()
            title = {"today": "今日机会报告", "weekly": "周趋势机会报告", "high": "高优先级机会报告"}[report_type]
            lines = [f"# {title}", "", "> 机会分是社媒信号，不等于销量或利润结论。", "", "| 产品 | 类目 | 平台 | 机会分 | 动能 | 需求 | 评论 | 下一步 |", "|---|---|---|---:|---:|---:|---:|---|"]
            lines += [f"| {x[0].replace('|','/')} | {x[1]} | {x[2]} | {x[3]} | {x[4]} | {x[5]} | {x[6]} | {x[7].replace('|','/')} |" for x in rows]
    return {"type": report_type, "markdown": "\n".join(lines)}


@app.get("/api/export.csv")
def export_csv(market: str = "US"):
    market = normalize_market(market)
    with db() as conn:
        rows = conn.execute("""SELECT product,category,platform,score,momentum,demand,comment_count,pain_points,gap,risk,source_url,created_at
          FROM opportunities WHERE market=%s ORDER BY score DESC,created_at DESC""", (market,)).fetchall()
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["product", "category", "platform", "score", "momentum", "demand", "comments", "pain_points", "gap", "risk", "source_url", "created_at"])
    writer.writerows(rows)
    from fastapi.responses import Response
    return Response(out.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=trend-radar-opportunities.csv"})


@app.post("/api/opportunities", status_code=201)
def add_opportunity(item: Opportunity):
    score = round(item.momentum * .35 + item.cross_platform * .25 + item.demand * .25 + 15)
    with db() as conn:
        row = conn.execute("""INSERT INTO opportunities
          (product,category,platform,score,momentum,cross_platform,demand,gap,risk,source_url)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
          (item.product, item.category, item.platform, score, item.momentum,
           item.cross_platform, item.demand, item.gap, item.risk, item.source_url)).fetchone()
    return {"id": row[0], "score": score}


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc)}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8"))
    return r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>US Trend Radar</title><style>
:root{--ink:#172033;--muted:#64748b;--line:#e7ebf3;--blue:#2563eb;--bg:#f5f7fb;--card:#fff}*{box-sizing:border-box}body{font:14px/1.5 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:var(--bg);color:var(--ink);margin:0}.shell{max-width:1500px;margin:auto;padding:34px 28px 60px}h1{font-size:30px;letter-spacing:-.5px;margin:0}h2{font-size:18px;margin:0 0 5px}.sub{color:var(--muted);margin:7px 0 0}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:24px 0 16px}.card,.metric{background:var(--card);border:1px solid var(--line);border-radius:16px;box-shadow:0 6px 24px #1f3a5f0b}.metric{padding:18px 20px}.metric .label{color:var(--muted);font-size:12px}.metric b{display:block;font-size:29px;margin-top:7px}.card{padding:20px;margin-top:16px}.toolbar{display:flex;flex-wrap:wrap;gap:10px;margin:14px 0}.toolbar input,.toolbar select{border:1px solid #d6ddea;border-radius:9px;padding:9px 11px;background:white;color:var(--ink)}button{border:0;border-radius:9px;padding:9px 15px;background:var(--blue);color:white;cursor:pointer}button.secondary{background:#edf3ff;color:var(--blue)}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:13px 10px;border-bottom:1px solid var(--line);vertical-align:top}th{font-size:12px;color:var(--muted);font-weight:600}.score{font-weight:800;color:var(--blue);font-size:17px}.tag{display:inline-block;background:#edf3ff;color:var(--blue);border-radius:99px;padding:3px 8px;font-size:12px}.muted{color:var(--muted);font-size:12px}.pill{display:inline-block;padding:4px 8px;border-radius:7px;background:#f1f5f9;margin:2px 3px 2px 0;font-size:12px}.section-head{display:flex;justify-content:space-between;align-items:center;gap:8px}.note{background:#fff9e8;border:1px solid #f3df9d;color:#7c5d0b;padding:11px 13px;border-radius:10px;margin:12px 0;font-size:12px}.chart{width:100%;height:220px}.empty{color:var(--muted);padding:20px 0}.split{display:grid;grid-template-columns:1.2fr .8fr;gap:16px}.bar{height:8px;background:#e8eefb;border-radius:9px;overflow:hidden}.bar i{display:block;height:100%;background:var(--blue)}a{color:var(--blue)}@media(max-width:1000px){.grid{grid-template-columns:repeat(2,1fr)}.split{grid-template-columns:1fr}}@media(max-width:650px){.shell{padding:22px 14px}.grid{grid-template-columns:1fr}table{min-width:900px}.card.table-wrap{overflow:auto}}
</style></head><body><main class="shell"><h1>US Trend Radar <span class="tag">美区社媒选品</span></h1><p class="sub">每日信号 · TikTok 发现 · 趋势曲线 · 评论痛点 · 类目基准</p><section class="grid"><div class="metric"><span class="label">机会信号</span><b id="total">—</b></div><div class="metric"><span class="label">平均机会分</span><b id="avg">—</b></div><div class="metric"><span class="label">优先验证（≥70）</span><b id="high">—</b></div><div class="metric"><span class="label">已采集评论</span><b id="comments">—</b></div></section><section class="card"><div class="section-head"><div><h2>机会筛选</h2><span class="muted">按信号筛选，点击来源可回看原帖</span></div><button class="secondary" id="reset">重置</button></div><div class="toolbar"><input id="q" placeholder="搜索产品、痛点或改进方向"><select id="category"><option value="">全部类目</option></select><select id="platform"><option value="">全部平台</option></select><select id="min"><option value="0">最低分：不限</option><option value="70">最低分：70</option><option value="80">最低分：80</option><option value="90">最低分：90</option></select><button id="filter">应用筛选</button></div></section><section class="card table-wrap"><div class="section-head"><h2>机会榜</h2><span class="muted" id="updated"></span></div><div class="note">机会分是社媒需求信号，不是销量或利润结论；采购前仍需核验竞品、成本、知识产权、安全与平台规则。</div><table><thead><tr><th>产品机会</th><th>类目</th><th>平台</th><th>机会分</th><th>动能 / 需求</th><th>评论痛点</th><th>下一步</th><th>来源</th></tr></thead><tbody id="rows"></tbody></table></section><div class="split"><section class="card"><div class="section-head"><h2>趋势曲线（近30天）</h2><span class="muted">每日快照</span></div><canvas class="chart" id="trendChart"></canvas><div id="trendLegend" class="muted"></div></section><section class="card"><div class="section-head"><h2>评论痛点</h2><span class="muted">关键词归类，需人工复核</span></div><div id="pains"></div></section></div><section class="card table-wrap"><div class="section-head"><h2>竞品/类目基准</h2><span class="muted">同类目与平台的信号对标，不等于市场销量</span></div><table><thead><tr><th>类目</th><th>平台</th><th>信号数</th><th>平均分</th><th>最高分</th><th>平均动能</th><th>平均需求</th><th>评论数</th></tr></thead><tbody id="benchmarks"></tbody></table></section></main><script>
const $=id=>document.getElementById(id);const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function loadFilters(){const d=await fetch('/api/filters').then(r=>r.json());for(const x of d.categories){$('category').insertAdjacentHTML('beforeend',`<option>${esc(x)}</option>`)}for(const x of d.platforms){$('platform').insertAdjacentHTML('beforeend',`<option>${esc(x)}</option>`)}}
async function loadRows(){const p=new URLSearchParams({category:$('category').value,platform:$('platform').value,q:$('q').value,min_score:$('min').value});const data=await fetch('/api/opportunities?'+p).then(r=>r.json());$('rows').innerHTML=data.length?data.map(x=>`<tr><td><b>${esc(x.product)}</b><div class="muted">${esc(x.risk)}</div></td><td><span class="tag">${esc(x.category)}</span></td><td>${esc(x.platform)}</td><td class="score">${x.score}</td><td>${x.momentum} 动能<br>${x.demand} 需求</td><td>${x.comment_count?`<b>${x.comment_count}</b> 条<br>`:''}${esc(x.pain_points||'暂无评论归类')}</td><td>${esc(x.gap)}</td><td>${x.source_url?`<a target="_blank" rel="noreferrer" href="${esc(x.source_url)}">原帖</a>`:'—'}</td></tr>`).join(''):'<tr><td colspan="8" class="empty">暂无符合条件的信号</td></tr>';$('updated').textContent=`更新于 ${new Date().toLocaleString()}`}
async function loadSummary(){const s=await fetch('/api/summary').then(r=>r.json());$('total').textContent=s.total;$('avg').textContent=s.average_score;$('high').textContent=s.high_priority;$('comments').textContent=s.comment_count}
async function loadPains(){const d=await fetch('/api/pains').then(r=>r.json());$('pains').innerHTML=d.length?d.map(x=>`<div style="margin:13px 0"><div><span class="pill">${esc(x.label)}</span><b>${x.count}</b> 条 <span class="muted">${x.likes} 赞</span></div><div class="muted">例：${esc(x.example)}</div></div>`).join(''):'<div class="empty">评论数据将在下一轮采集后出现</div>'}
async function loadBenchmarks(){const d=await fetch('/api/benchmarks').then(r=>r.json());$('benchmarks').innerHTML=d.length?d.map(x=>`<tr><td>${esc(x.category)}</td><td>${esc(x.platform)}</td><td>${x.opportunity_count}</td><td class="score">${x.avg_score}</td><td>${x.top_score}</td><td>${x.avg_momentum}</td><td>${x.avg_demand}</td><td>${x.comments}</td></tr>`).join(''):'<tr><td colspan="8" class="empty">暂无基准数据</td></tr>'}
async function loadTrend(){const d=await fetch('/api/trends?days=30').then(r=>r.json());const c=$('trendChart'),ctx=c.getContext('2d'),w=c.clientWidth*devicePixelRatio,h=c.clientHeight*devicePixelRatio;c.width=w;c.height=h;ctx.clearRect(0,0,w,h);if(!d.length){$('trendLegend').textContent='趋势快照将在下一轮采集后形成';return}const groups={};d.forEach(x=>(groups[x.category]??=[]).push(x));const colors=['#2563eb','#16a34a','#ea580c','#9333ea'];const max=Math.max(...d.map(x=>x.avg_score),100);Object.entries(groups).forEach(([name,vals],i)=>{ctx.strokeStyle=colors[i%colors.length];ctx.lineWidth=3;ctx.beginPath();vals.forEach((x,j)=>{const px=24+(j/Math.max(vals.length-1,1))*(w-42),py=h-28-(x.avg_score/max)*(h-48);j?ctx.lineTo(px,py):ctx.moveTo(px,py)});ctx.stroke()});$('trendLegend').innerHTML=Object.keys(groups).map((x,i)=>`<span style="color:${colors[i%colors.length]};margin-right:15px">● ${esc(x)}</span>`).join('')}
async function load(){await Promise.all([loadRows(),loadSummary(),loadPains(),loadBenchmarks(),loadTrend()])}$('filter').onclick=loadRows;$('reset').onclick=()=>{$('q').value='';$('category').value='';$('platform').value='';$('min').value='0';loadRows()};loadFilters().then(load);</script></body></html>'''
