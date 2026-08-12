import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import psycopg

DB = os.environ["DATABASE_URL"]
TOKEN = os.environ.get("BRIGHTDATA_API_TOKEN", "")
INTERVAL = int(os.environ.get("COLLECTION_INTERVAL_SECONDS", "86400"))
LIMIT = int(os.environ.get("POSTS_PER_KEYWORD", "3"))
COLLECT_COMMENTS = os.environ.get("COLLECT_COMMENTS", "true").lower() in {"1", "true", "yes"}
MAX_COMMENT_POSTS = int(os.environ.get("MAX_COMMENT_POSTS_PER_RUN", "3"))
GOOGLE_TRENDS_ENABLED = os.environ.get("GOOGLE_TRENDS_ENABLED", "false").lower() in {"1", "true", "yes"}
GOOGLE_TRENDS_KEYWORDS = [x.strip() for x in os.environ.get("GOOGLE_TRENDS_KEYWORDS", "pet cooling mat,walking pad,pantry organizer").split(",") if x.strip()]
SERP_ZONE = (os.environ.get("BRIGHTDATA_SERP_ZONE") or "serp_api1").strip()
KEYWORDS = [("pet cooling mat", "Pets"), ("under desk walking pad", "Fitness"), ("pantry organizer", "Home")]

MARKETS = {
    "US": {"name": "美国", "geo": "US"},
    "SG": {"name": "新加坡", "geo": "SG"},
    "MY": {"name": "马来西亚", "geo": "MY"},
    "TH": {"name": "泰国", "geo": "TH"},
    "VN": {"name": "越南", "geo": "VN"},
    "PH": {"name": "菲律宾", "geo": "PH"},
}
MARKET_ORDER = ["US", "SG", "MY", "TH", "VN", "PH"]
COLLECTION_MARKETS = [x.strip().upper() for x in os.environ.get("COLLECTION_MARKETS", "US").split(",")
                      if x.strip().upper() in MARKETS] or ["US"]

SOURCE_SPECS = {
    "tiktok": {"name": "TikTok", "kind": "social", "posts_default": "gd_lu702nij2f790tmv9h", "comments_default": "gd_lkf2st302ap89utw5k", "input_default": "search_keyword"},
    "instagram": {"name": "Instagram", "kind": "social", "input_default": "url", "posts_mode_default": "scrape"},
    "youtube": {"name": "YouTube", "kind": "social", "input_default": "url", "posts_mode_default": "scrape"},
    "facebook": {"name": "Facebook", "kind": "social", "input_default": "url", "posts_mode_default": "scrape"},
    "x": {"name": "X", "kind": "social", "input_default": "url", "posts_mode_default": "scrape"},
    "reddit": {"name": "Reddit", "kind": "social", "input_default": "keyword"},
    "amazon": {"name": "Amazon", "kind": "reviews", "input_default": "url", "posts_mode_default": "scrape"},
    "google_maps": {"name": "Google Maps", "kind": "reviews", "input_default": "url", "posts_mode_default": "scrape"},
}
TIKTOK_URL_DATASET_ID = "gd_lu702nij2f790tmv9h"
SOURCE_ORDER = ["tiktok", "instagram", "youtube", "facebook", "x", "reddit", "amazon", "google_maps"]
ENABLED_PLATFORMS = {x.strip().lower() for x in os.environ.get("SOCIAL_PLATFORMS_ENABLED", "tiktok").split(",") if x.strip()}

PAIN_RULES = [
    ("durability", r"\b(broke|breaks|broken|tear|ripped|durab|doesn't last|didn't last)\b"),
    ("fit / size", r"\b(size|sized|fit|fits|small|large|too big|too small|desk fit)\b"),
    ("cleaning", r"\b(clean|wash|washed|dirty|stain|odor|smell)\b"),
    ("comfort / noise", r"\b(noise|loud|quiet|comfort|comfortable|painful|hot)\b"),
    ("setup / usability", r"\b(hard|difficult|confus|setup|assemble|assembly|instructions|use)\b"),
    ("value / price", r"\b(price|expensive|cheap|worth|money|cost)\b"),
]


class SourceNotConfigured(RuntimeError):
    pass


def truthy(value):
    return str(value or "").lower() in {"1", "true", "yes", "on"}


def source_config(slug, market="US"):
    spec = SOURCE_SPECS[slug]
    prefix = f"BRIGHTDATA_{slug.upper()}"
    market = market.upper()
    posts_id = os.environ.get(f"{prefix}_POSTS_DATASET_ID_{market}") or os.environ.get(f"{prefix}_POSTS_DATASET_ID") or spec.get("posts_default", "")
    comments_id = os.environ.get(f"{prefix}_COMMENTS_DATASET_ID_{market}") or os.environ.get(f"{prefix}_COMMENTS_DATASET_ID") or spec.get("comments_default", "")
    market_inputs = os.environ.get(f"{prefix}_POSTS_INPUTS_{market}")
    posts_inputs = [x.strip() for x in (market_inputs if market_inputs is not None else os.environ.get(f"{prefix}_POSTS_INPUTS", "")).split(",") if x.strip()]
    country_field = (os.environ.get(f"{prefix}_COUNTRY_FIELD") or "").strip()
    posts_mode = (os.environ.get(f"{prefix}_POSTS_MODE") or spec.get("posts_mode_default", "discovery")).strip().lower()
    discover_by = (os.environ.get(f"{prefix}_DISCOVER_BY") or ("url" if slug == "tiktok" and posts_mode == "scrape" else "")).strip().lower()
    payload_mode = (os.environ.get(f"{prefix}_PAYLOAD_MODE") or ("wrapped" if slug == "tiktok" else "array")).strip().lower()
    market_error = ""
    if slug == "tiktok" and discover_by == "url":
        # Bright Data's official TikTok URL discovery Dataset has a fixed ID
        # and requires the wrapped {"input": [{"url": ...}]} request shape.
        # Do not reuse BRIGHTDATA_TIKTOK_POSTS_DATASET_ID here: that variable
        # is commonly set to the keyword Dataset and its schema rejects url.
        # Only the explicit URL override is allowed, and it must still match
        # Bright Data's documented URL Dataset ID.
        posts_id = os.environ.get(f"{prefix}_URL_DATASET_ID") or TIKTOK_URL_DATASET_ID
        if posts_id != TIKTOK_URL_DATASET_ID:
            market_error = f"TikTok URL Dataset ID 必须为 {TIKTOK_URL_DATASET_ID}；当前为 {posts_id}"
        payload_mode = "wrapped"
    has_market_inputs = market_inputs is not None and bool(posts_inputs)
    if market != "US" and spec["kind"] == "social" and not country_field and not has_market_inputs:
        market_error = f"当前 {spec['name']} Dataset 未配置市场字段；请配置该市场的 URL Dataset 输入后再采集"
    return {
        "slug": slug,
        "market": market,
        "name": spec["name"],
        "kind": spec["kind"],
        "enabled": slug in ENABLED_PLATFORMS,
        "posts_dataset_id": posts_id.strip(),
        "comments_dataset_id": comments_id.strip(),
        "input_key": (os.environ.get(f"{prefix}_POSTS_INPUT_KEY") or spec["input_default"]).strip(),
        "comments_input_key": (os.environ.get(f"{prefix}_COMMENTS_INPUT_KEY") or "url").strip(),
        "payload_mode": payload_mode,
        "posts_mode": posts_mode,
        "discover_by": discover_by,
        "posts_inputs": posts_inputs,
        "country_field": country_field,
        "market_error": market_error,
    }


def wait_for_db():
    for _ in range(30):
        try:
            with psycopg.connect(DB):
                return
        except Exception:
            time.sleep(2)
    raise RuntimeError("database unavailable")


def ensure_worker_schema():
    for _ in range(30):
        try:
            with psycopg.connect(DB) as conn:
                conn.execute("ALTER TABLE collector_runs ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'TikTok'")
                conn.execute("ALTER TABLE collector_runs ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'social'")
                conn.execute("ALTER TABLE collector_runs ADD COLUMN IF NOT EXISTS market TEXT NOT NULL DEFAULT 'US'")
                conn.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS market TEXT NOT NULL DEFAULT 'US'")
                conn.execute("ALTER TABLE trend_snapshots ADD COLUMN IF NOT EXISTS market TEXT NOT NULL DEFAULT 'US'")
                conn.execute("ALTER TABLE trend_snapshots DROP CONSTRAINT IF EXISTS trend_snapshots_snapshot_date_category_platform_key")
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS trend_snapshots_market_key ON trend_snapshots(snapshot_date, market, category, platform)")
                conn.execute("ALTER TABLE google_trends ADD COLUMN IF NOT EXISTS region TEXT NOT NULL DEFAULT 'US'")
                conn.execute("ALTER TABLE google_trends DROP CONSTRAINT IF EXISTS google_trends_keyword_trend_date_related_query_key")
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS google_trends_region_key ON google_trends(keyword, region, trend_date, related_query)")
                conn.execute("""CREATE TABLE IF NOT EXISTS source_status (
                  source_key TEXT NOT NULL,
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
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("database schema unavailable")


def mark_source(config, status, rows=0, error=""):
    with psycopg.connect(DB) as conn:
        conn.execute("""INSERT INTO source_status
          (source_key,market,display_name,kind,enabled,configured,status,rows_collected,error_message,last_run_at,updated_at)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())
          ON CONFLICT(source_key,market) DO UPDATE SET display_name=EXCLUDED.display_name,kind=EXCLUDED.kind,
            enabled=EXCLUDED.enabled,configured=EXCLUDED.configured,status=EXCLUDED.status,
            rows_collected=EXCLUDED.rows_collected,error_message=EXCLUDED.error_message,
            last_run_at=EXCLUDED.last_run_at,updated_at=now()""",
                      (config["slug"], config["market"], config["name"], config["kind"], config["enabled"],
                       bool(config.get("configured", config["posts_dataset_id"])), status, rows, str(error or "")[:500]))


def request_json(url, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            raw = response.read().decode().strip()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"Bright Data HTTP {exc.code}: {body[:600]}") from exc
    try:
        parsed = json.loads(raw)
        items = parsed if isinstance(parsed, list) else parsed.get("data") if isinstance(parsed, dict) and isinstance(parsed.get("data"), list) else [parsed]
    except json.JSONDecodeError:
        items = [json.loads(line) for line in raw.splitlines() if line.strip()]
    for item in items:
        if isinstance(item, dict) and (item.get("error") or item.get("status") == "error"):
            raise RuntimeError(str(item.get("error") or item))
    return items


def dataset_url(dataset_id, discovery=True, discover_by=""):
    params = {"dataset_id": dataset_id, "format": "json"}
    if discovery:
        params.update({"type": "discover_new", "discover_by": "keyword", "include_errors": "true"})
    elif discover_by:
        params.update({"type": "discover_new", "discover_by": discover_by, "include_errors": "true"})
    return "https://api.brightdata.com/datasets/v3/scrape?" + urllib.parse.urlencode(params)


def dataset_payload(config, key, value, include_limit=False):
    values = value if isinstance(value, list) else [value]
    items = []
    for entry in values:
        item = {key: entry}
        if include_limit:
            item["num_of_posts"] = LIMIT
        if config.get("country_field") and config.get("discover_by") != "url":
            item[config["country_field"]] = config["market"].lower()
        items.append(item)
    # TikTok discover-by-URL is strict: it rejects a bare array and any
    # country field. Keep the request shape correct even if an old .env still
    # contains BRIGHTDATA_TIKTOK_PAYLOAD_MODE=array.
    payload_mode = config["payload_mode"]
    if config.get("slug") == "tiktok" and config.get("discover_by") == "url":
        payload_mode = "wrapped"
    return items if payload_mode == "array" else {"input": items}


def collect_platform(config, keyword):
    if not config["posts_dataset_id"]:
        raise SourceNotConfigured(f"{config['name']} posts Dataset ID is not configured")
    discover_by = config.get("discover_by", "")
    if config.get("market_error"):
        raise SourceNotConfigured(config["market_error"])
    if discover_by == "url" and not config["posts_inputs"]:
        raise SourceNotConfigured(
            f"{config['name']} discover_by=url requires BRIGHTDATA_{config['slug'].upper()}_POSTS_INPUTS_{config['market']}"
        )
    values = config["posts_inputs"] or [keyword]
    return request_json(dataset_url(config["posts_dataset_id"], discovery=config["posts_mode"] != "scrape", discover_by=discover_by),
                        dataset_payload(config, config["input_key"], values, include_limit=discover_by != "url"))


def request_google_trends(keyword, market="US"):
    market = market.upper()
    encoded = urllib.parse.quote_plus(keyword)
    source_url = f"https://trends.google.com/trends/explore?q={encoded}&geo={market}&brd_trends=timeseries,geo_map&brd_json=1"
    payload = {"zone": SERP_ZONE, "url": source_url, "format": "raw"}
    data = json.dumps(payload).encode()
    req = urllib.request.Request("https://api.brightdata.com/request", data=data,
      headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            return json.loads(response.read().decode()), source_url
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"Bright Data Google Trends HTTP {exc.code}: {body[:600]}") from exc


def save_google_trends(keyword, market="US"):
    market = market.upper()
    parsed, source_url = request_google_trends(keyword, market)
    timeline, related = [], []
    for widget in parsed.get("widgets", []):
        data = widget.get("data", {}).get("default", {})
        if data.get("timelineData"):
            timeline = data["timelineData"]
        if widget.get("id") == "RELATED_QUERIES":
            for group in data.get("rankedList", []):
                related.extend(group.get("rankedKeyword", []))
    saved = 0
    with psycopg.connect(DB) as conn:
        for point in timeline:
            try:
                trend_date = datetime.fromtimestamp(int(point.get("time", 0)), timezone.utc).date()
                interest = int((point.get("value") or [0])[0] or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            conn.execute("""INSERT INTO google_trends(keyword,region,trend_date,interest,source_url)
              VALUES(%s,%s,%s,%s,%s) ON CONFLICT(keyword,region,trend_date,related_query) DO UPDATE SET interest=EXCLUDED.interest""",
              (keyword, market, trend_date, interest, source_url))
            saved += 1
        for item in related[:30]:
            query = str(item.get("query") or item.get("keyword") or "")[:200]
            if not query:
                continue
            value = int(item.get("value") or 0)
            conn.execute("""INSERT INTO google_trends(keyword,region,trend_date,interest,related_query,related_value,source_url)
              VALUES(%s,%s,CURRENT_DATE,0,%s,%s,%s) ON CONFLICT(keyword,region,trend_date,related_query) DO UPDATE SET related_value=EXCLUDED.related_value""",
              (keyword, market, query, value, source_url))
            saved += 1
    return saved


def safe_int(value):
    if isinstance(value, list):
        value = value[0] if value else 0
    if isinstance(value, str):
        raw = value.strip().replace(",", "")
        match = re.match(r"^([0-9]+(?:\.[0-9]+)?)([KMB])?$", raw, re.I)
        if match:
            number = float(match.group(1))
            factor = {"k": 1000, "m": 1000000, "b": 1000000000}.get((match.group(2) or "").lower(), 1)
            return int(number * factor)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def first_value(item, keys):
    for key in keys:
        value = item.get(key)
        if value not in (None, "", []):
            return value
    return ""


def classify_pain(text):
    for label, pattern in PAIN_RULES:
        if re.search(pattern, text or "", re.IGNORECASE):
            return label
    return "other"


def extract_url(item):
    value = first_value(item, ["url", "post_url", "video_url", "reel_url", "web_url", "permalink", "link", "source_url"])
    return str(value).strip() if value else ""


def extract_text(item, fallback):
    value = first_value(item, ["description", "title", "caption", "text", "content", "body", "name", "product_name"])
    return str(value or fallback).strip()[:160]


def save_posts(items, category, config, keyword):
    urls = []
    with psycopg.connect(DB) as conn:
        for item in items:
            if not isinstance(item, dict):
                continue
            url = extract_url(item)
            if not url.startswith("http"):
                continue
            plays = safe_int(first_value(item, ["play_count", "view_count", "views", "video_views", "views_count"]))
            comments = safe_int(first_value(item, ["comment_count", "comments_count", "comments", "num_comments"]))
            shares = safe_int(first_value(item, ["share_count", "shares", "reposts", "retweets"]))
            likes = safe_int(first_value(item, ["like_count", "likes", "likes_count", "upvotes", "reactions_count"]))
            momentum = min(100, round((plays / 1000000) * 45 + (shares / 1000) * 20 + (likes / 100000) * 20 + (comments / 500) * 15))
            demand = min(100, round((comments / 300) * 70 + (likes / 100000) * 20 + 10))
            score = min(100, round(momentum * .45 + demand * .30 + 25))
            product = extract_text(item, f"{config['name']} · {keyword}")
            row = conn.execute("""INSERT INTO opportunities
              (product,category,platform,score,momentum,cross_platform,demand,gap,risk,source_url,comment_count,market)
              SELECT %s,%s,%s,%s,%s,%s,%s,'Review comments and competitor gaps before sourcing',
                'Validate IP, safety and product claims',%s,%s,%s
              WHERE NOT EXISTS (SELECT 1 FROM opportunities WHERE source_url=%s AND market=%s)
              RETURNING id, source_url""",
              (product, category, config["name"], score, momentum, 25, demand, url, comments, config["market"], url, config["market"])).fetchone()
            if row:
                urls.append((row[0], row[1]))
    return urls


def collect_comments(opportunity_id, post_url, config):
    if not COLLECT_COMMENTS or not config["comments_dataset_id"] or not post_url:
        return
    try:
        items = request_json(dataset_url(config["comments_dataset_id"], discovery=False),
                             dataset_payload(config, config["comments_input_key"], post_url))
        with psycopg.connect(DB) as conn:
            for item in items:
                if not isinstance(item, dict):
                    continue
                text = str(first_value(item, ["comment_text", "comment", "text", "content"]) or "").strip()
                comment_url = str(first_value(item, ["comment_url", "url", "link"]) or "").strip()
                if not text:
                    continue
                unique_url = comment_url or f"{post_url}#comment-{abs(hash(text))}"
                conn.execute("""INSERT INTO comments(opportunity_id,platform,comment_text,likes,replies,pain_label,comment_url)
                  VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (comment_url) DO NOTHING""",
                  (opportunity_id, config["name"], text[:1000], safe_int(first_value(item, ["num_likes", "likes", "upvotes"])),
                   safe_int(first_value(item, ["num_replies", "replies", "comments_count"])), classify_pain(text), unique_url))
            labels = conn.execute("""SELECT pain_label,count(*) FROM comments WHERE opportunity_id=%s
              GROUP BY pain_label ORDER BY count(*) DESC LIMIT 3""", (opportunity_id,)).fetchall()
            pain = "; ".join(f"{label} ({count})" for label, count in labels if label != "other")
            total = conn.execute("SELECT count(*) FROM comments WHERE opportunity_id=%s", (opportunity_id,)).fetchone()[0]
            conn.execute("""UPDATE opportunities SET comment_count=%s,pain_points=%s,
              comments_checked_at=now() WHERE id=%s""", (total, pain, opportunity_id))
    except Exception as exc:
        print(f"comment collection failed for {config['name']} {post_url}: {exc}", flush=True)


def pending_comments(platform, market, limit):
    with psycopg.connect(DB) as conn:
        return conn.execute("""SELECT id,source_url FROM opportunities
          WHERE platform=%s AND market=%s AND source_url IS NOT NULL AND source_url<>'' AND comments_checked_at IS NULL
          ORDER BY score DESC LIMIT %s""", (platform, market, limit)).fetchall()


def snapshot(category, market):
    with psycopg.connect(DB) as conn:
        conn.execute("""INSERT INTO trend_snapshots(snapshot_date,market,category,platform,opportunity_count,avg_score,avg_momentum,avg_demand,comment_count)
          SELECT CURRENT_DATE,%s,category,platform,count(*),round(avg(score)),round(avg(momentum)),round(avg(demand)),coalesce(sum(comment_count),0)
          FROM opportunities WHERE category=%s AND market=%s GROUP BY category,platform
          ON CONFLICT (snapshot_date,market,category,platform) DO UPDATE SET opportunity_count=EXCLUDED.opportunity_count,
            avg_score=EXCLUDED.avg_score,avg_momentum=EXCLUDED.avg_momentum,avg_demand=EXCLUDED.avg_demand,
            comment_count=EXCLUDED.comment_count""", (market, category, market))


def run_platform(config, comment_budget):
    if not config["enabled"]:
        mark_source(config, "disabled")
        return comment_budget
    if config.get("market_error"):
        mark_source(config, "pending", error=config["market_error"])
        print(f"{config['name']} {config['market']} skipped: {config['market_error']}", flush=True)
        return comment_budget
    if not config["posts_dataset_id"]:
        mark_source(config, "pending", error="Bright Data Posts Dataset ID is not configured")
        print(f"{config['name']} skipped: posts Dataset ID is not configured", flush=True)
        return comment_budget
    if config["posts_mode"] == "scrape" and not config["posts_inputs"]:
        mark_source(config, "pending", error="Posts Dataset is scrape mode; configure BRIGHTDATA_*_POSTS_INPUTS")
        print(f"{config['name']} skipped: configure source URL inputs", flush=True)
        return comment_budget
    mark_source(config, "running")
    total_new = 0
    errors = []
    for keyword, category in KEYWORDS:
        started = None
        try:
            with psycopg.connect(DB) as conn:
                started = conn.execute("""INSERT INTO collector_runs(platform,source_kind,keyword,market,status)
                  VALUES(%s,%s,%s,%s,'running') RETURNING id""", (config["name"], config["kind"], keyword, config["market"])).fetchone()[0]
            urls = save_posts(collect_platform(config, keyword), category, config, keyword)
            total_new += len(urls)
            existing = pending_comments(config["name"], config["market"], max(comment_budget[0] - len(urls), 0))
            candidates = urls + [x for x in existing if x not in urls]
            for opportunity_id, post_url in candidates[:comment_budget[0]]:
                collect_comments(opportunity_id, post_url, config)
                comment_budget[0] -= 1
            snapshot(category, config["market"])
            with psycopg.connect(DB) as conn:
                conn.execute("UPDATE collector_runs SET status='success',new_items=%s,finished_at=now() WHERE id=%s", (len(urls), started))
            print(f"collected {config['name']} {keyword}: {len(urls)} new posts", flush=True)
        except Exception as exc:
            errors.append(str(exc))
            if started:
                with psycopg.connect(DB) as conn:
                    conn.execute("UPDATE collector_runs SET status='error',error_message=%s,finished_at=now() WHERE id=%s", (str(exc)[:500], started))
            print(f"{config['name']} collection failed for {keyword}: {exc}", flush=True)
    mark_source(config, "error" if errors else "success", total_new, "; ".join(errors))
    return comment_budget


def run_google_trends(market="US"):
    market = market.upper()
    config = {"slug": "google_trends", "name": "Google Trends", "kind": "search", "enabled": GOOGLE_TRENDS_ENABLED,
              "posts_dataset_id": SERP_ZONE, "configured": bool(SERP_ZONE), "market": market}
    if not GOOGLE_TRENDS_ENABLED:
        mark_source(config, "disabled")
        return
    mark_source(config, "running")
    saved = 0
    errors = []
    for keyword in GOOGLE_TRENDS_KEYWORDS:
        try:
            saved += save_google_trends(keyword, market)
            print(f"collected Google Trends {market}: {keyword}", flush=True)
        except Exception as exc:
            errors.append(str(exc))
            print(f"Google Trends failed for {keyword}: {exc}", flush=True)
    mark_source(config, "error" if errors else "success", saved, "; ".join(errors))


def run_market(market):
    market = market.upper()
    print(f"starting market {market}", flush=True)
    run_google_trends(market)
    comment_budget = [MAX_COMMENT_POSTS]
    for slug in SOURCE_ORDER:
        comment_budget = run_platform(source_config(slug, market), comment_budget)


def run():
    if not TOKEN:
        raise RuntimeError("BRIGHTDATA_API_TOKEN is missing")
    for market in COLLECTION_MARKETS:
        run_market(market)


def main():
    wait_for_db()
    ensure_worker_schema()
    while True:
        run()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
