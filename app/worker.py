import json
import os
import re
import time
import urllib.request

import psycopg

DB = os.environ["DATABASE_URL"]
TOKEN = os.environ.get("BRIGHTDATA_API_TOKEN", "")
INTERVAL = int(os.environ.get("COLLECTION_INTERVAL_SECONDS", "86400"))
LIMIT = int(os.environ.get("POSTS_PER_KEYWORD", "3"))
COLLECT_COMMENTS = os.environ.get("COLLECT_COMMENTS", "true").lower() in {"1", "true", "yes"}
MAX_COMMENT_POSTS = int(os.environ.get("MAX_COMMENT_POSTS_PER_RUN", "3"))
KEYWORDS = [("pet cooling mat", "Pets"), ("under desk walking pad", "Fitness"), ("pantry organizer", "Home")]
POSTS_URL = "https://api.brightdata.com/datasets/v3/scrape?dataset_id=gd_lu702nij2f790tmv9h&type=discover_new&discover_by=keyword&include_errors=true"
COMMENTS_URL = "https://api.brightdata.com/datasets/v3/scrape?dataset_id=gd_lkf2st302ap89utw5k&include_errors=true"

PAIN_RULES = [
    ("durability", r"\b(broke|breaks|broken|tear|ripped|durab|doesn't last|didn't last)\b"),
    ("fit / size", r"\b(size|sized|fit|fits|small|large|too big|too small|desk fit)\b"),
    ("cleaning", r"\b(clean|wash|washed|dirty|stain|odor|smell)\b"),
    ("comfort / noise", r"\b(noise|loud|quiet|comfort|comfortable|painful|hot)\b"),
    ("setup / usability", r"\b(hard|difficult|confus|setup|assemble|assembly|instructions|use)\b"),
    ("value / price", r"\b(price|expensive|cheap|worth|money|cost)\b"),
]


def wait_for_db():
    for _ in range(30):
        try:
            with psycopg.connect(DB):
                return
        except Exception:
            time.sleep(2)
    raise RuntimeError("database unavailable")


def request_json(url, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=180) as response:
        raw = response.read().decode().strip()
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        return [json.loads(line) for line in raw.splitlines() if line.strip()]


def collect(keyword):
    return request_json(POSTS_URL, {"input": [{"search_keyword": keyword, "num_of_posts": LIMIT}]})


def classify_pain(text):
    for label, pattern in PAIN_RULES:
        if re.search(pattern, text or "", re.IGNORECASE):
            return label
    return "other"


def save_posts(items, category):
    urls = []
    with psycopg.connect(DB) as conn:
        for x in items:
            plays = int(x.get("play_count") or 0)
            comments = int(x.get("comment_count") or 0)
            shares = int(x.get("share_count") or 0)
            momentum = min(100, round((plays / 1000000) * 55 + (shares / 1000) * 25 + (comments / 500) * 20))
            demand = min(100, round((comments / 300) * 70 + 20))
            score = min(100, round(momentum * .45 + demand * .30 + 25))
            url = x.get("url") or ""
            product = (x.get("description") or x.get("title") or "Emerging product signal")[:160]
            row = conn.execute("""INSERT INTO opportunities
              (product,category,platform,score,momentum,cross_platform,demand,gap,risk,source_url,comment_count)
              SELECT %s,%s,'TikTok',%s,%s,25,%s,'Review comments and competitor gaps before sourcing',
                'Validate IP, safety and product claims',%s,%s
              WHERE NOT EXISTS (SELECT 1 FROM opportunities WHERE source_url=%s)
              RETURNING id, source_url""", (product, category, score, momentum, demand, url, comments, url)).fetchone()
            if row and "/video/" in (row[1] or ""):
                urls.append((row[0], row[1]))
    return urls


def collect_comments(opportunity_id, post_url):
    if not COLLECT_COMMENTS or not post_url:
        return
    try:
        items = request_json(COMMENTS_URL, {"input": [{"url": post_url}]})
        with psycopg.connect(DB) as conn:
            for x in items:
                text = (x.get("comment_text") or "").strip()
                comment_url = x.get("comment_url") or ""
                if not text or not comment_url:
                    continue
                conn.execute("""INSERT INTO comments(opportunity_id,platform,comment_text,likes,replies,pain_label,comment_url)
                  VALUES(%s,'TikTok',%s,%s,%s,%s,%s) ON CONFLICT (comment_url) DO NOTHING""",
                  (opportunity_id, text[:1000], int(x.get("num_likes") or 0), int(x.get("num_replies") or 0), classify_pain(text), comment_url))
            labels = conn.execute("""SELECT pain_label,count(*) FROM comments WHERE opportunity_id=%s
              GROUP BY pain_label ORDER BY count(*) DESC LIMIT 3""", (opportunity_id,)).fetchall()
            pain = "; ".join(f"{label} ({count})" for label, count in labels if label != "other")
            total = conn.execute("SELECT count(*) FROM comments WHERE opportunity_id=%s", (opportunity_id,)).fetchone()[0]
            conn.execute("""UPDATE opportunities SET comment_count=%s,pain_points=%s,
              comments_checked_at=now() WHERE id=%s""", (total, pain, opportunity_id))
    except Exception as exc:
        print(f"comment collection failed for {post_url}: {exc}", flush=True)


def pending_comments(category, limit):
    with psycopg.connect(DB) as conn:
        return conn.execute("""SELECT id,source_url FROM opportunities
          WHERE category=%s AND source_url LIKE '%%/video/%%' AND comments_checked_at IS NULL
          ORDER BY score DESC LIMIT %s""", (category, limit)).fetchall()


def snapshot(category):
    with psycopg.connect(DB) as conn:
        conn.execute("""INSERT INTO trend_snapshots(snapshot_date,category,platform,opportunity_count,avg_score,avg_momentum,avg_demand,comment_count)
          SELECT CURRENT_DATE,category,platform,count(*),round(avg(score)),round(avg(momentum)),round(avg(demand)),coalesce(sum(comment_count),0)
          FROM opportunities WHERE category=%s GROUP BY category,platform
          ON CONFLICT (snapshot_date,category,platform) DO UPDATE SET opportunity_count=EXCLUDED.opportunity_count,
            avg_score=EXCLUDED.avg_score,avg_momentum=EXCLUDED.avg_momentum,avg_demand=EXCLUDED.avg_demand,
            comment_count=EXCLUDED.comment_count""", (category,))


def run():
    if not TOKEN:
        raise RuntimeError("BRIGHTDATA_API_TOKEN is missing")
    comment_budget = MAX_COMMENT_POSTS
    for keyword, category in KEYWORDS:
        try:
            urls = save_posts(collect(keyword), category)
            existing = pending_comments(category, max(comment_budget - len(urls), 0))
            candidates = urls + [x for x in existing if x not in urls]
            for opportunity_id, post_url in candidates[:comment_budget]:
                collect_comments(opportunity_id, post_url)
                comment_budget -= 1
            snapshot(category)
            print(f"collected {keyword}: {len(urls)} new posts", flush=True)
        except Exception as exc:
            print(f"collection failed for {keyword}: {exc}", flush=True)


wait_for_db()
while True:
    run()
    time.sleep(INTERVAL)
