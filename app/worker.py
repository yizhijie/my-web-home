import json, os, time, urllib.request
import psycopg

DB = os.environ["DATABASE_URL"]
TOKEN = os.environ.get("BRIGHTDATA_API_TOKEN", "")
INTERVAL = int(os.environ.get("COLLECTION_INTERVAL_SECONDS", "86400"))
LIMIT = int(os.environ.get("POSTS_PER_KEYWORD", "3"))
KEYWORDS = [("pet cooling mat", "Pets"), ("under desk walking pad", "Fitness"), ("pantry organizer", "Home")]
URL = "https://api.brightdata.com/datasets/v3/scrape?dataset_id=gd_lu702nij2f790tmv9h&type=discover_new&discover_by=keyword&include_errors=true"

def wait_for_db():
    for _ in range(30):
        try:
            with psycopg.connect(DB): return
        except Exception: time.sleep(2)
    raise RuntimeError("database unavailable")

def collect(keyword, category):
    data = json.dumps({"input":[{"search_keyword":keyword,"num_of_posts":LIMIT}]}).encode()
    req = urllib.request.Request(URL, data=data, headers={"Authorization":f"Bearer {TOKEN}","Content-Type":"application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=180) as r:
        raw = r.read().decode().strip()
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        return [json.loads(line) for line in raw.splitlines() if line.strip()]

def save(items, category):
    with psycopg.connect(DB) as conn:
        for x in items:
            plays = int(x.get("play_count") or 0); comments = int(x.get("comment_count") or 0); shares = int(x.get("share_count") or 0)
            momentum = min(100, round((plays/1000000)*55 + (shares/1000)*25 + (comments/500)*20))
            demand = min(100, round((comments/300)*70 + 20))
            score = min(100, round(momentum*.45 + demand*.30 + 25))
            url = x.get("url") or x.get("profile_url") or ""
            product = (x.get("description") or x.get("title") or "Emerging product signal")[:160]
            conn.execute("""INSERT INTO opportunities(product,category,platform,score,momentum,cross_platform,demand,gap,risk,source_url)
            SELECT %s,%s,'TikTok',%s,%s,25,%s,'Review comments and competitor gaps before sourcing','Validate IP, safety and product claims',%s
            WHERE NOT EXISTS (SELECT 1 FROM opportunities WHERE source_url=%s)""", (product,category,score,momentum,demand,url,url))

def run():
    if not TOKEN: raise RuntimeError("BRIGHTDATA_API_TOKEN is missing")
    for keyword, category in KEYWORDS:
        try: save(collect(keyword, category), category)
        except Exception as e: print(f"collection failed for {keyword}: {e}", flush=True)

wait_for_db()
while True:
    run()
    time.sleep(INTERVAL)
