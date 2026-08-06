import os
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

DATABASE_URL = os.environ["DATABASE_URL"]
app = FastAPI(title="US Trend Radar")

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
          gap TEXT NOT NULL, risk TEXT NOT NULL, source_url TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""")
        count = conn.execute("SELECT count(*) FROM opportunities").fetchone()[0]
        if count == 0:
            conn.execute("""INSERT INTO opportunities
            (product,category,platform,score,momentum,cross_platform,demand,gap,risk,source_url) VALUES
            ('Portable pet cooling mat','Pets','TikTok',72,82,60,74,'Improve non-slip backing and washable cover','Verify materials and heat claims',''),
            ('Under-desk walking pad accessories','Fitness','YouTube',68,69,64,73,'Quiet, compact storage and desk-fit bundles','Check electrical and warranty requirements',''),
            ('Modular pantry organizer','Home','Instagram',65,61,58,71,'Solve deep-cabinet access and label compatibility','Low; validate dimension demand','')""")

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

@app.get("/api/opportunities")
def list_opportunities():
    with db() as conn:
        rows = conn.execute("SELECT id,product,category,platform,score,momentum,cross_platform,demand,gap,risk,source_url,created_at FROM opportunities ORDER BY score DESC, created_at DESC").fetchall()
    keys = ["id","product","category","platform","score","momentum","cross_platform","demand","gap","risk","source_url","created_at"]
    return [dict(zip(keys, row)) for row in rows]

@app.get("/api/summary")
def summary():
    with db() as conn:
        total, avg, high = conn.execute("SELECT count(*), coalesce(round(avg(score)),0), count(*) FILTER (WHERE score>=70) FROM opportunities").fetchone()
        categories = conn.execute("SELECT category, count(*) FROM opportunities GROUP BY category ORDER BY count(*) DESC").fetchall()
    return {"total": total, "average_score": avg, "high_priority": high, "categories": categories}

@app.post("/api/opportunities", status_code=201)
def add_opportunity(item: Opportunity):
    score = round(item.momentum*.35 + item.cross_platform*.25 + item.demand*.25 + 15)
    with db() as conn:
        row = conn.execute("""INSERT INTO opportunities(product,category,platform,score,momentum,cross_platform,demand,gap,risk,source_url)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""", (item.product,item.category,item.platform,score,item.momentum,item.cross_platform,item.demand,item.gap,item.risk,item.source_url)).fetchone()
    return {"id": row[0], "score": score}

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc)}

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """<!doctype html><html><head><meta charset='utf-8'><title>US Trend Radar</title><style>
body{font:15px system-ui;background:#f5f7fb;color:#182033;margin:0;padding:38px;max-width:1500px;margin:auto}h1{margin:0;font-size:30px}p{color:#64748b}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-top:22px}.metric,.card{background:white;border-radius:16px;padding:22px;box-shadow:0 2px 12px #dce1ee}.metric b{display:block;font-size:30px;margin-top:8px}.label{color:#64748b;font-size:13px}.card{margin-top:18px}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:14px 12px;border-bottom:1px solid #e8ebf2;vertical-align:top}th{color:#64748b}.score{font-weight:800;color:#2563eb}.tag{background:#e8f0ff;color:#2563eb;border-radius:99px;padding:4px 9px;font-size:12px}a{color:#2563eb}@media(max-width:800px){.grid{grid-template-columns:1fr}body{padding:18px}}</style></head><body>
<h1>US Trend Radar <span class='tag'>美国社媒选品雷达</span></h1><p>每日一次采集 · TikTok 真实信号 · 家居 / 宠物 / 健身</p><section class='grid'><div class='metric'><span class='label'>已发现机会</span><b id='total'>–</b></div><div class='metric'><span class='label'>平均机会评分</span><b id='avg'>–</b></div><div class='metric'><span class='label'>优先验证（≥70）</span><b id='high'>–</b></div></section><div class='card'><h2>机会榜</h2><p class='label'>评分是筛选信号，不等于销量结论；请结合竞品、利润与合规复核。</p><table><thead><tr><th>社媒信号 / 产品机会</th><th>类目</th><th>平台</th><th>评分</th><th>下一步验证</th><th>来源</th></tr></thead><tbody id='rows'></tbody></table></div>
<script>Promise.all([fetch('/api/opportunities').then(r=>r.json()),fetch('/api/summary').then(r=>r.json())]).then(([items,s])=>{total.textContent=s.total;avg.textContent=s.average_score;high.textContent=s.high_priority;rows.innerHTML=items.map(x=>`<tr><td>${x.product}</td><td><span class='tag'>${x.category}</span></td><td>${x.platform}</td><td class='score'>${x.score}</td><td>${x.gap}</td><td>${x.source_url?`<a target='_blank' href='${x.source_url}'>查看原帖</a>`:'示例信号'}</td></tr>`).join('')})</script></body></html>"""
