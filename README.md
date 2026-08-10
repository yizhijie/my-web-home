# Trend Radar

US social-commerce opportunity radar for the United States market. It collects normalized discovery signals from Bright Data sources on a daily schedule, stores them in PostgreSQL, and presents a web dashboard with filters, trend snapshots, comment pain labels, source health, and category/platform benchmarks.

The score is a social-demand signal, not a sales or profit conclusion. Validate marketplace competition, landed cost, IP, safety, platform policy, and product claims before sourcing.

## Automatic deployment

Push to the private repository `main` branch and GitHub Actions deploys to the ECS through a self-hosted runner. The runner lives on the ECS and connects outbound to GitHub, so no new public SSH ingress is required.

The deployment archive excludes `/opt/trend-radar/.env`, `data/`, and `backups/`. Bright Data credentials and database passwords stay only in the server `.env` file.

In GitHub: **Settings → Actions → Runners → New self-hosted runner**, choose Linux / x64, and run the generated commands on the ECS. Keep the runner service running with `./svc.sh install root`, `./svc.sh start`, and `./svc.sh status`.

## Collection settings

`COLLECTION_INTERVAL_SECONDS=86400` runs once per day. `POSTS_PER_KEYWORD=3` controls discovery volume. `COLLECT_COMMENTS=true` enables TikTok comment collection, while `MAX_COMMENT_POSTS_PER_RUN=3` caps comment calls per daily run for the free quota. These settings belong in `/opt/trend-radar/.env` on the ECS only.

## Market switch

The dashboard's top-right market selector supports US plus Singapore, Malaysia, Thailand, Vietnam and the Philippines. Existing rows are migrated to `US`. The Worker reads `COLLECTION_MARKETS` and stores the market on opportunities, snapshots, source health and Google Trends rows. Google Trends uses the selected market's `geo` value, so set `COLLECTION_MARKETS=US,SG,MY,TH,VN,PH` on the server when you want separate regional trend series on the next run (this increases Bright Data usage).

TikTok's default discover-by-keyword Dataset accepts `search_keyword` and `num_of_posts`, but does not expose a country input. The UI therefore keeps the Southeast Asia TikTok entries visible while marking them `待接入` until a localized TikTok Dataset or per-market URL inputs are configured. Do not label global keyword results as country-specific data. For a localized URL Dataset, set `BRIGHTDATA_TIKTOK_POSTS_MODE=scrape`, `BRIGHTDATA_TIKTOK_POSTS_INPUT_KEY=url`, and provide market-specific inputs such as `BRIGHTDATA_TIKTOK_POSTS_INPUTS_SG`.

## Multi-source Bright Data collection

TikTok is enabled by default because the starter deployment already has its keyword-discovery Dataset IDs. The Worker also has adapters for Instagram, YouTube, Facebook, X, Reddit, Amazon and Google Maps. Enable sources with `SOCIAL_PLATFORMS_ENABLED=tiktok,instagram,youtube` and fill the matching `BRIGHTDATA_<SOURCE>_POSTS_DATASET_ID` and (when available) `..._COMMENTS_DATASET_ID` values in the server `.env`. The dashboard reports each source as `正常`, `运行中`, `失败`, `待配置`, or `未启用`.

Bright Data Dataset input schemas are not interchangeable. For each Dataset, check the input fields shown in the Bright Data control panel and set `..._POSTS_INPUT_KEY`, `..._COMMENTS_INPUT_KEY`, and `..._PAYLOAD_MODE` (`wrapped` sends `{ "input": [...] }`; `array` sends a bare array). Discovery Datasets can use keyword inputs; scraper Datasets in `scrape` mode use the comma-separated `..._POSTS_INPUTS` URLs. The application never substitutes a sample Dataset ID from documentation and never creates fake rows when a source is not configured.

Google Trends is a separate optional search source. Set `GOOGLE_TRENDS_ENABLED=true` and use the SERP zone that exists in your Bright Data account (`BRIGHTDATA_SERP_ZONE`). A switch only enables requests; it does not grant zone access. If Bright Data returns an error, the source card and Worker logs show the error and the Google Trends panel remains empty.

The normalized opportunity table is intentionally a signal layer: social attention and review pain points do not prove sales, profit, market size, or product safety. Validate marketplace demand, landed cost, IP, safety, claims, and platform rules before sourcing.

## Product validation workbench

Open an opportunity detail and use **产品验证工作台** to save target price, product cost, inbound shipping, fulfillment, platform fee, advertising rate, return rate, other unit costs, competitor count/average price, compliance risk, seasonality, and notes. The API stores these inputs per opportunity and returns unit profit, margin, total cost, and break-even price. The recommendation is a guardrail:

- `推荐测试`: signal score is strong and modeled margin is at least 25% with no high compliance risk.
- `继续观察`: evidence or economics is incomplete but not immediately disqualifying.
- `暂缓`: modeled margin is below 15%, signal is weak, or compliance risk is high.

This is a pre-test unit economics model, not a tax, accounting, investment, or sales forecast.
