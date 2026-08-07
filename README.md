# Trend Radar

US social-commerce opportunity radar for the United States market. It collects TikTok discovery signals on a daily schedule, stores them in PostgreSQL, and presents a web dashboard with filters, trend snapshots, comment pain labels, and category/platform benchmarks.

The score is a social-demand signal, not a sales or profit conclusion. Validate marketplace competition, landed cost, IP, safety, platform policy, and product claims before sourcing.

## Automatic deployment

Push to the private repository `main` branch and GitHub Actions deploys to the ECS through a self-hosted runner. The runner lives on the ECS and connects outbound to GitHub, so no new public SSH ingress is required.

The deployment archive excludes `/opt/trend-radar/.env`, `data/`, and `backups/`. Bright Data credentials and database passwords stay only in the server `.env` file.

In GitHub: **Settings → Actions → Runners → New self-hosted runner**, choose Linux / x64, and run the generated commands on the ECS. Keep the runner service running with `./svc.sh install root`, `./svc.sh start`, and `./svc.sh status`.

## Collection settings

`COLLECTION_INTERVAL_SECONDS=86400` runs once per day. `POSTS_PER_KEYWORD=3` controls discovery volume. `COLLECT_COMMENTS=true` enables TikTok comment collection, while `MAX_COMMENT_POSTS_PER_RUN=3` caps comment calls per daily run for the free quota. These settings belong in `/opt/trend-radar/.env` on the ECS only.
