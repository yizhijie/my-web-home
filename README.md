# Trend Radar

US social-commerce opportunity radar. Deploy with Docker Compose.
## 自动部署（GitHub Actions）

项目推送到 GitHub 私有仓库的 `main` 分支后，会自动发布到 ECS；服务器上的 `.env`、`data/`、`backups/` 不会被上传或覆盖。

首次配置是在 ECS 上安装该私有仓库的 **self-hosted runner**。它由 ECS 主动连接 GitHub，因此不需要对 GitHub 放开 SSH 入站端口，也不需要配置 ECS SSH 私钥或地址等 Actions secrets。

在 GitHub 私有仓库打开 **Settings → Actions → Runners → New self-hosted runner**，选择 Linux / x64；页面会提供一组带一次性令牌的安装命令。请在 ECS 的 root 终端中逐条执行，然后执行页面最后的 `./svc.sh install` 与 `./svc.sh start`，使 Runner 长期在线。

`BRIGHTDATA_API_TOKEN`、数据库密码等只保存在 ECS 的 `/opt/trend-radar/.env`，绝不能提交到 GitHub 或填写进 Actions secrets。首次自动部署前，确认该 `.env` 文件仍存在。

部署工作流文件：`.github/workflows/deploy.yml`。每次推送后，在仓库的 **Actions → Deploy Trend Radar** 查看部署结果。
