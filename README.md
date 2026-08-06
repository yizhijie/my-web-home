# Trend Radar

US social-commerce opportunity radar. Deploy with Docker Compose.
## 自动部署（GitHub Actions）

项目推送到 GitHub 私有仓库的 `main` 分支后，会自动发布到 ECS；服务器上的 `.env`、`data/`、`backups/` 不会被上传或覆盖。

首次配置请在 GitHub 仓库的 **Settings → Secrets and variables → Actions** 新增以下 Actions secrets：

| 名称 | 填写内容 |
| --- | --- |
| `ECS_HOST` | ECS 公网 IP，例如 `47.96.107.82` |
| `ECS_USER` | `root` |
| `ECS_PORT` | `22` |
| `ECS_SSH_KEY` | 专用于自动部署的 SSH 私钥全文 |

`BRIGHTDATA_API_TOKEN`、数据库密码等只保存在 ECS 的 `/opt/trend-radar/.env`，绝不能提交到 GitHub 或填写进 Actions secrets。

部署工作流文件：`.github/workflows/deploy.yml`。每次推送后，在仓库的 **Actions → Deploy Trend Radar** 查看部署结果。
