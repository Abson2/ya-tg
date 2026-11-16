# Ya-TG 0.3.0

光鸭网盘资源分发工作台。链路：光鸭授权 → 定时扫描 → fileId 去重 → TMDB 识别 → 自动建分享 → 规则分发 → Telegram 多 Bot/多频道推送。独立 Docker 部署，不依赖 TgtoDrive，不监控 TG 来源频道。

> 0.2.0 及以前的项目名为 GuangYa TG Publisher。0.3.0 起品牌统一为 **Ya-TG**，Docker 镜像、容器名与管理界面均已改名；`guangya_*` 代码模块与 `GUANGYA_*` 环境变量保留，因为它们是光鸭开放平台的官方命名，改动反而增加迁移成本。

## 管理界面（0.3.0 重做）

- 深色侧栏 + 浅色工作区，靛蓝/青绿主色，卡片式布局与响应式收窄
- 概览：渐变 Hero、六项统计卡、系统健康清单、队列分布条、最近操作时间线
- 光鸭授权：Device / PKCE 双方案步骤卡、授权码大字展示与一键复制、会话状态面板
- 资源库：海报网格 / 表格双视图，搜索与分享状态筛选，分享角标、TMDB 标记、更新次数
- 分发中心：Bot → 频道 → 规则三步卡片，状态色标签，审核与队列独立页面
- 消息模板：变量一键插入、默认标记、文本预览
- 操作审计：全量时间线
- 所有交互均调用既有 `/api/*`，未改动后端接口契约

## 功能实现

| 模块 | 实现 |
| --- | --- |
| 光鸭授权 | Device Code（后端 sign_secret 签名）、Web OAuth 2.0 + PKCE、refresh_token 自动刷新、定时刷新、117 无效 Token 刷新重试一次 |
| 凭证安全 | Access/Refresh Token 与 Bot Token 使用 Fernet 加密落库；管理端只显示掩码；sign_secret 仅后端环境变量 |
| 分享闭环 | 队列 worker 推送前自动建分享并保存 share_url；幂等；分享/取消/修改全链路审计 |
| TMDB | 文件名解析（分辨率/年份/SxxEexx）、电影/电视剧决策、详情补全、自动与手动识别 |
| 消息 | 可配置模板（`{{变量}}` + `{{#if 变量}}` 条件块）、有海报用 sendPhoto，无海报回退 sendMessage |
| 队列 | 指数退避 + `next_retry_at`、Telegram 429 Retry-After、最大尝试次数、手动重试；resource+channel 唯一约束防重复推送 |
| 扫描 | 断点游标（已访问目录/待扫栈/页码持久化）、失败续扫、utime/size 变化计入 revision 并记审计 |

## 重要边界

1. **官方 OpenAPI 不提供创建分享接口。** `share_file` / `update_share` / `delete_share` 来自网页端抓包，不在公开清单内。本项目把两套认证彻底分开：扫描走官方 OpenAPI（`x-client-id` + Bearer），建分享走网页会话凭证。网页升级或认证策略变化可能导致失效。
2. **Device Code 有出口 IP 白名单。** 申请设备码由后端用 `sign_secret` 签名，平台按出口 IP 校验；未报备返回 `code=111`。轮询 Token、刷新、业务接口不校验该名单。
3. **`APP_SECRET` 是加密主密钥。** 更换后已加密的 Token 与 Bot Token 无法解密，需重新授权 / 重新录入 Bot。请一次设置、长期保留并备份。
4. 未做真实账号端到端验收。请先按下方清单用小样本验证，再全盘扫描。

## 部署方式

### A. 从 DockerHub 拉取（推荐）

在 NAS 上准备目录 `/vol1/1000/docker/ya-tg/`，放入 `docker-compose.hub.yml`、`.env`、`data/`、`logs/`：

```bash
cp .env.example .env && chmod 600 .env
# 编辑 .env：ADMIN_PASSWORD、APP_SECRET、DOCKERHUB_USERNAME 及各凭证
docker compose -f docker-compose.hub.yml pull
docker compose -f docker-compose.hub.yml up -d
```

访问 `http://NAS地址:8787`。

### B. 本地源码构建

```bash
cp .env.example .env && chmod 600 .env
docker compose up -d --build
```

### 配置要点

- 至少设置 `ADMIN_PASSWORD`、`APP_SECRET`（32 位以上随机串）。
- 光鸭授权二选一：Device（`GUANGYA_CLIENT_ID` / `GUANGYA_PROJECT_ID` / `GUANGYA_SIGN_SECRET` + 出口 IP 报备）或 Web PKCE（`GUANGYA_CLIENT_ID` / `GUANGYA_OAUTH_REDIRECT_URI`）。旧 Token 可留在 `GUANGYA_ACCESS_TOKEN` / `GUANGYA_REFRESH_TOKEN` 启动时一次性导入。
- 分享功能另需 `GUANGYA_SHARE_AUTHORIZATION` / `GUANGYA_SHARE_DID` / `GUANGYA_SHARE_DT` / `GUANGYA_SHARE_SMID`（在 NAS 本机填写，不要提交 Git）。
- TMDB 二选一：`TMDB_BEARER_TOKEN`（v4，优先）或 `TMDB_API_KEY`（v3）。

## 验收清单（小样本）

1. `/api/health` 返回 `version=0.3.0`、`app=Ya-TG`。
2. 管理后台「光鸭授权」完成授权 → 「验证连接」返回用户 ID 与 VIP 状态。
3. 扫描根目录设为测试目录 → 立即扫描 → 资源库出现文件且 fileId 不重复。
4. 资源库创建永久分享 → `share_status=active` 且保存 share_url。
5. 添加测试 Bot / 频道 / 自动发布规则 → 推送队列出现 `sent`，Telegram 收到消息。
6. 新建不勾选自动发布的规则 → 队列项 `review` → 审核中心通过 → 自动建分享并推送。
7. 扫描中途停容器 → 重启后扫描提示 `resumed=true`。
8. 通过后再做全盘扫描与多 Bot 分发。

## GitHub + DockerHub 发布与后续更新

本仓库带 GitHub Actions：push 到 `main` 或打 `v*` tag 时，云端自动 `docker build` 并推送镜像到 DockerHub。

首次发布步骤：

1. 在 GitHub 创建空仓库（建议名 `ya-tg`），在 DockerHub 创建仓库 `ya-tg`。
2. 本机或 NAS 上执行 `bash scripts/release.sh`，按提示输入仓库 HTTPS 地址。脚本会初始化 git、提交并推送 `main` 与 `v0.3.0` tag。
   - 若本机未配置 git 身份，先执行 `git config --global user.name "你的名字"` 与 `git config --global user.email "你的邮箱"`。
   - 私有仓库需使用 Personal Access Token 作为 HTTPS 密码。
3. 在 GitHub 仓库 **Settings → Secrets and variables → Actions** 添加：
   - `DOCKERHUB_USERNAME`：DockerHub 用户名
   - `DOCKERHUB_TOKEN`：DockerHub Access Token（非登录密码）
4. 推送后进入 Actions 页确认 `Build & Push Docker image` 成功；DockerHub 仓库出现 `latest` 与 `v0.3.0` 标签。
5. 日常更新：修改代码 → `git commit` → `git push origin main` → Actions 自动出新 `latest` 镜像；重要版本打 tag 生成不可变版本镜像。

安全提醒：`.env` 与 `data/` 已被 `.gitignore` 排除，切勿把真实凭证提交到仓库。

## 关键接口与文档

官方 OpenAPI v1.3（2026-09-20）：`https://app.guangyapan.com/pan/docs/open-api`
- 设备码：`POST https://openapi.guangyapan.com/v1/auth/device/code`（后端签名）
- Token：`POST https://openapi-account.guangyapan.com/v1/auth/token`
- 授权页：`https://www.guangyapan.com/oauth/`
- 文件列表：`GET /openapi/v1/file/get_file_list`（5 次/秒/IP）
- 用户信息：`GET /openapi/v1/user/get_user_info`

错误码：`111` 出口 IP 未报备、`116` 签名/时间戳错误、`117` Token 无效（自动刷新重试）、`123` 应用未开通写能力、`430` 用户非会员。
