# 更新日志

本文件记录 Ya-TG 的每个正式版本。版本标签遵循 `vX.Y.Z`，每个版本都会同步发布到：
- GitHub Releases：`https://github.com/Abson2/ya-tg/releases`
- DockerHub：`abson2/ya-tg:vX.Y.Z`

---

## v0.4.0 (2026-11-17)

新增**光鸭一键连接**：普通用户不用再申请/填写开放平台应用凭证或抓包会话凭证，在管理页用手机号+短信验证码即可自动完成全部光鸭配置。

### 新增
- 「一键连接光鸭」向导：管理页填手机号 → 收短信验证码 → 登录，自动获取并加密保存光鸭凭证
- 接入 TgtoDrive 作为光鸭认证网关（`TGTO_BASE_URL` / `TGTO_API_TOKEN`），复用其成熟的短信登录与 Token 刷新能力
- 运行时凭证配置层（`app_config` 表）：页面注入的凭证加密落库，优先级高于 `.env`，业务模块自动读取
- 光鸭凭证到期由定时任务经网关自动刷新，失败记入审计；`flow=tgto` 与官方 Device/PKCE 通道自动分流
- 新增 API：`/api/tgto/status`、`/api/tgto/connect`、`/api/tgto/sms/send`、`/api/tgto/sms/verify`、`/api/tgto/refresh`
- 原 Device Code / PKCE 直连方式保留为「高级方式」，供已有开放平台应用的进阶用户使用

### 变更
- `auth/status`、`share/status` 增加凭证来源标识（管理页一键登录 / `.env` 手工配置）
- 网页分享会话、扫描接口的 client_id 均改为从运行时配置读取，页面改配置即时生效
- 新增依赖无：复用现有 httpx / cryptography

### 说明
- 一键连接需在 NAS 上已部署 TgtoDrive（默认 `http://127.0.0.1:12366`）并在其「开放接口」页生成项目令牌，填入管理页一次即可
- 凭证全程加密落库、不写日志、不写仓库；管理页仅展示配置来源与掩码

### 镜像
```bash
docker pull abson2/ya-tg:v0.4.0
```

## v0.3.0 (2026-11-16)

品牌由 GuangYa TG Publisher 统一为 **Ya-TG**，管理界面重做，发布链路补齐。

### 新增
- 全新管理界面：深色侧栏、渐变概览卡、系统健康面板、队列分布、操作时间线
- 资源库支持海报网格与表格双视图，可按状态筛选与关键词搜索
- 光鸭授权页：Device Code 与 Web OAuth PKCE 双方案步骤引导，授权码一键复制
- 消息模板：变量一键插入、条件块、默认模板标记
- GitHub Actions 自动构建镜像并创建 GitHub Release，DockerHub 仓库自动设为 Public
- `docker-compose.yml` 直接拉取 DockerHub 镜像部署，普通用户无需源码或构建（另提供 `docker-compose.build.yml` 供源码构建）
- 一键发布脚本 `scripts/release.sh` 与更新脚本 `scripts/update.sh`

### 变更
- 容器名与镜像名改为 `ya-tg`，数据库改为 `ya-tg.db`
- FastAPI 标题与 `/api/health` 版本信息更新为 Ya-TG 0.3.0
- 保留 `guangya_*` 模块名与 `GUANGYA_*` 环境变量（光鸭官方命名）

### 修复
- 扫描后队列 worker 未自动建分享导致推送必然失败的问题
- 退避 jitter 突破上限、新库首次启动 ScanTask 未重载导致的崩溃

### 镜像
```bash
docker pull abson2/ya-tg:v0.3.0
```

---

## v0.2.0 (2026-11-16)

代码底座升级为可联调版本，打通完整业务链路。

### 新增
- 光鸭 Device Code 授权（后端 sign_secret 签名申请设备码）与 Web OAuth 2.0 + PKCE
- refresh_token 自动刷新、定时刷新、Token 无效（117）自动刷新重试一次
- Access/Refresh Token 与 Bot Token 使用 Fernet 加密落库，管理端仅显示掩码
- 队列 worker 推送前自动创建光鸭分享并保存 share_url，幂等防重复
- TMDB 文件名解析（年份/分辨率/SxxEexx）、电影电视剧决策、自动与手动识别
- Telegram 有海报走 sendPhoto、无海报回退 sendMessage，支持自定义消息模板
- 指数退避 + next_retry_at、429 Retry-After、人工审核队列、全量操作审计
- 扫描断点游标（已访问目录/待扫栈/页码）、失败续扫、utime/size 更新检测

### 镜像
该版本未发布到 DockerHub，请从源码构建。

---

## v0.1.0 (2026-11-16)

初始工程基线（GuangYa TG Publisher）。

### 包含
- FastAPI 后端 + Vue 3 中文管理界面 + SQLite + Docker Compose
- 北京时间 Cron 调度、光鸭文件列表递归扫描、fileId 去重
- 分享创建/修改/取消，1天/7天/30天/永久期限
- 多 Bot、多频道、分发规则、人工审核队列、TMDB 搜索封装、Telegram 文本推送

### 状态
可运行开发基线，非生产验收版本。
