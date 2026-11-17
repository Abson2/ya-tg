# 更新日志

本文件记录 Ya-TG 的每个正式版本。版本标签遵循 `vX.Y.Z`，每个版本都会同步发布到：
- GitHub Releases：`https://github.com/Abson2/ya-tg/releases`
- DockerHub：`abson2/ya-tg:vX.Y.Z`

---

## v0.5.3 (2026-11-17)

修复扫描报 `code=120 缺少clientId`：短信登录用户改用光鸭网页端文件接口，真正实现零配置扫描。

### 问题
- 扫描走官方 OpenAPI `/openapi/v1/file/get_file_list`，官方要求 `x-client-id` 为**开放平台分配的应用 ID**（与 project_id/sign_secret 配套，错误码 120 = 应用 ID/参数错误）；普通用户短信登录拿的是网页端凭证，不含该应用 ID，导致扫描失败
- 官方 curl 示例：`--header 'x-client-id: {client_id}' --header 'Authorization: Bearer {access_token}'`，其中 client_id 由开放平台分配，不是网页公开标识

### 修复
- 逆向光鸭网页前端确认网页端文件列表接口：`POST https://api.guangyapan.com/userres/v1/file/get_file_list`，body `{page,pageSize,parentId,...}`（根目录 `parentId="*"`），认证为 `Bearer + did + dt=4`，与短信登录/分享完全同一套，无需 client_id
- 新增 `GuangYaWebFiles` 客户端：复用短信登录的 token + did + dt 调网页端接口；登录态失效（401/116/117/登录失效文案）直接提示重新登录
- `scanner.run_scan()` 按登录通道分流：`flow in (sms,tgto)` 走网页端接口，官方 Device/PKCE 授权仍走官方 OpenAPI；两种接口返回字段一致，扫描解析逻辑零改动
- 网页端接口返回字段已核实：`fileId/fileName/fileSize/ext/ctime/utime/resType(=2 为目录)/parentId/thumbnail`，与原 scanner 完全兼容

### 测试
- 新增 `tests/test_web_files.py`：验证网页端请求带 Bearer+did+dt、**不含 x-client-id**、根目录映射 `*`、非短信登录被拒；全套 18 项通过

### 镜像
```bash
docker pull abson2/ya-tg:v0.5.3
```

## v0.5.2 (2026-11-17)

修复"短信登录后页面一刷新就显示未授权/凭证掉了"的问题，并把凭证有效期与自动刷新状态做成可见信息。功能链路不变，凭证本身一直安全存在库中。

### 修复
- 根因：SQLite 的 `DateTime(timezone=True)` 读回会丢 tzinfo（naive UTC），而 `/api/auth/status` 用 aware 的 `utcnow()` 与之比较，抛 `TypeError: can't compare offset-naive and offset-aware datetimes` 导致 500；前端把三个状态请求放在同一 `try`，一处失败就连 `sms/status` 一起跳过，页面显示"未授权、用户 ID —、过期时间 —"，看起来像凭证掉了
- 新增 `utils.as_utc()` 统一归一化所有从库读出的时间，修复 `guangya_auth.status`、`poll_device`、`refresh`、`_job_token_refresh` 等全部 naive/aware 比较点，定时刷新任务不再有崩溃隐患
- 前端 `loadAuth()` 拆成三个独立请求，任一端点失败不再拖垮其余状态
- 页面刷新后按 `/sms/status` 的真实登录态自动恢复向导到"已连接"，不再回到第 1 步的假象

### 新增（凭证可视化）
- `/api/sms/status` 增加 `expires_in_seconds`（剩余秒数）、`auto_refresh`（是否可自动续期）、`needs_refresh`（是否临近到期）字段；`expires_at` 改为带 `Z` 的 UTC 时间，前端显示无歧义
- 管理页"光鸭授权"新增凭证状态栏：**到期倒计时**（每秒刷新）、**自动刷新开关状态**、**北京时间过期时间**，以及"置顶已连接"状态标签；临近到期会提示正在自动刷新

### 配置
- `docker-compose.yml` / `docker-compose.build.yml` 及文档示例的 `env_file` 由 `[.env]` 改为块列表 `- ./.env`，消除不同 compose 版本的解析歧义

### 测试
- 新增 `tests/test_status_consistency.py`：锁定"短信登录后所有状态端点均 200 且数据一致"、倒计时字段、naive 时间归一化；全套 16 项通过

### 镜像
```bash
docker pull abson2/ya-tg:v0.5.2
```

## v0.5.1 (2026-11-17)

**纯文档修正版本**，功能与镜像行为同 v0.5.0。修正上一版文档中仍残留、会误导普通用户填写光鸭凭证的内容，与自研短信登录的实际使用方式对齐。

### 修正
- `.env.example` 精简：普通用户只需修改 `ADMIN_PASSWORD`、`APP_SECRET` 两项；所有 `GUANGYA_*` 高级凭证移到文件末尾并全部注释，明确"光鸭凭证由管理页手机号+短信验证码一键登录自动获取、加密保存、到期自动刷新，无需申请应用或抓包"
- `README.md` 同步：`.env` 示例删除光鸭凭证空项；配置表改为"光鸭凭证不用在 .env 填"；分享失败 FAQ 从"重新抓包更新 .env"改为"管理页重新短信登录"
- 版本号统一更新到 0.5.1

### 镜像
```bash
docker pull abson2/ya-tg:v0.5.1
```

## v0.5.0 (2026-11-17)

把短信登录**改为 Ya-TG 自研实现**，彻底移除对外部项目 TgtoDrive 的运行时依赖；同时按用户习惯统一数据库目录为 `db`。两个项目从此完全独立、互不干扰。

### 变更
- 短信登录不再经 TgtoDrive 网关：内置光鸭网页端账号服务接口（captcha/init → verification → verify → signin），借鉴开源客户端契约自行实现，零外部依赖
- 移除 `tgto_bridge.py`、`TGTO_BASE_URL` / `TGTO_API_TOKEN` 配置、`extra_hosts` 与全部 `/api/tgto/*` 端点
- 新增 `/api/sms/status`、`/api/sms/send`、`/api/sms/verify`、`/api/sms/refresh`
- 短信登录的 Token、Refresh Token、设备 ID（did）加密落库；到期/401 经 `/v1/auth/token` 自动刷新
- 建分享自动复用短信登录的 Token + did + dt，无需单独抓包会话凭证
- 数据库目录由 `./data` 统一为 `./db`：`DATABASE_URL=sqlite:////app/db/ya-tg.db`、卷 `./db:/app/db`，README/.env/.gitignore 同步

### 新增
- `AuthToken.device_id` 字段与轻量迁移
- `guangya_sms.py` 自研短信登录模块与 mock 单测

### 升级注意
- 从 v0.4.x 升级：若此前数据在 `./data/ya-tg.db`，请先复制到 `./db/ya-tg.db` 再启动；全新安装无此问题
- 已短信登录用户需重新登录一次（登录方式变更，flow 从 tgto 改为 sms）

### 镜像
```bash
docker pull abson2/ya-tg:v0.5.0
```

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
