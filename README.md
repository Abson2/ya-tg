# Ya-TG

一个跑在 Docker 里的「光鸭网盘资源分发机器人」。它会定时扫描你的光鸭网盘文件，用 TMDB 识别出电影/电视剧信息，自动生成分享链接，再按你设定的规则推送到 Telegram 频道。

简单说：**把网盘里的片子，自动整理成带海报简介的 Telegram 帖子发出去。**

- 项目地址：https://github.com/Abson2/ya-tg
- 官方镜像：`abson2/ya-tg`（DockerHub，公开可拉取）
- 当前版本：v0.5.3
- 部署方式：一条 `docker compose` 命令，不需要源码、不需要构建、不需要懂 Python

---

## 一、它能做什么

| 步骤 | 说明 |
| --- | --- |
| 光鸭授权 | 管理页手机号+短信验证码一键登录（Ya-TG 内置实现，不依赖任何第三方项目），凭证加密保存、自动刷新；也支持官方 Device/PKCE |
| 定时扫描 | 按 Cron（默认每 30 分钟）递归扫描指定目录，用 fileId 去重，只处理新文件 |
| TMDB 识别 | 从文件名解析片名/年份/集数，搜索 TMDB 补全海报、简介，区分电影和电视剧 |
| 自动建分享 | 推送前自动创建光鸭分享链接（永久/1天/7天/30天可选），保存链接 |
| 规则分发 | 按「目录 ID」或「文件名关键词」把资源路由到不同 Telegram 频道 |
| 多 Bot 推送 | 支持多个 Bot、多个频道；有海报发图片+简介，没海报发纯文本 |
| 审核模式 | 规则可选「自动发布」或「人工审核」，审核通过后才推送 |
| 失败重试 | 推送失败按指数退避自动重试，避免被 Telegram 限流 |
| 管理界面 | 中文 Web 后台，统计、资源库、授权、模板、审计一应俱全 |

---

## 二、开始前要准备什么

请先把下面 4 项准备好，缺一样都会卡住：

1. **一台装了 Docker 的机器**
   - 飞牛 NAS、群晖、普通 Linux 服务器都可以。
   - 需要能执行 `docker` 和 `docker compose` 命令（新版本 Docker 用 `docker compose`，旧版用 `docker-compose`）。
   - 验证：执行 `docker --version`，能显示版本号即可。

2. **一个光鸭网盘账号（一键登录，推荐）**
   - **只需在 Ya-TG 管理页输入手机号、收短信验证码**，程序就会自动登录光鸭并保存全部凭证。登录逻辑内置在 Ya-TG 里，**不依赖 TgtoDrive 或任何外部项目**，两个项目完全独立。
   - 这样你**完全不用**去光鸭开放平台申请应用，也不用 `client_id` / `project_id` / `sign_secret`，更不用从浏览器抓包。凭证到期后程序自动刷新。
   - 备用方案：如果你已有光鸭开放平台应用，也可在管理页用官方 Device Code / PKCE 授权（见「高级」说明）。

3. **Telegram Bot 和 TMDB（可选但推荐）**
   - 在 Telegram 找 @BotFather 创建 Bot，拿到 token；把 Bot 拉进目标频道并给它发消息权限。
   - TMDB API：到 https://www.themoviedb.org/settings/api 申请，用于识别海报和简介。不填也能推送，但没有海报和简介。

> 只想先跑起来看看界面？第 2、3 项都可以等部署成功后，在管理后台慢慢补。光鸭凭证从未配置到配置完成，全程在网页里点，不用改 `.env`、不用命令行。

---

## 三、部署（普通用户看这一节就够了）

### 第 1 步：建目录

在 NAS 或服务器上新建一个文件夹，例如飞牛 NAS 的 `/vol1/1000/docker/ya-tg`：

```bash
mkdir -p /vol1/1000/docker/ya-tg/db
mkdir -p /vol1/1000/docker/ya-tg/logs
cd /vol1/1000/docker/ya-tg
```

### 第 2 步：下载两个文件

在这个目录里放两个文件：`docker-compose.yml` 和 `.env`。

**docker-compose.yml** —— 直接复制下面整段（无需修改）：

```yaml
services:
  ya-tg:
    image: abson2/ya-tg:${IMAGE_TAG:-latest}
    container_name: ya-tg
    restart: unless-stopped
    ports:
      - "${WEB_BIND:-0.0.0.0}:${WEB_PORT:-8787}:8787"
    env_file:
      - ./.env
    environment:
      TZ: Asia/Shanghai
      DATABASE_URL: sqlite:////app/db/ya-tg.db
    volumes:
      - ./db:/app/db
      - ./logs:/app/logs
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8787/api/health')"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 20s
```

**注意**：如果你想部署指定历史版本（而不是最新版），在 `.env` 里加一行 `IMAGE_TAG=v0.5.3`，详见第六节。

**.env** —— 把下面内容复制进去，至少改掉 `ADMIN_PASSWORD` 和 `APP_SECRET`：

```env
# 必填：管理后台密码，改成你自己的强密码
ADMIN_PASSWORD=请改成强密码

# 必填：32 位以上随机字符串，用来加密 token。生成方法见下方
APP_SECRET=请改成32位以上随机字符串

# 网页访问端口，默认 8787
WEB_PORT=8787

# 光鸭账号不用在这里配置！
# 部署完成后打开管理网页，用手机号+短信验证码一键登录，凭证自动获取并加密保存。
# 不需要申请开放平台应用，也不需要抓包。

# TMDB（识别海报和简介，可选）
TMDB_BEARER_TOKEN=
TMDB_LANGUAGE=zh-CN

# 扫描计划：每 30 分钟扫一次，扫描根目录 ID 留空表示全盘
SCAN_CRON=*/30 * * * *
SCAN_ROOT_ID=

# 分享期限：0=永久，86400=1天，604800=7天，2592000=30天
AUTO_CREATE_SHARE=true
SHARE_DURATION=0
```

生成 `APP_SECRET` 的方法（复制命令执行，把结果粘到 .env）：

```bash
openssl rand -hex 32
```

> `.env` 里有密码和 token，执行 `chmod 600 .env` 保护它，并且**永远不要**把它发到网上或提交到 Git。

### 第 3 步：启动

```bash
docker compose up -d
```

第一次会自动从 DockerHub 拉取镜像，等几十秒。查看状态：

```bash
docker compose ps
```

看到 `ya-tg` 状态是 `Up (healthy)` 就成功了。如果一直是 `health: starting`，看日志：

```bash
docker compose logs -f ya-tg
```

### 第 4 步：打开管理界面

浏览器访问：

```
http://你的机器IP:8787
```

输入 `.env` 里的 `ADMIN_PASSWORD`，进入控制台。

> 如果访问不了：检查防火墙是否放行 8787 端口；飞牛 NAS 需要在「控制面板 → 防火墙」里添加规则。

---

## 四、首次使用向导（进入后台后做什么）

按这个顺序点，一般不会错：

1. **一键连接光鸭（最重要的一步）**
   - 进入「光鸭授权」页，最上方的「一键连接光鸭」卡片：
   - ① 填入你的**光鸭账号手机号**（如 `13800138000`），点「获取验证码」。
   - ② 收到短信后填入**验证码**，点「登录并连接」。
   - 看到「光鸭已连接」即成功。访问 Token、刷新 Token、设备 ID 全部自动获取并加密保存，无需改 `.env`、无需第三方项目，到期自动刷新。
   - 下方「网页分享会话」显示来源为「短信登录（自动）」即为正常——扫描和建分享会自动共用这套凭证。

   > 高级用户：如果你已有光鸭开放平台应用，可用页面下方「高级方式」里的 Device Code / Web PKCE 授权。

2. **（可跳过）手动配置分享会话**
   - 用上面的一键登录时，这一步会自动完成，不用管。
   - 只有坚持手工方式时，才需要到 `.env` 补 `GUANGYA_SHARE_*` 凭证并 `docker compose restart`。

3. **设置扫描范围**
   - 进入「扫描设置」页。建议第一次先填一个**测试目录 ID**，不要直接全盘。
   - 点「立即扫描」，看资源库里有没有文件出现。

4. **添加 Telegram Bot 和频道**
   - 进入「分发规则」页，按 01 → 02 → 03 的顺序：
     - 添加 Bot（填 @BotFather 给的 token）
     - 添加频道（填频道 ID，形如 `-1001234567890`）
     - 添加规则（选频道，决定「自动发布」还是「人工审核」）

5. **测试推送**
   - 在资源库点某个资源的「分享」，期限选 0（永久）。
   - 如果规则是自动发布，稍等片刻队列里会出现 `sent`，Telegram 频道收到消息。
   - 如果是人工审核，去「审核中心」点「通过」。

6. **正式使用**
   - 测试都通过后，把扫描根目录 ID 清空（全盘）或设为正式目录，保存计划，让定时任务自动跑。

---

## 五、常用配置项说明

| 配置项 | 作用 | 建议 |
| --- | --- | --- |
| `ADMIN_PASSWORD` | 管理后台登录密码 | 必须修改，用强密码 |
| `APP_SECRET` | 加密 token 的主密钥 | 必须修改；**改了之后要重新短信登录和重新添加 Bot** |
| 光鸭凭证（全部） | 扫描与建分享的登录凭证 | **不用在 .env 填**：管理页手机号+短信验证码一键登录，自动获取并刷新 |
| `TMDB_BEARER_TOKEN` | TMDB v4 令牌 | 推荐填，用于海报和简介 |
| `SCAN_CRON` | 扫描频率 | 默认每 30 分钟；5 段 Cron，北京时间 |
| `SCAN_ROOT_ID` | 从哪个目录开始扫 | 留空=全盘；测试时填小目录 |
| `SHARE_DURATION` | 分享有效期 | 0=永久，最省心 |
| `AUTO_CREATE_SHARE` | 是否自动建分享 | 默认 true |
| `IMAGE_TAG` | 镜像版本 | 留空=latest；指定版本见下节 |
| `WEB_PORT` | 网页端口 | 默认 8787，被占用可改 |

---

## 六、指定版本与回滚（历史版本）

每个正式版本都有独立的镜像标签和 GitHub Release，**不会被覆盖**。

- 查看所有版本：https://github.com/Abson2/ya-tg/releases
- 查看所有镜像标签：https://hub.docker.com/r/abson2/ya-tg/tags

### 部署指定版本

在 `.env` 里加一行（例如锁定 v0.5.3）：

```env
IMAGE_TAG=v0.5.3
```

然后：

```bash
docker compose pull
docker compose up -d
```

也可以用一键脚本切换：

```bash
bash scripts/update.sh v0.5.3
```

### 回滚到上一版

把 `IMAGE_TAG` 改成旧版本号，再执行 `docker compose pull && docker compose up -d`。数据库文件在 `./db` 里，不会被删除，回滚后数据还在。

> 如果旧版本的数据结构不兼容，请先备份 `db` 目录再回滚。

---

## 七、日常更新

```bash
cd /vol1/1000/docker/ya-tg
docker compose pull          # 拉取最新镜像
docker compose up -d         # 用新镜像重建容器
docker compose logs -f ya-tg # 观察日志
```

或者一条命令：

```bash
bash scripts/update.sh
```

---

## 八、常见问题

**Q：容器起不来，日志报光鸭接口 111？**
A：Device 授权申请设备码时校验出口 IP。到你光鸭开放平台后台把机器公网出口 IP 加入白名单。查看出口 IP：`curl ifconfig.me`。

**Q：授权成功了，但扫描报 117？**
A：Token 失效，程序会自动刷新重试。如果反复失败，去授权页重新授权一次。

**Q：推送一直失败，队列里显示错误？**
A：常见原因：Bot token 错、Bot 没在频道里、频道 ID 写错（要带负号）、光鸭登录凭证过期。看队列页的 `error` 字段定位。

**Q：分享创建失败 / 提示未登录？**
A：到「光鸭授权」页用手机号+短信验证码重新登录一次即可。登录凭证到期后一般会自动刷新；若短信登录失效（如长时间未用），重新登录会把新凭证加密保存，扫描和建分享自动恢复。无需抓包、无需改 `.env`。

**Q：TMDB 识别不准？**
A：在资源库点「手动识别」，输入正确的 TMDB ID。文件名越规范（含年份、SxxEexx），自动识别越准。

**Q：改了 .env 没生效？**
A：需要重建容器：`docker compose up -d --force-recreate`。

**Q：数据会丢吗？**
A：数据库和配置都在 `./db`，只要不删这个目录，升级/回滚数据都在。建议定期备份。

---

## 九、安全提醒

- `.env` 含密码和 token，权限设为 `chmod 600`，不要提交 Git、不要发聊天。
- 首次部署建议只在局域网访问；外网访问请叠加 VPN 或 Cloudflare Access。
- 不要用已经泄露过的会话凭证，撤销旧会话后重新登录。
- 定期备份 `db` 目录。

---

## 十、开发者：从源码构建

普通用户不需要这一节。如果你想改代码：

```bash
git clone https://github.com/Abson2/ya-tg.git
cd ya-tg
cp .env.example .env && chmod 600 .env
docker compose -f docker-compose.build.yml up -d --build
```

本地跑测试：

```bash
pip install -r backend/requirements.txt pytest
pytest tests -q
```

---

## 附：技术说明

- 官方 OpenAPI v1.3 文档：https://app.guangyapan.com/pan/docs/open-api
- 文件列表接口限频 5 次/秒，程序已内置限速和指数退避。
- 错误码：`111` IP 未报备、`116` 签名错误、`117` Token 无效、`123` 应用未开通写能力、`430` 非会员。
- 品牌说明：v0.2.0 及以前叫 GuangYa TG Publisher，v0.3.0 起改为 Ya-TG。`guangya_*` 模块名和 `GUANGYA_*` 环境变量保留（光鸭官方命名）。

版本历史详见 [CHANGELOG.md](CHANGELOG.md)。
