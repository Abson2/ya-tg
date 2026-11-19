# 更新日志

本文件记录 Ya-TG 的每个正式版本。版本标签遵循 `vX.Y.Z`，每个版本都会同步发布到：
- GitHub Releases：`https://github.com/Abson2/ya-tg/releases`
- DockerHub：`abson2/ya-tg:vX.Y.Z`

> 文档分工：`README.md` 只讲功能、部署与使用；逐版本的变更与修复细节统一写在这里，不写进 README。

---

## v0.9.5 (2026-11-19)

按用户反馈修复「扫码登录后凭证调用未生效、镜像同步报错」，并完成分发与互动中心最后一轮语义重构。不改 A→B 镜像主链路、不改推送口径（仍由全局单 Bot 推到频道/群聊）。

### 一、登录：二选一都能跑通完整业务（关键修复）
- 根因：扫码登录（webqr）写入 `AuthToken(id=1)` 时未补 `did`，且 flow=webqr 不被业务客户端认可，`GuangYaWebFiles` 直接抛 401「尚未用短信登录」，导致扫码登录后镜像/扫描/分享全部失效。
- 修复：`guangya_auth._save_token()` 对 webqr 补本地 did（与短信登录一致，业务接口只校验非空、不绑定 client_id）；`GuangYaWebFiles` / `GuangYaShareClient` / `scanner` / 连接验证的业务 flow 白名单纳入 webqr；`guangya_auth.refresh()` 对 webqr 走网页端设备码专属 base + 公开 client_id。现在短信与扫码二选一，登录后都能扫描、镜像、建分享、推送。
- 刷新显示：`auth/status` 与 `sms/status` 把 webqr 计入 configured / has_refresh_token / auto_refresh；扫码成功前端同时刷新 auth 与 sms 凭证卡。
- 删除 Device Code 授权与 Web OAuth 2.0 + PKCE 两套登录方式的前后端端点、函数、状态与入口，管理页只保留短信登录 + 扫码登录。

### 二、推送白名单改为「目录路由表」
- 新配置 `push_routes_json`（默认 1 条、最多 10 条）：每条 = 影视库 B 的下属目录 ID（空=根目录通配）+ 频道 ID 或群 ID（任填其一）。不同目录经同一 Bot 推到不同频道/群。
- 入队链路 `_enqueue_after_identify()` 优先按目录路由匹配资源（精确 parent_id 或命中目录链），路由表为空时回退旧分发规则，保持升级兼容；同一作品同一目标仍只入一条。
- 新增 `GET/POST /api/push/routes`：保存路由表时自动同步维护启用 Channel（按 chat_id 复用，标记 `route:` 前缀仅清理路由自创目标，不碰老数据），并在一次请求里更新全局单 Bot token。

### 三、使用限制独立 + 跳转
- 使用限制改为独立开关 `bot_restrict_enabled`，与推送白名单不再二选一；「关注的频道 ID / 加入的群 ID」任填其一即可（OR 语义，满足任一候选即放行，不再要求全部满足）。
- 未满足限制的用户使用 Bot 时，回复带「关注官方频道 / 交流群」跳转按钮的消息；链接填 @用户名 或 t.me 地址（`bot_required_channel_link` / `bot_required_group_link`）。

### 四、推送消息频道/群跳转（与打赏配置合并）
- 打赏配置卡扩展为「打赏与消息跳转配置」：新增 `tg_channel_link` / `tg_group_link`，推送消息加「📢 官方频道」「👥 交流群」inline 按钮。
- 消息模板新增 `{{channel_link}}` / `{{group_link}}` 变量，与按钮共用同一份配置；按钮模式/纯文本模式都可显示跳转。

### 五、前端「分发与互动中心」收敛
- 删除重复的 Bot 管理、推送目标、分发规则三个表格与统一弹窗；保留并增强「交互 Bot 与推送配置」（token 填写 / 验证 / 测试推送 / 管理）。
- a 板块重排为：交互 Bot 与推送配置 → 推送白名单（目录路由）→ 内置指令速查 → 使用限制 → 打赏与消息跳转；b 板块投稿求片、c 板块 TgtoDrive 保持。

### 六、回归
- 新增 `test_v095_push_routes.py`（目录路由匹配/归一/上限、OR 限制、t.me 链接归一、模板变量、路由保存端点同步 Channel 与 Bot），并为扫码补 did 与 WebFiles 可用性断言。全套 306 passed，前端 vite build 通过。

## v0.9.4 (2026-11-19)

按用户反馈把「分发与互动中心」里功能重复的六个模块融合成一张配置卡，并做数据去重：同一份 Bot Token、同一份推送目标不再重复填写。不改 A→B 镜像主链路、不改推送口径（仍由 Bot 推到频道/群聊）、不改白名单/限制二选一语义。

### 一、数据去重（后端）
- 交互 Bot Token 复用 Bot 管理：新增 `interaction_bot_id` 引用 bots 表 id；`bot_listener.effective_token()` 取所选 Bot 的解密 token 用于监听私聊指令与事务性回复，未选/停用回退旧 `contribution_bot_token`（老配置兼容）。管理页只选 Bot，不再单填 token。
- 推送白名单复用推送目标：`push_whitelist()` 改为读 channels 表 enabled 记录，按新增的 `Channel.kind`（channel/group，仅菜单分类用）分频道/群聊；channels 为空时回退旧 `bot_push_*_json`。publisher 闸门 `is_push_allowed()`、私聊菜单「官方频道/交流群组」都随之读同一份数据。
- 老库轻量迁移：channels 表补 `kind TEXT DEFAULT 'channel'`；老库未选交互 Bot 时行为不变。
- 兼容：`/api/interaction/config` 回显 `interaction_bot_id` 与 `bot_options`（token 仅掩码）；保存可写入/清空 bot 引用，非法 id 返回 400。add_channel 归一化 kind。

### 二、UI 融合为一张卡（前端）
- 交互 Bot、推送目标（频道/群聊）、分发规则、推送白名单、使用限制合并到同一张「交互 Bot 与推送配置」卡：选 Bot 下拉（复用 Bot 管理 token）、限制方式二选一、白名单来源说明（=推送目标启用记录）。
- 删除重复的「交互 Bot Token」输入框与「推送白名单 ID」textarea；推送目标表新增「类型」列（频道/群），添加目标时可选类型。
- 使用限制（关注/加群）保留为其 membership 分支，内置指令速查、TG 推送配置、推送调度、打赏、投稿求片、TgtoDrive 联动维持原位置。

### 验证
- 全套 pytest 297 passed（新增 `test_v094_merge_config.py` 11 项锁死去重红线），前端 vite build 1448 modules 通过。

---

## v0.9.3 (2026-11-19)

v0.9.2 发版后的两处收尾：后台扫码二维码渲染修复、分发与互动中心 UI 三板块重构。不改后端主链路、不改推送口径、不改认证协议。

### 一、后台扫码登录二维码修复
- 现象：光鸭授权页点扫码登录只显示授权码，没有二维码图。
- 原因：后端 `/api/auth/webqr/start` 已返回 `verification_uri_complete`，前端只取了 `user_code` 文本，没渲染二维码。
- 改法：前端引入 `qrcode@^1.5.4`，用 `verification_uri_complete` 生成 PNG DataURL 渲染在授权页；二维码只能用光鸭 App 扫一扫，不能用浏览器打开。短信登录保留。

### 二、分发与互动中心 UI 重构为三板块
- 删除「分发配置 / 互动设置」二级切换（`distSub` 及 `.sub-tabs` 全部移除），只保留一个「分发与互动中心」页面，原分发配置功能全部并入。
- a 板块「交互 Bot · 频道 / 群聊 · 推送白名单 · 打赏」：限制方式二选一、推送白名单、使用限制、交互 Bot（Token / 指令监听 / 内置指令速查）、Bot 管理 / 推送目标（频道·群聊）/ 分发规则三卡、TG 推送配置、推送调度 + 链路自检 + Telegram API 地址、打赏配置。
- b 板块「用户投稿 · 用户求片」：用户投稿审核表、我想看关键词（求片同步）。
- c 板块「TgtoDrive 联动」：只保留地址 / 场景名 / OpenAPI Token / 测试连接；Bot Token 归入 a 板块交互 Bot，打赏配置归入 a 板块，TgtoDrive 卡内不再出现这两项。
- 打赏说明改用文本 `reward_code` 变量（原模板里 `{{reward_code}}` 会被 Vue 当插值消费，已修正）。

### 验证
- 全套 pytest 286 passed，前端 vite build 通过（1448 modules）。二维码真机扫码、A/B 真实环境联调仍需真实环境确认。

---

## v0.9.2 (2026-11-19)

基于 v0.9.1 的继续完善：B 库改名断层修复、Bot 真交互菜单、限制方式二选一、推送口径统一，以及后台扫码登录。均不改 A→B 镜像主链路。

### 一、B 库单文件改名断层修复
- 根因：复制任务 `done` 后，B 库文件在网盘列表中的可见性有秒级延迟；旧逻辑列目录一次没命中就凭 DB 状态判断，出现「文件已复制但没改名」。
- 改法：改名只认 B 库只读列目录实际命中的 fileId，最多只读复查 2 次（仍走 `file_idle_seconds` 风控间隔，不增加写接口）；全部待改文件可见才发改名，否则整部标 `pending_visible`、本轮跳过。
- 新增后台短任务每 120 秒复查 `pending_visible`/`failed`/未处理行，拿不到镜像互斥锁就跳过，不与手动同步抢写锁。专项 11 项回归通过。

### 二、Bot 真交互菜单
- `poll_once` 新增 `callback_query` 处理：/start 弹两列 inline 按钮（频道 / 群组 / 投稿求片 / 搜索 / 打赏），点击走 callback 路由（answerCallbackQuery + editMessageText），授权地址仅 App 内授权、不私聊刷屏。
- 投稿 / 求片审核后在管理页点「通过/拒绝/转存」时，Bot 主动回复用户一次（事务性通知，非推送）。

### 三、使用限制与推送白名单二选一
- 新增 `bot_gate_mode`：whitelist（默认，只按白名单过滤推送）/ membership（只校验关注频道或加群）/ off（都不拦截）。空值兼容旧 `bot_restrict_enabled`。
- 频道与群聊两类不必都配；推送闸门与用户限制按所选模式各自生效，不再双重要求。

### 四、推送口径统一：由 Bot 推送到频道 / 群聊
- 删除 `custom_bot_group` 全局覆盖分支：`_send_telegram` 恒用该 Channel 所属 Bot 的 token 发到该 Channel 的 chat_id，频道与群聊走完全相同路径，都过白名单。
- 配置端把旧 `push_mode` 归一为 `bot_channel` 并清空 `tg_custom_*`（存量升级自动迁移）；UI 推送方式改只读说明，「频道管理」改「推送目标（频道 / 群聊）」并显示 chat_id。

### 五、后台扫码登录（与 TG 无关）
- 后台「光鸭授权」页新增网页端扫码登录：复用官方 Skill 的设备码协议（公开 client_id，无需开放平台应用凭证 / 签名 / IP 白名单），光鸭 App 扫一扫确认即可。短信登录保留。
- 端点 `POST /api/auth/webqr/start`、`/poll`；Token 落 `flow=webqr`。二维码地址仅供 App 内授权，不能用浏览器打开。

### 验证与边界
- 全套 pytest 286 passed，前端 vite build 通过；版本号四处一致（package.json / main.py / App.vue / README）。
- 不含任何用户数据：投稿、求片、Token、HAR、网盘目录均不进入仓库 / Release / 外部服务。A/B 真实环境联调、投稿转存闭环仍需真实数据与 HAR。

---

## v0.9.1 (2026-11-19)

v0.9.0 发版后的三项收尾：剧集状态格式改为季集标签、镜像同步单轮作品上限、分发规则并入 Bot 交互中心。均为 v0.9.0 遗留项的继续，不涉及主链路改动。

### 一、推送模板剧集状态改为季集标签
- `episode_status` 由「更新至第 N 集 / 共 M 集 · 连载中」「全 M 集 · 已完结」改为用户要求的 **「更新至 S01E07 · 连载中」/「S01E24 · 完结」**。季号取本地已收录最后一集所在季（多季剧取最后一季），集号取该季最大集号。
- 总集数 / 进度不再塞进状态文案，仍由 `episode_progress`（N/M）与 `episode_count`（本地已收录）单独承载，信息不重复。未标连载 / 完结时只输出「更新至 SxxExx」，不臆断状态。
- 同步更新默认模板注释、消息模板页变量说明与相关测试断言；单集文件名仍需隐藏，只保留季集进度标签（进度标签不是文件名泄漏）。

### 二、镜像同步单轮作品上限（防止一次整理过多）
- 用户诉求：「限制单次扫描整理的整部剧 / 电影数量，处理完等下一次定时继续；第一部剧不要间断，整理完再休息」。
- `MirrorRule` 新增 `max_works_per_run`（0=不限量，默认）与 `works_cursor`（轮转游标），配置页可手调（0~1000）。
- `mirror.plan_work_batch()` 把作品列表按「上限 + 游标」切批：从上次最后一部之后接着取，到末尾回绕，保证每部作品都会轮到、同一部剧不会被拆到两轮（sync_rule 对单部作品是文件复制 + 批量轮询的原子处理）。到上限即停，剩余留给下一次定时同步。
- `sync_rule()` 的 `last_message` 追加「本轮限额 N 部 / 实处理 M 部…余 K 部下轮继续」；镜像状态页同步显示生效上限。测试 9 项（切批 / 回绕 / 旧游标 / 不限量 / 作品级原子 / 配置回显）。

### 三、分发规则并入 Bot 交互中心
- 侧栏「分发规则」与「Bot 交互中心」两个入口合并为单一「分发与互动中心」，页内二级切换两子页：**互动设置**（指令 / 推送白名单 / 使用限制 / 投稿搜索求片打赏）与**分发配置**（Bot → 频道 → 规则三卡 + 推送间隔 + TG 配置 + 赞赏码）。
- 推送间隔（检查队列秒数 / Cron / 每条消息间隔）提升为分发配置页顶部独立卡片，原链路自检面板里的重复控件移除。
- 四点 Bot 口径不变：所有推送由 Bot 发起且只推白名单内频道 / 群聊、不私聊刷屏；内置四指令固定 + 后台自定义；使用限制（关注频道 / 加群 + 缓存）；推送白名单（频道需管理员 / 群聊无需）。

### 四、A/B 真实环境联调清单
- `docs/需求追踪矩阵.md` 追加可执行联调清单：10 项冒烟（含新上限的验证步骤）、5 类需抓包材料、`source/mode` 数据层隔离缺口验证方法、离线已覆盖范围、结论回填口径。明确**离线无法代跑真实网盘 / 账号 / Token**，不把未实测项标成通过。

---

## v0.9.0 (2026-11-19)

按用户「UI 是全体重构、不是只处理 bot」「分发中心和互动中心做在一起」的要求，本版完成全站视觉语言统一 + Bot 交互中心落地，并随版交付 v0.8.9.1 改名修复。功能口径不变，只重构界面、整合入口与补齐交互配置。

### 一、全站 UI 视觉重构（参考 embyhub，覆盖每个页面）
- 把原先只在 Bot 中心使用的 embyhub 卡片语言提升为**全局标准**：所有 `.panel` 标题带渐变图标块、大圆角（20px）、柔和双层阴影与悬浮反馈；`.page-head` 主标题加双色竖条；`.stat-card` 图标放大、数字加粗、悬浮抬升。
- 全站统一组件圆角与留白：顶栏阴影、表格表头浅底、输入框 / 按钮 / Tag 圆角、二级数据卡（授权摘要 / 凭证条 / 方法卡）圆角对齐主卡。背景改为固定淡紫 + 淡绿径向渐变。
- 概览、光鸭授权、资源库、分发规则、审核、队列、TMDB、消息模板、操作审计等所有页面共享同一套卡片语言，不再是「只有 Bot 页换了皮」。

### 二、分发与互动合并 + Bot 交互中心
- 侧栏导航由「分发中心 / 系统 / 互动中心」三段合并为「工作台 / 光鸭资源 / 分发与互动 / 系统」四组；分发规则、Bot 交互中心、审核中心、推送队列归入同一组。
- Bot 交互中心改为 embyhub 风格卡片：**内置指令速查**（求片 / 投稿 / 搜索 / 打赏固定四项 + 后台自定义「收到指令→固定文本」）、**使用限制**（关注频道 / 加入群聊 + 校验缓存秒数，浅绿卡）、**推送白名单**（频道需管理员 / 群聊无需管理员）、Bot Token / TgtoDrive / 打赏折叠收纳。
- 后端新增 7 个运行时配置 key（`bot_commands_json` / `bot_push_channels_json` / `bot_push_groups_json` / `bot_restrict_enabled` / `bot_restrict_cache_seconds` / `bot_required_channels_json` / `bot_required_groups_json`），`/api/interaction/config` 保存与回显；`publisher.process_queue()` 增加白名单闸门——Bot 只向白名单内频道 / 群聊推送，不私聊用户刷屏，白名单为空时保持兼容全放行。
- 最终口径：**所有推送由 Bot 发起且只推白名单内会话**；签到 / 抽奖 / 注册 / 积分等只作 embyhub 视觉参考，不实现。

### 三、镜像同步页信息治理
- 「同步状态」原本 15 个数字挤成一排，改为**文件传输 / 英文改名 / 目录 / 任务风控**四个语义分组卡片，每组独立小网格。
- 「生效配置 / 已确认接口 / 待验证接口」三大段长串小字改为结构化 chip 与 `code` 标签列表，开关状态用绿 / 红色高亮，不再堆满一屏。
- `/api/mirror/status` 新增 `pending_items` 聚合（目录待建 + 待复制 / 复制中 + 待改名 + 待确认任务统一成一张待办表），配合原有映射清单构成「映射清单 + 待办清单」两个清单。

### 四、随版修复与版本
- 交付 v0.8.9.1 单集 / 电影中文名改英文名失效修复（详见下节），全套 246 passed。
- 前后端版本统一升至 0.9.0（三处声明：FastAPI app、health、package.json）；前端构建 1399 modules 通过。

---

## v0.8.9.1 修复（未单独发版，随 v0.9.0 交付）：单集 / 电影中文名改英文名失效

用户实测：「v0.8.9 单集、单电影中文名替换英文的能力又丢了」「翻译没成功」，现场是**文件名仍是中文**、状态页有「尚未解析到英文剧名」提示，且**只配了 TMDB、没配 AI**。

根因（v0.8.9 引入的两处回退，均已在 `_resolve_english_title()` 修复）：
- **TMDB ID 取不到**：`tid` 只从 A 库目录的 `{tmdb-}` 标记或已识别 `Resource` 取。镜像阶段目录名未必带标记、资源尚未入库，于是 `tid=None` → `if tid` 整段 TMDB 查询被跳过 → 直接落到未配置的 AI / 拼音，文件名保持中文。修复：从 `MirrorFile.identity`（镜像时已写入 `tmdb:135646:tv:...`）补出 TMDB ID。
- **剧集类型判断错**：`MirrorMedia` 在镜像阶段不写 `media_type`（默认空），v0.8.9 写死 `else "movie"`，所有电视剧用 `/movie/{id}` 查 en-US 英文名，必然查不到。v0.8.8 是 movie + tv 都试才有英文名。修复：从 `MirrorFile.identity` 的 `:tv:` / `:movie:` 推断类型；类型仍未知时 movie + tv 各查一次补回覆盖率。
- 卡死红线不破坏：双查**每次仍带 6 秒硬超时**，最坏 12 秒且绝不重试，仍由 `_bounded()` 包裹。

新增 3 项回归（`test_v089_no_deadlock.py`）：identity 补 TMDB ID、identity 补剧集类型、已知 ID 未知类型双端点补查。全套 246 passed。

---

## v0.8.9 (2026-11-18)

按用户实测反馈「整理剧集，修改名称的过程中，后台管理页面卡死，打不开了」和
「在 tmdb 上找不到英文名称的剧集卡死了，找不到英文名称的剧集直接使用直译」修复。
两者是同一条链路上的三个叠加缺陷，本版一次收口。

### 一、TMDB 找不到英文名时卡住整轮同步（用户直接指向的根因）
- `_resolve_english_title()` 对 `media_type` 未知的作品会**串行查 TMDB movie + tv 两次**（`httpx` 单次超时 20 秒），再等 AI 兜底；一部剧就能卡几十秒，剧集多了整轮同步几分钟不返回。查不到又只缓存空值，于是每轮重查，正是「第一次不成功、多点几次才成功」的来源。
- 修复：TMDB **只查一次**并加 6 秒硬超时；AI 直译加 10 秒硬超时；所有外部调用经 `_bounded()` 包裹，超时 / 异常一律当作「没查到」立刻进入下一层，**绝不等待、绝不重试**。
- **用户口径「找不到就直接直译」**：TMDB 无官方英文名 → 立即用 AI 直译中文剧名 → AI 未配置 / 超时 / 模型不给结果时用 `file_naming.pinyin_title()` 本地拼音直译（纯 Python、零网络），保证永远有可用 ASCII 剧名。改名结果新增 `literal_ai` / `literal_pinyin` 计数，管理页提示带「无官方英文名·直译 N 部 · 拼音兜底 N 部」。
- 拼音是音译、AI 是语义直译，AI 可用时优先；仅作最后一层兜底，避免某部剧没有官方英文名就把整个同步卡住。`backend/requirements.txt` 新增 `pypinyin==0.55.0`（纯 Python 依赖）。

### 二、改名 / 复制循环跨 `await` 持有 SQLite 写事务（页面打不开的直接原因）
- v0.8.8 只在 `scanner.run_scan()` 修过「写事务跨网络 await」；`mirror.py` 全链路仍用 `db.flush()` 不提交。复制/改名每个文件之间都有 `await asyncio.sleep(idle)`、HTTP 请求与任务轮询，写事务一持就是几十秒。SQLite 只有一个写者，叠加 v0.8.8 的 30 秒 `busy_timeout`，管理页所有要写库的请求全部排队 → 页面卡死打不开。
- 修复：`mirror.py` 里 9 处 `db.flush()` 全部改为 `db.commit()`（`_record_dir` / media 创建 / MirrorFile / MirrorTask / 英文名缓存 / 改名成功与跳过分支），**任何 `await` 之前都不留未提交写事务**；模块头新增第 8 条设计红线，并加回归禁止再出现 `db.flush()`。
- 用独立 SQLite 连接 `BEGIN IMMEDIATE` 探针，在每次 `await rename_file()` 时验证写锁可立即获取，锁死这条红线。

### 三、重复点击叠加多个分钟级长任务
- 手动「立即同步」、「按英文规范改名」、「继续轮询任务」与定时镜像之间没有任何互斥，用户等不及连点就会并发跑多个长任务，互相抢写锁并成倍放大列目录 / rename 请求。
- 修复：`main.py` 增加进程内 `threading.Lock` 与 `_mirror_guard()`；三个手动端点共用，拿不到锁立刻返回 409「已有镜像同步 / 改名任务正在运行，请等当前任务结束后再操作」，**不让请求排队堆积**；定时镜像拿不到锁直接跳过本轮并记录日志。前端按钮补 `:disabled="mirrorBusy"`，运行期间不可重复点。

### 四、主链路边界：改名永远不阻断复制 / 入库 / 分享 / 推送
- 不在「入库识别管线」之后补跑英文名解析（会再次触发 TMDB / AI 网络等待）；改名环节自身已快速直译兜底。
- `_safe_rename()` 继续隔离整体异常，异常只记日志与 `rename.errors`；复制 → 回填映射 → 改名 → 刷新入库 → 识别 → 按分发规则推送的链路照常走完。新增回归锁死「改名整体抛异常时管线仍被调用」。
- `rename_mirror_files()` 的「英文名待处理」现在只在三层兜底全部失败时才会出现（有拼音后极少），文案明确写出「已跳过改名，复制、入库、分享与推送不受影响」。

### 五、升级与验证
- **升级后直接点一次「立即同步」即可**：TMDB 查不到英文名的剧集会当轮直译改名，不需要连点多次；管理页在同步期间保持可打开。
- 若之前已经卡死：先 `docker compose restart ya-tg`。已改名成功的文件与 A→B 映射都已逐条提交落库，不会丢；重启后按 B 库实际文件名自动纠正状态，不需要清库或重建数据库。
- 新增 `tests/test_v089_no_deadlock.py` 10 项（拼音纯函数、TMDB 只查一次、硬超时兜底、AI 优先、直译改名计数、写锁探针、禁止 flush、互斥 409、四入口上锁、改名异常不阻断管线）；`tests/test_v086_file_rename.py` 的「无英文名整部跳过」用例改为断言拼音直译生效。

### 六、长剧集改名分批与断点续改
- 用户反馈「剧集太长容易出现改名失败，比如中国通史，实际上改了一部分」：一轮连发上百次 rename，叠加每文件 `idle` 秒节流与 429/5xx 三次退避，单轮会超过前端 300 秒超时，界面报失败但文件其实已改了一部分。
- `rename_mirror_files()` 增加三重预算：`RENAME_MAX_FILES=50`、`RENAME_MAX_SECONDS=90`、`RENAME_MAX_CONSECUTIVE_FAILURES=5`；每个文件改名后立即 `db.commit()`，单文件异常（含 `db.commit()` 写冲突）只影响当前文件，绝不中断整部剧。
- 预算触发后本轮提前结束，后续作品不再空跑 TMDB / AI 解析；剩余量按「同规则下 `status=synced` 且 `rename_status != done`」一次性统计，写入 `remaining` / `truncated_reason` 与 `notices[]`（**不是 errors**，前端不会误报失败），提示「剩余 N 个文件下一轮自动续改」。B 库列目录暂未找到的文件标 `pending_visible`，下一轮自动重试，不再一次性判死。

### 七、推送模板增加剧集状态与集数进度变量
- 用户反馈「推送的模版增加剧集状态比如更新至多少集，或者当前集/总集数」。
- `Resource` 新增 `total_episodes`（TMDB `tv.number_of_episodes`，由 `_apply_identification()` 回填，电影恒为 0），与已有的 `episode_count`（本地已收录集数）刻意分开，追更剧两者不等才有意义。
- `_build_message()` 新增 5 个上下文变量：`episode_status`（如「更新至第 12 集 / 共 24 集 · 连载中」/「全 24 集 · 已完结」）、`episode_progress`（`12/24`）、`updated_to_episode`、`current_episode`、`total_episodes`；电影时全部为空，配合 `{{#if}}` 自动不渲染。
- 默认模板补「📡 状态」「🔢 进度」两行，集数行改为「本地已收录：共 N 集」以区分官方总集数；`_needs_template_upgrade()` 用「旧版集数行原句 + 默认骨架」精确识别 v0.6.5~v0.8.8 存量默认模板并自动升级，**用户自定义模板一律不动**。前端变量面板与说明同步补齐。
- 新增 `tests/test_v089_episode_status.py` 9 项。

### 八、目录映射清单「清理无效映射」与镜像同步页分区
- 用户反馈「目录映射清单没有清理无效的按钮」「镜像同步模块的界面优化，信息乱七八糟」。
- 新增 `POST /api/mirror/mappings/prune`（支持 `dry_run=true` 预览）与 `mirror.prune_stale_mappings()`：只读核对 B 库实际内容，清理其中已不存在的 `MirrorFile.target_file_id` / `MirrorDir.target_dir_id`。**只重置映射不删行**，保留 A 库 `file_name` / `source_file_id` / `fingerprint`，下一轮「立即同步」自动补复制并改名；不动 `Resource`、分享链接与推送队列。列目录不可信、分页截断或整部作品 B 库为空时一律跳过并计数，绝不误清。该端点与同步 / 改名 / 任务轮询共用同一把互斥锁。
- 镜像同步页把「配置说明 / 生效配置 / 问题诊断 / 映射清单」收进折叠分区，问题诊断标题带待处理数量角标；目录映射清单新增「清理无效映射」按钮并回显清理数量与跳过原因。
- 新增 `tests/test_v089_mapping_prune.py` 9 项。

### 九、验证
- 全套 `243 passed, 1 warning`；前端 `vite build` 通过（1399 modules）。
- 真实环境待验证：拼音剧名对审核的接受度、`pypinyin` 在镜像里的加载耗时、TMDB / AI 硬超时在慢网络下是否偏紧（可在 `mirror.py` 的两个常量调整）、分批改名的 50 文件 / 90 秒预算在超长剧集上的轮数。

---

## v0.8.8 (2026-11-18)

按用户实测反馈「现在文件是有了，不过影视单集、电影文件汉字名称没有翻译」修复改名链路。
现场同步结果是「重试 4 · 完成 4 · **改名 0 · 待翻译作品 0 · 改名失败 0**」——
v0.8.7 的空目录补复制已经生效，但改名整批没执行。

### 一、头号根因：补复制回来的是中文原名，DB 却记着「已改名」
- v0.8.7 把 DB 标记 `synced` 但 B 库实际缺失的文件**复用同一行**重新复制；复制回来的是 **A 库中文原名**。但该行只重置了 `target_file_id`，`rename_status=done` / `target_name=英文名` / `rename_error` 残留。
- `rename_mirror_files()` 旧逻辑用 `row.target_name == target and row.rename_status == "done"` 判幂等 → 整批 `skipped`，一次 `rename_file` 都不调。前端提示只显示「改名 / 待翻译 / 改名失败」三项，恰好全是 0，形成诊断黑洞。
- 修复 ①：`sync_rule()` 的补复制分支同步清空 `target_name` / `rename_status` / `rename_error`，新复制的文件一律视为「待改名」。
- 修复 ②（更根本）：`rename_mirror_files()` 改为**以 B 库实际文件名为准**，与 v0.8.7「DB 状态不等于网盘实际」同一红线。列目录后建立 `fileId → 实际（归一化）文件名` 反查：B 库当前名已是目标英文名才跳过（并顺手把 DB 状态纠正为 `done`）；否则无论 DB 写的是什么都重新改名。`fileId` 也优先取 B 库列目录命中值，DB 的 `target_file_id` 只在列目录失败时兜底（可能是补复制前的旧 ID）。
- 列目录失败 / 分页不完整时 `verified=False`，仍退回 DB 幂等判断，防接口抖动误改。冲突加 `-vN`、文件间 `file_idle_seconds` 风控、`file_name` 恒为 A 原名、A 库只读、B 库分类目录 / 影视主目录 / 季目录名不动等既有红线全部不变。
- 用户现场文件名 `小黄人大眼萌 (2015) - 2160p.BluRay REMUX.HDR.TrueHD 7.1 Atmos-DreamHD.mkv` 已补纯函数回归：命名规则本身没问题，问题只在状态机。

### 二、诊断补齐：不再出现「三个零」
- `rename_mirror_files()` 新增 `no_rows`（没有 `status=synced` 文件的作品数）；`_safe_rename()` 整体异常返回同样带该字段。
- `POST /api/mirror/run` 把 `rename.errors[]` 首条追加到 `rule.last_message`，刷新「同步状态」页仍能看到「文件改名未执行：…」或单文件真实报错，不再只留在本次响应里。
- 管理页同步结果提示改为「改名 N · **已达标跳过 N · 无中文跳过 N** · 待翻译作品 N · 改名失败 N」，并带出「无待改名文件 N 部」「首个改名错误：…」；开关关闭或未返回统计时明确写出原因；有改名错误时提示降级为 warning。

### 三、SQLite `database is locked` 并发写冲突（用户「失败一直重试中」）

现场报错：`sqlite3.OperationalError: database is locked`，SQL 为 `UPDATE resources SET thumbnail=?, scanned_at=? WHERE resources.id = ?`。两层根因：

- **连接层**：`db.py` 的 SQLite 连接既没开 WAL，也没放宽写等待（pysqlite 默认约 5 秒）。后台 job（3 秒 Token 轮询 / 10 秒 Bot 轮询 / 30 秒队列 worker）、定时扫描、定时镜像与手动「立即同步」都在同一事件循环里并发写库，短写事务撞上扫描的长事务就立刻报错。
- **扫描层（更关键）**：`scanner.run_scan()` 在 `db.flush()` 插入 `ResourceScanBatch` 后，直接 `await api.list_files()` 发网络请求——**写事务跨 await 持有**，把持锁时间从毫秒级放大成一整次网络往返。

修复：① `db.py` 给 SQLite 连接设 `timeout=30`，并在 `connect` 事件里执行 `PRAGMA journal_mode=WAL` / `busy_timeout=30000` / `synchronous=NORMAL`（内存库等不支持场景静默跳过）；② 扫描批次插入后先 `flush()` 取 ID、再**立即 `commit()`**，绝不跨网络 await 持有写事务；③ 锁错误落可操作中文提示到 `ResourceScanBatch.error` / `ScanCursor.error` / `ScanTask.last_message` 与 502 响应——「请确认只运行一个容器实例、没有两个 compose 项目共用同一个 `./db`」，避免用户在「失败 → 重试 → 又失败」里空转。扫描的对账 / 断点语义不变：异常仍标 `failed`、保留现场，下一轮全量重扫。

### 四、升级与验证
- **升级后点一次「立即同步」即可自愈改名**：新逻辑按 B 库实际文件名发现中文名并重新改成英文剧名，不需要清库、不需要重新复制、不需要重建数据库。也可直接点「镜像同步 → 按英文规范改名」（或「重新解析英文名并改名」）立刻处理存量。
- **数据库写冲突**：升级后重启一次容器让 WAL 生效；若仍报 `database is locked`，先确认只有一个容器实例、没有两个 compose 项目共用同一个 `./db` 目录。
- 新增 `tests/test_v088_rename_after_recopy.py` 9 项 + `tests/test_v088_sqlite_concurrency.py` 5 项（WAL / busy_timeout 生效、批次在网络 await 前已提交、锁错误落中文提示、502 带可操作提示）。另补两项发版前加固回归：三条 PRAGMA 各自独立执行（`journal_mode` 自身抛 `database is locked` 时不会连带吞掉最关键的 30 秒写等待）、`last_message` 为 NULL 时追加改名诊断不会把整次同步拖成 failed。
- 全套 `215 passed, 1 warning`；v0.8.6 的 18 项改名回归与 v0.8.7 的 17 项空目录回归全部保持通过；前端 `vite build` 通过（1399 modules）。
- 真实环境待验证：`rename` 对中文→英文长文件名（含 `BluRay REMUX` / `TrueHD 7.1 Atmos` 等空格与点号）的接受度、TMDB en-US 对中文作品的英文名覆盖率、改名后审核是否通过、多容器实例下的写等待表现。

---

## v0.8.7 (2026-11-18)

按用户实测反馈「又出现之前的问题，B库内影视文件夹下为空目录」修掉 v0.8.4 只补了一半的根因，
并按两条追加口径重建资源库刷新语义与多层分类树镜像：
「资源库里面刷新定义为读取最新的B库内容，B库文件夹已无该影视则清理出面板」、
「目录结构未来可能会是：影视-电视剧-国产剧-侠客行（2001），这里目录树也需要参考一起镜像，只改动侠客行文件夹及单集」。
选项确认：刷新入口 = 三个入口统一重建；空目录处理 = 补复制 + 清理面板。

### 一、头号根因：DB 标记 `synced` 不等于 B 库真的有文件
- v0.8.4 只修了 `copying/failed/unknown` 被当成 `unchanged` 永久跳过；本版补上另一半：文件在 B 库被删、B 库换目录、复制任务报告成功却没落地时，`MirrorFile.status` 永远停在 `synced`，再点几次「立即同步」都判 unchanged，作品目录永远是空的。
- `sync_rule()` 每部作品只读递归列一次 B 库目录树，核对已同步记录是否真实存在；`_b_file_candidates()` 由返回单一映射改为返回 `(候选映射, 列目录是否完整可信)`。
- 只有「列目录完整可信 + 确实找不到」才把 `synced` 重置为 `pending`，**复用同一行**重新复制，不新建记录、不碰 A 库、不改任何目录名；`file_name` 仍是 A 原名，A→B 映射不丢。
- 三道防误判闸门：① 列目录异常 / 分页不完整 / 目录层级超 60 层 → `verified=False`，一律不动 `synced`；② B 库文件数不少于已同步记录数 → 绝不重置（多半只是网盘改名或重名加了后缀），只记 `name_mismatch`；③ 命中 `target_name` 优先反查，v0.8.6 改过英文名的文件不会被误判丢失。
- 列目录由「主目录 + 一层直属子目录」改为递归遍历，修 `作品/Season 1/1080p/` 这类多层结构被误判成「B 库空」而重复复制整部作品。
- 计数与诊断：新增 `missing_in_b` / `name_mismatch` 进 `stats` 与 `rule.last_message`，文件级错误进 `file_errors`，管理页同步结果可直接看到补复制了几集。补复制同样走 `file_idle_seconds`（硬下限 0.5 秒）风控。

### 二、资源库「刷新」= 读取最新 B 库内容并按实际内容重建
- 顶部「刷新」、资源库「立即扫描」、「重新扫描库」三个入口统一走新增的 `main._refresh_library_from_b()`：`run_scan(B 库, resume=False)` → `_reconcile_library` → `_post_scan_identify` → `process_change_events`。新增 `POST /api/library/refresh`；`POST /api/scan-now` 的 `resume` 参数仅为兼容旧调用保留，统一为全量扫描。
- 旧 `_reconcile_library()` 在 `seen=[]` 时一律跳过对账，导致「B 库一个视频都没有」时旧作品卡片永远清不掉。新增 `allow_empty=True`，但只允许**可信空批次**清库：`status=success` + `complete` + 非断点续扫 + **确实扫到了目录**（`directories>0`）+ `seen_count=0`。
- B 库根目录都读不到（`directories=0`）、批次不完整 / 失败 / 断点续扫时仍返回 `empty_scan_untrusted` / `batch_not_allowed`，接口抖动不会误清整个资源库。审计记录新增 `empty_seen_allowed`，返回值带同名字段，前端提示「B 库为空，面板已按实际内容清空」。
- **定时扫描与镜像后管线不走这条重建路径**，继续保持「不自动删数据」；镜像完成仍只推本轮识别成功且有海报的影视，不群推、不批量补推历史内容变化事件。

### 三、多层分类目录树 1:1 镜像与边界修正
- 端到端验证 `影视/电视剧/国产剧/侠客行（2001）/Season 1/E01.mkv`：三层分类目录逐层按 A 库原名建，只有作品主目录按 `·` 约定命名，`Season 1` 同名镜像；`MirrorMedia.category_path` 记录完整分类链；共享分类链的多部作品复用缓存，不重复建目录。
- 修边界 bug：作品目录**不带年份 / `{tmdb-}` 标记**且直属子目录是 `Season N` 时，旧 `_discover_works()` 会继续下探，把 `Season 1` 当成作品主目录、分类链多一层。新增 `_SEASON_DIR` 正则与 `has_season_child` 判断，此类目录直接判定为作品主目录。
- A 库全程只读：所有 `create_dir` 的 parent 必须是 B 库目录；分类目录不改名；复制目标是最内层的 `Season 1` 而非分类目录。

### 四、升级与验证
- 升级后**点一次「立即同步」**即可自愈历史空目录（按 B 库实际内容补复制，不动已存在文件）；想按 B 库实际内容重建面板，点顶部「刷新」。无需清库、无需重建数据库、无需重新复制已存在的整部作品。
- 新增 `tests/test_v087_empty_b_dir.py` 17 项：空目录自动补复制 / 有文件绝不重复制 / 列目录失败与分页不全不误判 / 深层目录不误判为空 / 名称不符但文件数够时不重置 / 复用同一行且 A 原名不变 / 可信空批次清面板 / 未显式放行仍跳过 / 无目录不信任 / 批次失败不完整断点都不清 / 三入口统一 allow_empty / 定时扫描不对账 / 多层分类链逐层原名 / 只改作品目录与单集 / Season 子目录边界 / 无年份作品分类链完整 / 多作品共享分类链不重复建。
- `tests/test_v080_mirror_and_interaction.py` 的假 API 补齐「复制落地后 B 库真的有文件」并重置类变量，避免 v0.8.7 的存在性核对被旧假数据误判。
- 全套 `201 passed, 1 warning`；前端 `vite build` 通过。真实环境待验证：递归列目录在超多季 / 超多文件作品上的接口耗时、网盘对重名后缀的实际行为。

---

## v0.8.6 (2026-11-18)

按用户实测反馈「目前看流程正常，能推送，不过影视文件的审核没通过，需要对剧集中的每一集、电影文件也进行改名，将中文翻译为英文剧名」实现 B 库文件级英文改名。
用户同时划死边界：**A 库仍然保持不动，B 库影视文件夹还是保持之前的命名，本次是对单集、单电影文件进行命名优化，别忘记，仍然需要和 A 库保持映射关系，便于后期对比变量更新**。
选项确认：英文名来源 = TMDB en-US + AI 兜底；命名格式 = 只换中文片名段；存量处理 = 立即批量改名。

### 一、边界（红线）
- 只对 **B 库单集 / 单电影文件** 调 `POST /userres/v1/file/rename`；A 源头库全程只读，绝不传 A 的 `fileId`。
- B 库分类目录（A 原名 1:1）、影视主目录（v0.8.3 起 `·` 命名）、季目录一律不改名。
- 不重新复制文件、不写 Resource、不建分享、不入推送队列；改名赶在「扫描入库」之前完成。
- 解析不到英文剧名时**绝不瞎猜改名**，作品列入「英文名待处理」清单。

### 二、命名规则与英文名来源
- 新增 `backend/app/file_naming.py` 纯函数：`work_core_title()` / `sanitize_english_title()` / `english_file_name()` / `with_conflict_suffix()`；无网络、无 DB，可单测。
- 只替换中文片名段，季集号 / 年份 / 画质 / 编码 / 压制组 / 扩展名（含其中文）原样保留。示例：`大醉侠 第04集 2160p.WEB-DL.H.265.mkv` → `The Drunk Hero 第04集 2160p.WEB-DL.H.265.mkv`。
- `_resolve_english_title()` 每部作品只解析一次：优先取已识别 `Resource.original_title`（纯 ASCII），再走新增的 `tmdb.english_title(id, type, db)`（`language=en-US`，只接受纯 ASCII 且含拉丁字母的 `name` / `title` / `original_*`），最后用 `ai_assist.translate_title()` 翻译中文剧名；缓存到 `MirrorMedia.english_title` / `english_source`（`tmdb` / `ai` / `resource`）。日韩作品 `original_*` 非拉丁文字会被判为不可用，交给 AI。
- 目标名先过网盘非法字符清洗（`* ? : / \ " < > |`）；B 库已有同名且非同一 `fileId` 时追加 ASCII `-vN`，避免 A 库内容更新后的新旧版本互相覆盖。
- 文件之间沿用 `file_idle_seconds`（硬下限 0.5 秒）节流，与复制 / 建目录共用同一风控口径。

### 三、A→B 映射不丢（本版核心约束）
- `MirrorFile.file_name` 永远保存 **A 库原名**，改名结果另存新增的 `target_name`；增量比对仍只用 `source_file_id + fingerprint(size:utime)`，与两边文件名无关，因此改名不影响「后期对比变量更新」。
- 新增 `rename_status`（`""` / `done` / `failed` / `skipped_ascii`）与 `rename_error`，失败原因落库、刷新页面仍在。
- `_b_names()` / `_lookup_b_file()` 让「重试前核对 B 库」与 `backfill_target_file_ids()` **优先按 `target_name` 反查** B 库 `fileId`；改名后下一轮不会被误判为文件不存在、也不会重复复制。
- 只处理 `status == "synced"` 的文件；已改过且目标名未变直接跳过（幂等）。

### 四、入口与管理页
- 新增开关 `MirrorRule.rename_files`（默认开）；`POST /api/mirror/run`、定时镜像、`POST /api/mirror/tasks/poll` 都在扫描入库前调用 `rename_mirror_files()`。
- 新增 `POST /api/mirror/rename?force=`：存量已同步文件一键批量改名；`force=true` 重新解析英文剧名；**即使关闭自动开关，手动按钮仍可用**。
- `/api/mirror/run` 返回 `rename_enabled` 与 `rename{works, renamed, skipped, ascii, untranslated, failed, errors, untranslated_works, idle_seconds}`；`/api/mirror/status` 回显 `rename_errors` / `untranslated_works` 与 `files_renamed` / `files_rename_failed` / `works_without_english` 计数。
- 管理页新增「B 库单集/电影文件英文改名」开关、「按英文规范改名」与「重新解析英文名并改名」按钮；同步结果提示带「改名 N · 待翻译作品 N · 改名失败 N」；状态卡新增「英文名待处理 / 改名失败」两张清单。
- `db.py` 补列：`mirror_rules.rename_files`、`mirror_media.english_title/english_source`、`mirror_files.target_name/rename_status/rename_error`；老库升级自动 ALTER，不用重建数据库。

### 五、测试与交付
- 新增 `tests/test_v086_file_rename.py` 18 项：只换中文片名段、ASCII/非法英文名不改、TMDB 优先且映射保持、AI 兜底、无英文名不改并列清单、幂等、改名后仍能反查 B 库且不重复复制、单文件失败隔离并持久化、冲突加 `-vN`、文件间风控间隔、**A 库与 B 库目录名零改动**、关闭开关时手动端点仍可用、开关与状态回显、版本与 UI 契约、**改名整体异常不阻断手动 / 定时同步管线（2 项）**。
- 全套 `184 passed`，前端 `vite build` 通过。
- **隔离保护**：新增 `main._safe_rename()`，手动 / 定时同步 / 任务轮询里的改名整体异常只记日志与 `rename.errors`，绝不阻断已完成的复制与后续入库识别推送；两项专项测试锁死这条边界。
- **升级提示**：存量已同步文件不会自动改名（改名随后续同步 / 任务轮询发生），升级后到「镜像同步」点一次「按英文规范改名」即可，不重新复制文件。真实网盘 `rename` 对中文→英文文件名的接受度与审核结果仍需真实环境确认。

---

## v0.8.5 (2026-11-18)

按用户三轮明确口径接通「镜像完成 → 刷新入库 → 识别 → 只推本轮识别成功影视」链路：
「同步完没有自动触发资源库扫描入库」→「扫描入库完，识别成功（获取到海报），自动按照 tg 分发规则推送」→
「**镜像完成不是群推送**，镜像完成，触发资源库入库刷新，识别影视，识别成功的影视就推送」。

本版**反转**了 v0.8.0~v0.8.4 写死的 V2.0 第十章「镜像完成不自动推送 Telegram」红线，
但没有放开两道防群推闸门（见第二节）。

### 一、`auto_identify` 是死配置（头号根因）
- `MirrorRule.auto_identify` 在 v0.8.0 就已经存库、`/api/mirror/config` 也回显，前端初始 state 里也有 `auto_identify: true`，但 `mirror_run()` 与 `_scheduled_mirror()` **从未读取它**；保存开关后不产生任何运行时效果。
- 前端「立即同步」硬编码 `runMirror(false)` → `/mirror/run?post_identify=false`，永不扫描；「同步并识别（不推送）」传 `true`，但 `mirror_run()` 强制 `publish_after_identify=False`。
- 定时镜像 `_scheduled_mirror()` 只有 `sync_rule()` + `backfill_target_file_ids()`，完全没有扫描。
- 修：新增共享管线 `_mirror_post_sync_pipeline()`，手动与定时两条路径统一调用；开关默认开启。

### 二、管线边界（防群推红线）
- 管线只做三件事：① `run_scan(_effective_scan_root(), resume=False)` 刷新 B 库入库；② `_post_scan_identify(db, publish_after_identify=True, limit=500)` 识别待识别资源；③ 本轮识别成功且有海报的影视按启用规则入队（`_enqueue_after_identify` → `_enqueue_ready`）。
- **不调用** `process_change_events(db, publish=True)`：v0.8.0~v0.8.4 刻意积压了 `queue_id=0` 的内容变化事件，在此放开会一次性批量补推历史更新。返回值里的 `changes_pending` 只是只读计数。
- 不做对账删除、不重建/取消分享、不回溯推送历史已识别资源；`_enqueue_ready` 的作品级去重保证同作品同频道已推过就不再入队。
- 内容变化更新仍由用户主动的「立即扫描 / 重新扫描库 / 定时扫描」按既有机制处理。

### 三、手动与定时两条路径
- `/api/mirror/run` 的 `post_identify` 改为 `bool | None = None`：不传 = 遵循 `auto_identify`；`false` = 仅同步；`true` = 开关关闭也强制执行。
- 返回 `pipeline_enabled` 与 `pipeline{files, directories, complete, root_source, identify, queued, changes_pending}`。
- 定时镜像按开关走同一管线；`last_message` **追加**「[自动入库] …」而不是覆盖，保留 v0.8.4 的文件级复制错误诊断。

### 四、管理页
- 镜像设置新增「同步后自动入库识别并推送」开关；同步状态「生效配置」回显 `auto_identify`。
- 按钮改为「立即同步」（遵循开关）、「强制入库识别」、「仅同步」、「继续轮询任务」，删除语义冲突的「同步并识别（不推送）」。
- 同步结果提示追加「自动入库 N 文件 · 识别成功 N · 入队 N · 内容变化待处理 N」。
- `/api/mirror/config` 的 `note` 修正：v0.8.1 文案仍写 `* 命名` 与「镜像完成不会自动推送 Telegram」，两处均已过时。

### 五、测试与交付
- 新增 `tests/test_v085_post_sync_pipeline.py` 14 项：手动/定时两条路径都读开关、显式 `true/false` 覆盖开关、识别失败与无海报不入队、历史已识别资源不回溯推送、同作品已有队列项不重复入队、**历史 `queue_id=0` 事件保持原样且 `process_change_events` 零调用**、管线不做对账、配置与版本口径一致。
- 全套 `166 passed`，前端 `vite build` 通过。
- **行为变更提示**：升级后默认开启自动链路。若不想让镜像同步触发推送，到「镜像同步」关闭开关，或只用「仅同步」按钮；`queue_id=0` 的历史内容变化事件**不会**被本版补推。

---

## v0.8.4 (2026-11-18)

按用户实测反馈「**影视目录的文件夹创建正常、命名正常，对应的文件没有镜像过去**」修复文件复制链路。v0.8.2 解决的是「建目录失败看不到原因」，本版解决的是「目录建好了但文件没过去、且反复同步无效」。

### 一、未完成的文件被永久跳过（头号根因）
- `sync_rule()` 旧版把该作品下**所有** `MirrorFile` 塞进 `existing = {(source_file_id, fingerprint): row}`，只要键命中就判 `unchanged` 跳过。上一轮 `copy_file` 抛异常或任务未确认时留下的是 `copying` / `failed` / `unknown`，文件根本没到 B 库，下一轮却被当成「已存在」，永远不再发起复制。
- 修：只有 `status == "synced"` 才算已到位；`pending` / `copying` / `failed` / `unknown` 归入 `RETRYABLE_FILE_STATUS`，**复用同一行重新发起复制**（不新建行，避免唯一键冲突与历史留痕膨胀），统计新增「重试」计数。
- **防重复复制**：重试前先只读列 B 库该作品目录（含直属子目录），按「归一化文件名 + 大小」核对。任务其实已成功、只是轮询超时的，直接回填 `target_file_id` 并标 `synced`，不再重发 `copy_file`——需求明确禁止「先删旧再复制」。`backfill_target_file_ids()` 同步复用这个核对逻辑，失败/未确认记录若已在 B 库存在也会纠正为 `synced`。

### 二、复制任务失败被误记成「待确认」
- 前端 JS `index.4998cfbcbd.js` 的轮询函数 `p5` 给出确切语义：`status=2` 且 `detail.code !== 0` → 失败（如 157 云盘空间不足）；`status=3` → 失败；只有 `status=2` 且无错误码才是成功。旧版 `get_task_status()` 丢掉了 `data.detail`，且只认 `status=2` 为完成，其余一律记 `unknown`。
- 修：`get_task_status()` 原样返回 `detail`；新增 `_task_outcome()` 区分 `done` / `failed` / `pending`；失败时把 `MirrorTask.status`、`MirrorFile.status` 标 `failed` 并写入可读错误（含 `detail.msg`），下一轮自动重试。`poll_pending_tasks()` 也按同一语义处理。

### 三、文件级错误是新的诊断黑洞
- v0.8.2 只把 `create_dir` 的真实报错落库回显；文件复制的 `stats["errors"]` 仅存在于 `POST /api/mirror/run` 的**那一次响应**里，刷新页面即丢，`rule.last_message` 也只有计数，界面会显示「已同步」而 B 库是空目录。
- 修：`MirrorFile.error` / `MirrorTask.error` 持久化；`last_message` 追加「复制失败 N · 首个文件错误：…」；`GET /api/mirror/status` 新增 `counts.files_failed` / `files_copying` / `files_unknown` / `files_retryable` / `tasks_failed` 与 `file_errors[]`（最近 50 条，含作品名、文件名、任务 ID、真实报错）。管理页镜像同步新增「文件复制失败 / 待重试」清单、状态卡新增失败计数，「立即同步」与「轮询任务」提示显式带出失败数与首个错误。还有未完成文件时不把作品标 `synced`。

### 四、列目录请求体与分页判定对齐抓包
- `w.har` 中网页端 `get_file_list` 的真实请求体是 `{pageSize:50, orderBy:3, sortType:1, parentId, page:0}`，响应 `data.total` / `data.list`。旧版自造了 `needSubFolderStat` 字段、发 `pageSize:100`，且用 `page*100 >= total` 判完整——接口实际每页返回不足 100 条时会提前结束并误判为「分页不完整」，整轮镜像直接中止或漏掉文件层条目。
- 修：请求体按抓包对齐（`LIST_PAGE_SIZE=50` + `orderBy` / `sortType`），分页改用「累计条数 vs total」判定，短页但 `total` 更大时明确判不完整。`scanner.py` 的分页不受影响，仍用独立逻辑。

### 五、接口取证口径更正（重要）
- 复查原始 `w.har`：54 条请求里**没有任何一次 `copy_file` 真实请求**，抓到的写请求只有 `create_dir` / `rename` / `delete_file` / `get_task_status`。`copy_file {fileIds, parentId} → data.taskId` 来自前端 JS 的 `onCopyTo` 调用代码，**不是抓包响应**。
- 因此把 `copy_file` 从「抓包已确认」改标为「仅前端 JS 契约，待 HAR 复核」，并加入 `UNVERIFIED_ENDPOINTS`。这与用户现象一致：抓包确认的建目录能用，只有 JS 契约的复制接口需要进一步取证。**请在网页端手动执行一次「复制到」并抓包**，重点看 `data.taskId`、错误码（如 157 空间不足）与鉴权差异。

### 六、验证与仍待确认
- `pytest tests -q`：**152 passed, 1 warning**（新增 `tests/test_v084_file_copy.py` 10 项；更新 `test_v081_mirror_dirs.py` 的待验证接口契约断言）；`vite build` 通过；`py_compile` / `git diff --check` 通过。
- **仍待用户实测**：升级后点「立即同步」，确认 B 库空目录里的文件被补上；看「文件复制失败 / 待重试」清单是否给出 `copy_file` 或 `get_task_status` 的真实报错；若全部卡在 `copy_file`，按第五节补抓 HAR。投稿分享转存 HAR、`QueueItem` / `DistributionEvent` 的 `source/mode` 数据层隔离仍未闭环，**本版不宣称全面功能实现**。

---

## v0.8.3 (2026-11-18)

按用户「库的复制已实现」后的三条实测反馈修复：**命名字符、扫描根目录、日志膨胀**。

### 一、网盘拒绝 `*`，B 库影视主目录改用 `·`
- 真实环境 `POST /userres/v1/file/create_dir` 返回 `code=141 msg=文件夹名包含非法字符: *`，v0.8.2 只能降级成不带标记的标准影视名。本版按用户要求把标记换成 `·`：`大醉侠 (2002) {tmdb-135646}` → `大·醉·侠 (2002) {tmdb-135646}`（中文按字符、英文按单词，括号年份与 TMDB 标记原样）。
- `standard_media_name()` / `normalize_work_name()` 同时识别 `*` 与 `·`：旧带 `*` 名称就地转换；v0.8.2 已降级创建的**无标记目录被归一化命中复用**，不重复建目录、不改存量目录名；A/B/无标记三种写法仍落到同一作品键。
- 分类目录改为直传 A 库原名，原名里带中点也不会被剥离（新增 `动画·电影` 1:1 回归）。
- 保底降级仍在：只有网盘连 `·` 也拒绝时，才自动降级为无标记标准影视名，`created_fallback` 与真实报错照常落库回显。

### 二、资源库定时配置不再有目录 ID 输入框
- 删除「扫描根目录（自动引用，无需填写）」输入框与 `rootSourceLabel`，界面只保留一句「自动引用镜像同步 B 库 ID」说明。
- `ScanConfig` 移除 `root_id`，保存扫描计划不再把 B 库 ID 写进 `ScanTask` 形成第二份配置；扫描（含定时任务）统一由 `_effective_scan_root()` 实时取值，改 B 库 ID 立即生效。

### 三、日志保留策略（自动清理 + 手动清空 + 容器限额）
- 此前 `AuditLog`、`ResourceScanBatch`、`MirrorTask`、`ResourceVersion`、`DistributionEvent`、`QueueItem`、`MirrorFile(superseded)` 全部只增不减，SQLite 与界面列表会无限膨胀。
- 新增 `log_retention.py`：保留天数（默认 30）+ 每表条数上限（默认 5000）+ 自动清理开关；管理页「操作审计 → 日志保留与清理」可配置、可「立即清理」、可「清空全部日志」（需二次确认）。
- **绝不误删业务关键行**：running 扫描批次、pending/running 镜像任务、`queue_id=0` 待处理分发事件、pending/retry/review 队列项、非 `superseded` 文件映射一律保留；资源、目录映射、分享链接不在清理范围。
- 自动清理在启动时与每天 03:30（Asia/Shanghai）各跑一次；两个 compose 文件给 stdout 日志加 `json-file` 限额 `3 × 10MB`。
- 配置走运行时加密落库（`log_retention_days` / `log_max_rows` / `log_auto_prune`），不新增 `.env` 项；API：`GET /api/logs/overview`、`POST /api/logs/config|prune|clear`。

### 四、回归
- `pytest tests -q`：**142 passed**（新增 `tests/test_v083_fixes.py` 11 项；`test_v082_mirror_diagnostics.py` 补 1 项 `code=141` 回归；v0.8.0 / v0.8.1 / v0.8.2 命名契约断言同步更新）。
- `vite build` 通过、`py_compile` 通过、`git diff --check` 通过。
- **仍待真实环境确认**：`·` 是否被该网盘接受、已复制库的无标记目录是否按预期复用、日志自动清理在真实数据量下的耗时；投稿转存 HAR 与 `QueueItem`/`DistributionEvent` 的 `source/mode` 数据层隔离仍未做。

---

## v0.8.2 (2026-11-18)

按用户实测「镜像库同步并没有成功」修复：42 部作品全部 `missing` / `blocked` / 文件数 0，而界面只显示通用文案「请在 B 库该父目录下建立此目录后重新同步」，**真实错误被吞掉，无法判断根因**。

### 一、诊断黑洞（定位根因的前提）
- `mirror_status()` 原先把 `missing_dirs[].tip` **硬编码**成通用建议，覆盖了 `sync_rule()` 里记录的 create_dir 真实报错。现在 `MirrorDir` 新增 `error` 列，失败原因持久化并原样返回，管理页「待创建目录 → 失败原因」直接显示接口报错。
- `_target_children_cached()` 原先把「B 库访问失败」吞成「B 库是空目录」，接着对每部作品各调一次 create_dir 全部失败。现在异常向上抛，且 `sync_rule()` 开头**预检 B 库根目录可读**：不可读时直接中止并给出含目录 ID 与接口错误的明确提示，不会留下 42 条无意义的 missing。
- 同步结果 `last_message` 带首个建目录错误；`/api/mirror/status` 回显 `config`（`auto_create_dirs` / `star_naming` / A/B 目录 ID / 空闲时间），一眼看出自动创建是否被关掉。
- 容器日志新增 `[mirror]` 建目录失败与同步异常记录，便于 `docker compose logs` 排查。

### 二、让同步真正能成功
- 网盘若拒绝带 `*` 的目录名，`create_dir` 失败后自动用**不带 `*` 的标准影视名**重试一次；成功后整轮后续作品直接用标准名，并记录 `MirrorDir.status=created_fallback`、`MirrorMedia.title_mirror` 更新为实际名称。归一化键不变，下次同步仍能复用同一目录。
- 关闭自动创建时，失败原因明确写「自动创建目录已关闭：请在管理页开启」，不再伪装成「请手工建立」。
- `auto_create_dirs` 为 NULL 时按开启处理（列本身 NOT NULL，仅防御异常数据；也已排除 NULL 是本次根因）。

### 三、回归
- `pytest tests -q`：**130 passed**（新增 `tests/test_v082_mirror_diagnostics.py` 6 项：真实报错持久化与回显、B 库根目录不可访问明确中止、带 `*` 名称失败自动降级、开关关闭原因、配置回显、生效配置一致性）。
- `vite build` 通过。

### 升级后请做一次诊断
点「立即同步」，看「同步状态 → 生效配置」与「待创建目录 → 失败原因」：
- 提示 **B 镜像库根目录不可访问** → 核对镜像设置里的 B 库目录 ID 与光鸭登录态；
- 提示**目录名非法 / 名称不允许包含星号** → v0.8.2 已自动降级为不带 `*` 的标准名，同步应能完成；
- 提示其它接口报错（权限、会员、限频等）→ 把容器日志 `docker compose logs --no-color --since 1h ya-tg > ya-tg.log` 发来，按真实报错处理。

---

## v0.8.1 (2026-11-18)

按用户实测反馈修复 A/B 镜像与分享粒度，并把抓包 HAR 里已确认的写接口落地。HAR 原始凭证已脱敏，取证结论见 `docs/光鸭接口取证.md`。

### 一、分类目录识别与 B 库 1:1 镜像
- `mirror._discover_works()` 不再把 A 根目录的直接子目录一律当作影视主目录：结合目录名线索、子树是否直接含视频文件、是否像作品名，逐层找到真正的作品主目录，并记录分类目录链 `category_path`。
- B 库按 A 库结构自动建目录：
  - **分类目录**（如「动画电影 / 国产电影 / 欧美电影 / 日韩电影」）按**原名** 1:1 创建，不改名、不加 `*`；
  - **影视主目录**按 `*` 约定创建；
  - **子目录**（如 `Season 1`）按原名创建。
- 新增 `mirror_dirs` 表记录每个目录的 A→B 映射（`kind=category/work/season`、`status=mapped/created/missing`）。`GET /api/mirror/status` 返回 `dirs` 与 `missing_dirs` 清单，管理页可直接查看。
- 开启「B 库缺目录自动创建」时不再要求手工预建；关闭时只登记 `missing_dirs`、作品标 `blocked`（**不是 failed**），不再把 47 部作品一次性打成失败。旧版误判留下的空 `failed` 记录会被清理。

### 二、B 库 `*` 命名规则纠正
- 用户纠正：**括号里的年份与 TMDB 标记不加 `*`**。
  ```
  大醉侠 (2002) {tmdb-135646}  →  大*醉*侠 (2002) {tmdb-135646}
  ```
- 括号段整体保持原样，括号前只保留一个空格；纯英文片名改为按「单词」插 `*`（中文仍按字符），避免生成 `S*p*i*d*e*r` 式乱码目录名。

### 三、HAR 取证后的写接口
`GuangYaWebFiles` 新增三个已确认写接口，URL / 请求体 / 响应体均有契约测试锁死：

```http
POST /userres/v1/file/create_dir   {"parentId":"...","dirName":"..."} → data.fileId
POST /userres/v1/file/rename       {"fileId":"...","newName":"..."}
POST /userres/v1/file/delete_file  {"fileIds":["...","..."]}          → data.taskId
POST /userres/v1/get_task_status   {"taskId":"..."}                     → data.status（2 = 完成）
```

- `delete_file` 支持批量 `fileIds`，返回 `taskId` 后需轮询；没有 `taskId` 直接报错，不假装成功。
- 前端 JS bundle 里还出现 `move_file` / `recycle_file` / `clear_recycle_bin` 等路径，但**没有请求体证据，不实现、不猜参数**。
- 需求文档 8.6 的待验证清单现在只剩「投稿分享转存（TgtoDrive 侧接口，未抓包）」。

### 四、文件操作风控节流
- `MIN_FILE_IDLE_SECONDS = 0.5`：来自官方 OpenAPI v1.3 §10 写接口 2 次/秒限频，是**硬下限，人为不可调低**，前端 `min` 与后端 400 校验双重约束。
- `MirrorRule.file_idle_seconds` 可在管理页调高（默认 1.0 秒，上限 60 秒），每次 `copy_file` / `create_dir` / 变化更新前按 `max(配置值, 0.5)` 等待；`get_task_status` 另有 0.2 秒轮询保护。
- 任务轮询改为**批量**提交后再统一等待，避免逐条阻塞放大同步耗时。

### 五、电影按作品目录分享
- 旧逻辑只有 `media_type == "tv"` 才计算 `share_dir_id`，电影一律 `share_scope="file"`，导致电影分享的是单文件而不是对应目录。
- `series_dir_info(..., media_type="movie")` 现在把**电影主目录 = 直接父目录**，电影也走目录分享；若电影文件直接散放在分类目录下（父目录名像分类且无年份 / TMDB 标记），退化为按文件分享，避免误把整个分类目录当成一部作品。
- `create_share()` 与 `publisher.ensure_share()` 改以 `bool(dir_id)` 判断是否按目录分享，不再按媒体类型写死；识别与入队时若缺 `share_dir_id` 会现场补算。

### 六、扫描设置合并到资源库
- 删除侧栏独立的「扫描设置」页与独立 section；扫描计划 / 断点 / 批次保护折叠进「资源库」，页头新增「立即扫描」「扫描计划」按钮。
- **扫描根目录 ID 不再重复填写**：`_effective_scan_root()` 优先引用「镜像同步」里已填的 B 库目录 ID（`ScanTask.root_id` / `.env` 仅作兜底）；`GET /api/scan-task` 返回 `effective_root_id` 与 `root_source`，前端只读展示。`scan_now` / `rescan_library` / 定时扫描统一走有效根目录。

### 七、诊断与界面
- `POST /api/mirror/run` 异常不再吞成笼统 502：保留异常类型与消息并写入 `MirrorRule.last_status=failed` / `last_message`。
- 同步状态新增「目录已映射 / 自动创建目录 / 待创建目录 / 文件操作下限」计数，新增「待创建目录」「目录映射清单」两张表与影视清单的「分类目录」列。
- 同步结果提示合并成一条，不连弹多个提示。

### 八、取证工具随仓库交付
- 新增 `scripts/har_extract.py`：HAR 脱敏提取、base64 响应解码、前端 JS 接口路径汇总、凭证残留自检；`.gitignore` 禁止 `*.har` 入库，避免原始登录凭证外泄。补抓「投稿转存」的步骤见 `docs/光鸭接口取证.md` 第七节。

### 回归
- `pytest tests -q`：**124 passed**（v0.8.0 为 110，新增 `tests/test_v081_mirror_dirs.py` 14 项）。
- `vite build` 通过。旧用例 `test_mirror_missing_target_dir_is_skipped_not_created` 改为 `..._when_auto_create_off`，显式关闭自动建目录后断言 `missing_dirs` 与 `blocked`。

---

## v0.8.0 (2026-11-18)

按《tg-gy 最终版项目开发需求文档》的全局架构推进第二批：A/B 镜像同步、扫描批次表、内容变化检测、完结校验与手动打标、用户互动/TgtoDrive 对接。同时把扫描设置页改成文档要求的时间间隔按钮。

### 需求2：A 源头库 → B 镜像库
- 新增 `mirror_rules` / `mirror_media` / `mirror_files` / `mirror_tasks` 四张映射表；A/B fileId 不同，映射单独保存，绝不把 A 的目录链或 `share_dir_id` 搬到 B。
- `GuangYaWebFiles` 实现文档第八章**已确认**的两个接口，URL 与请求体按原文实现并有契约测试锁死：
  ```http
  POST /userres/v1/file/copy_file      {"fileIds":["..."],"parentId":"..."} → data.taskId
  POST /userres/v1/get_task_status    {"taskId":"..."}                     → data.status
  ```
  只把 `status=2` 当完成；其它取值原样记录 `raw_status` 并标记 `unknown`，不猜成功/失败，可继续轮询。
- 增量同步：只读遍历 A 库 → 按作品主目录分组 → 用 `size:utime` 指纹比对 → 只复制新增/变化文件 → 轮询任务 → **单部影视完成即提交入库**。身份用 `TMDB ID + 类型 + 季 + 集`，不明确时只允许新增、禁止自动替换。
- 禁止「先删旧再复制」：内容变化时旧 B 文件保留并标记 `superseded`，只新增复制记录。
- **B 库带 `*` 命名**：`normalize_work_name()` 剥掉 `*` 与 `{tmdb-}` 标记，A/B 同剧归一到同一作品键；`standard_media_name()` 保证资源库、分享标题、TG 推送一律用标准影视名。带 `*` 的目录名只存在于网盘。
- **镜像完成禁止自动推 TG**：镜像模块不写 `push_queue`、不调用任何入队函数；`POST /api/mirror/run?post_identify=true` 可额外做 B 库扫描+识别，但强制 `publish_after_identify=False`。
- 管理页新增「镜像同步」：A/B 目录 ID、间隔按钮、启停、立即同步、继续轮询、同步状态与影视映射清单。

### 扫描批次保护与扫描设置
- 新增 `resource_scan_batches` 表；`run_scan()` 每轮写批次（`complete` / `resumed` / `seen_json` / `reconcile_allowed`）。`_reconcile_library()` 传入 `batch_id` 时以批次表为**唯一权威信号**，批次未授权一律跳过。管理页新增批次列表。
- 扫描周期改成按钮预设（10/15/30 分钟、1/2/6/12 小时、每天），保留可折叠的高级自定义 Cron。

### 需求3：完结/追更手动修正与内容变化
- `Resource.finish_locked` + `POST /api/library/batch` 新 action `set_finish`：按整部作品标记 `ended` / `returning` / 清空 / `auto`（恢复自动）。手动标记后 TMDB 自动回填与手动重新识别都不得覆盖，卡片角标显示「·手动」。批量操作条与作品卡下拉均可操作。
- 新增 `GET /api/library/finish-check`：TMDB 季数/已播出集数与本地实际文件比对，给出「应有集/本地集/缺集/建议」，只读核对不自动改状态。`tmdb._normalize()` 补回 `number_of_seasons` / `number_of_episodes` / `tmdb_status`。
- 修复一个会导致裂卡的真实缺陷：`_apply_identification()` 在缺目录链时会把「Season 1」写进 `share_dir_name`，旧作品键直接拿它当作品名，导致识别前后同一部剧裂成两张卡。`work_group_key()` 现跳过季目录/通用目录名。
- 新增 `resource_versions` / `distribution_events`：扫描发现 `size/utime` 变化时只留版本，**不重建分享、不取消再创建**；同一批变化按作品合并成唯一一条「内容更新」事件，只重置一条队列项为待发送，不逐集重复推送。无变化不产生事件。

### 需求4：用户互动 / TgtoDrive
- 新增 `user_contributions` / `media_requests`；新增 Bot `getUpdates` 轮询（Telegram 官方 API，无需抓包）：`/want`、`/contribute`、`/donate`、`/help`。管理页「互动中心」可配置、测试连接、审核投稿、手工添加想看关键词、设置打赏。
- TgtoDrive OpenAPI 按本地实证结构实现「GET 全量 → 内存合并 → POST 回写 → 回读校验」，Bearer 鉴权；只按幂等 id `kw_<tmdb>_<season>` 新增/更新 `source=ya-tg` 的规则，同 id 属其它来源直接返回 `conflict` 且一次都不写回，绝不覆盖原有关键词与其它项目规则。
- 凭证类配置继续走运行时加密落库（`tgto_*` / `contribution_*` / `donation_*`），已登记到 `DEFAULTS`，不会出现保存被静默忽略。

### 明确边界：仍待验证的光鸭接口（文档 8.6，未实现、未猜测）
创建文件夹、重命名文件夹（B 库自动加 `*`）、删除 B 库文件、删除/批量删除目录、投稿分享转存。因此：B 库影视目录需先在光鸭网页端手工建立；镜像不会自动改名或删旧文件；投稿只登记与审核，不自动转存入库。管理页会直接列出这份待验证清单。

### 升级提示
- 升级后在「镜像同步」填 A/B 目录 ID 并保存，再到「资源库」点一次「重新扫描库」补齐批次与目录链。
- 「完结校验」是只读的；确认识别有误后在作品卡「完结状态」下拉里手动标记并锁定。

### 测试
- 新增 `tests/test_v080_mirror_and_interaction.py`（15 项）：带 `*` 归一化、身份置信度、间隔预设往返、复制接口契约、增量/无变化/变化/未知状态/缺少目标目录、镜像不推 TG、手动完结锁定与解锁、批次权威、变化事件合并与分享复用、完结校验、TgtoDrive 合并与冲突、Bot 投稿/想看。
- 全套 **110 项通过**，前端 `vite build` 通过。

### 镜像
```bash
docker pull abson2/ya-tg:v0.8.0
```

---

## v0.7.1 (2026-11-18)

需求3 补漏修复版：把 v0.7.0 里「代码写了但链路没通」的四项真正接起来，并按最终版需求文档第 12.2 节补齐扫描批次保护。**本版仍不含** A/B 镜像同步（需求2）与投稿/TgtoDrive 对接（需求4第二批），这两项按全局架构分阶段推进，不因本版发版而标记为完成。

### 用户反馈的四项修复
- **推送集数/大小显示不对**：根因是 `publisher._build_message()` 只用 `share_dir_id` / `tmdb_id` 找同剧成员，而这两个字段只有识别成功才写入，未识别单集被整批漏算；且集号只读 DB 的 `season/episode`，同样识别后才有。现统一改用 `utils.work_group_key()` 圈定同剧全部单集，用 `is_video_file()` 排除字幕/nfo，用 `work_episode_numbers()` 在 DB 缺字段时从文件名现场解析季集号。36 集里仅 1 集识别成功的复现场景，由「共 1 集 / 2.0GB」修正为「共 36 集 / 72.0GB」。
- **作品键裂卡**：`normalize_work_name()` 的括号噪声正则补上 `{}`，并去掉裸 `tmdb-` / `imdb-` 标记。A/B 库标准目录名 `侠客行 (2001) {tmdb-135646}` 与已识别单集的 `share_dir_name=侠客行` 现归一到同一作品键，不再一部剧拆成两张卡。展示（`/api/library/series`）、分享、推送三处共用 `work_group_key()`，且只依赖扫描阶段就有的字段。
- **完结标签没实现**：`tmdb._normalize()` 与 `Resource.finish_status` 在 v0.7.0 已有，但存量已识别资源从未回填。新增 `_post_scan_identify()` 的回填分支：对已识别、有 `tmdb_id`、缺 `finish_status` 的剧集回查 TMDB detail 补 `ended/returning`，按 TMDB ID 去重请求、限量 60 条/轮。TMDB 未配置/401 时静默降级，不阻断扫描。
- **扫描后不自动识别 / 没有重新扫描库**：
  - `POST /api/library/rescan` + 资源库页「重新扫描库」按钮：全量重扫 → 对账移除 B 库已不存在的影视（删除前先取消分享、清队列）→ 自动识别 → 完结回填，一次返回统计。前端对全量重扫单独放宽请求超时到 180 秒。
  - `POST /api/scan-now` 增加 `auto_identify` 参数，扫描完成后默认自动识别；Cron 定时扫描改走 `_scheduled_scan()`，同样自动识别，但**不做对账删除**，避免接口抖动误删。
  - `scanner.run_scan()` 新增返回 `seen_file_ids` 与 `complete`，供对账判断。

### 扫描批次保护（最终版需求文档 12.2）
- `_reconcile_library()` 强制三重条件同时满足才允许清理：全量扫描（非断点续扫）、全部分页读取成功、`seen_file_ids` 非空；否则返回 `skipped` 原因（`incomplete_pages` / `resumed_incomplete_seen` / `empty_seen`），一条数据都不删。
- `run_scan()` 按目录累计实际抓取条数与接口 `total` 比对；覆盖「total=150、第 2 页提前返回空页」这类 `page*100 >= total` 但实际少读数据的隐蔽不完整批次。API 异常则直接抛出，调用方走 502，不进入对账。

### 镜像同步的推送隔离预埋
- `_post_scan_identify()` 增加 `publish_after_identify` 开关：镜像同步复用识别管线时传 `False`，识别与回填照常进行但绝不自动入队推送 TG，满足「镜像完成禁止自动推送」的隔离要求。
- **边界说明**：这是调用层开关，不是完整隔离。数据层独立状态（`QueueItem` 的 source/mode）、`mirror_rules` / `mirror_media` / `mirror_files` / `mirror_tasks` / `resource_scan_batches` 映射表、网页端 `copy_file` / `get_task_status` 客户端均尚未实现；8.6 列出的创建文件夹、重命名、删除、投稿转存接口仍待抓包验证，不会凭猜测落地。

### 升级提示
- 升级后建议在资源库点一次「重新扫描库」：全量重扫会补齐旧资源缺失的目录链，自动回填完结状态，并清理网盘里已删除的旧记录。
- 该按钮会取消被移除资源的光鸭分享链接，属预期行为；若本轮分页不完整，按钮会显示「对账已跳过」并保留全部数据。

### 测试
- 新增 `tests/test_v071_scan_pipeline.py`（9 项）：`{tmdb-}` 归一化、36 集整剧统计、非视频文件排除、文件名现场解析 `S01E28`、扫描后自动识别与完结回填、`publish_after_identify=False` 不入队、TMDB 失败降级、批次保护三类跳过、完整批次对账清理、短分页批次判定、重扫端点返回统计。
- 全套 **95 项通过**，前端 `vite build` 通过。

### 镜像
```bash
docker pull abson2/ya-tg:v0.7.1
```

---

## v0.7.0 (2026-11-18)

四大需求分批交付的第一批：授权脱敏、资源库重构与批量管理、TG 分发配置升级（纯本地功能）。镜像库复制（需求2）与投稿/TgtoDrive 对接（需求4第二批）按计划放下一版。

### 需求1：光鸭授权账号脱敏
- 新增 `utils.mask_account()`：`/api/sms/status` 的 `account`、`/api/auth/status` 的 `sub` 返回前已打码（手机号 `138****5678`，其它 ID 保留头尾）
- 前端「用户 ID」标注「已脱敏」。Token 本就加密落库，本次堵住展示层泄露

### 需求3：资源库重构与批量管理
- TMDB `_normalize()` 补 `finish_status`（剧集 status → ended/returning），`Resource` 加 `finish_status` 列并随识别落库；`library_series()` 返回 finish_status/share_duration，并支持 `finish/identify_status/share_status` 筛选参数
- 资源库加一级状态栏（识别：全部/已识别/待识别；类型：全部/电影/剧集/完结/追更），参考用户海报墙布局；分享状态/效期/完结追更作角标
- 新增 `POST /api/library/batch`：批量设置效期（1/7/30天/永久）、取消分享、重新分享、批量重识别；按作品代表资源整剧处理，单个失败不影响其余
- 前端作品卡支持勾选/全选、批量操作条；**移除单集卡片/单集列表视图**，统一为整部作品海报墙

### 需求4第一批：TG 分发配置升级（纯本地）
- 新增运行时配置与端点：`GET/POST /api/tg/extra-config`（推送模式 channel/bot_channel/custom_bot_group、自定义 bot token+群聊、福袋 bot 用户名、链接样式、详情篇幅）、`GET/POST /api/reward/config`（赞赏码）
- 分享链接按钮化：`tg_link_style=button` 时用 Telegram inline_keyboard 显示「📥 获取资源」按钮 + 可选「🎁 福袋/入库」跳转（福袋 bot `?start=library`），并从正文去掉纯文本链接
- 精简详情：`tg_detail_level=compact` 时简介压到 110 字
- `custom_bot_group` 模式用自定义 bot token/群聊覆盖发送；新增模板变量 `{{reward_code}}`

### 下一版待办（需求2 + 需求4第二批）
- 需求2 镜像库复制：光鸭官方 OpenAPI 无 copy 接口、写接口默认关闭(123)且需 VIP(430)+开放平台应用；用户选择抓包网页端复制接口后实现
- 需求4 投稿模块（投稿转存/我想增加关键词监控）依赖 TgtoDrive（用户确认已配光鸭）+ 新增 bot 接收消息(getUpdates)模块；关键词监控复刻 embyhub 的 `kw_<tmdb>_<season>` 幂等方案，不覆盖原有规则

### 测试
- 新增 `tests/test_v070_features.py`（8 项）：脱敏、TMDB 完结状态、落库、批量取消/设效期、状态筛选、TG 配置、消息精简
- 全套 **86 项通过**，前端 `vite build` 通过

### 镜像
```bash
docker pull abson2/ya-tg:v0.7.0
```

---

## v0.6.5 (2026-11-18)

重构推送触发链路：扫描只入库，识别成功且有 TMDB 海报后才入队；同一作品（整剧）只推一条，不再一集一条；资源库展示与推送共用同一作品键；补齐整剧大小/集数与逐条发送节流。

### 问题（用户反馈）
- 推送从扫描就开始了，新剧一入库就一集一条推，太不优雅；要求「扫描只入库，识别完成拿到海报后才推」
- 已升级到最新版仍一集一集推；识别成功的会分享剧目录，但同剧显示两张卡，集数显示 `E25-E25`
- 虽然分享是剧文件夹链接，但 TG 消息里文件大小、剧集信息都是错的（单集大小/无集数）
- 推送间隔没起作用，要手动可调，避免 TG 风控

### 分享粒度修正：剧集一律按文件夹（逐集推送的根因）
- 根因：`series_dir_info()` 在缺目录链（旧数据/未重扫）时 `scope="file"` 退化为按单集分享；手动「分享」端点 `create_share()` 更是一直直接用 `x.file_id` 按单集建链。单集分享 → 单集链接 → 逐集推送
- `series_dir_info()`：只要有父目录（parent_id）就按目录分享，缺目录链时用 parent_id 兜底，绝不退化成单集（仅连父目录都没有才无法按目录分享）
- 手动「分享」端点改为按剧集目录建链：目录 fileId + 标题带季，同目录已有分享直接复用，新分享同步给同目录所有单集，返回 `scope`/`members`
- 旧剧集入队前现场补算：已识别剧集若缺 `share_dir_id`，`_enqueue_ready()` 调 `series_dir_info` 现场补齐，保证推送也走整剧目录分享

### 推送重构：识别后才入队 + 整剧单条
- 扫描链路 `scanner.py` 移除规则加载与入队逻辑，`run_scan` 只负责把文件落库
- 新增 `_enqueue_ready()` / `_enqueue_after_identify()`：只有 `identify_status=identified` 且 `poster_path` 非空才入队；`identify` / `auto_identify` / `identify_all` 识别成功后统一触发
- 同一作品同一频道只入一条：作品键优先归一化作品名/目录，展示与推送共用 `_series_group_key()`，保证「一张卡 = 一条推送」
- 修复批量识别去重失效的真实 bug：`Session` 为 `autoflush=False`，入队后未 `flush` 导致前一条队列项对后续同剧单集不可见，36 集全部重复入队
- `_enqueue_existing()`（保存规则回溯）同步遵守「已识别 + 有海报 + 同作品一条」，未识别资源不再被补入队列
- 一次性数据收敛：升级时同频道同作品的旧队列残留只保留最早一条未发送项（配置标记确保只跑一次），已 sent/failed 历史不动

### 资源库作品聚合修正
- 作品键从「TMDB ID 优先」改为「归一化作品名/目录优先」，已识别与未识别的同剧单集不再裂成两张卡
- `episode_label` 修正：单集显示「已识别 E25」，多集连续才显示 `E01-E36`，不再出现 `E25-E25`
- 作品卡片补回「手动修正」按钮（LitePan 风格手动匹配窗口）

### TG 模板整剧信息
- 文件大小改为同作品所有单集 `size` 之和；季 label 取整剧/多季范围，不再取代表单集
- 新增模板变量 `{{episode_count}}`，默认模板增加「📼 集数：共 N 集」，消息不再出现单集文件名
- 模板迁移从精确字符串匹配改为特征识别：v0.5.x 老模板、v0.6.4 整剧模板（含 🎬 但缺集数行）都会自动升级到 v0.6.5；用户自定义模板不动。解决了「已升级但模板没变」

### 推送节流
- 新增运行时配置 `telegram_send_delay`（每条 TG 消息之间的间隔秒数，默认 2，范围 0-60），`process_queue()` 逐条发送时 sleep，规避风控
- 分发规则 UI 拆分「检查队列间隔」与「每条间隔」两个可调项；修复失败分支退避秒数覆盖节流变量的 bug

### 手机浏览器适配
- 新增三档响应式断点（≤900 / ≤680 / ≤400px）：侧栏在窄屏从左侧竖排改为顶部横向可滚动导航，主区占满宽度；顶栏隐藏时钟、 Compress 内边距
- 多列 grid（统计卡/凭证栏/键值栏/变量栏/光标盒）在手机降为两列或单列；海报墙保持两列竖版卡片，按钮缩小
- 固定宽度内联输入/下拉在窄屏强制换行占满；调度设置、搜索工具栏、匹配控件 flex 换行；弹窗宽度 92% 适配小屏；表格可横向滚动不撑破布局
- 修正侧栏版本号长期显示 v0.6.1 的遗留 bug

### 测试
- 新增 `tests/test_v065_push_logic.py`（13 项）：识别后才入队、同剧单条、作品归一不裂卡、单集集数 label、整剧大小求和、模板分代迁移、旧队列收敛、回溯只入已识别、节流配置、手动分享按目录、旧剧集入队补目录、worker 运行期去重
- 更新 `tests/test_flow.py`、`tests/test_v061_fixes.py`（回溯契约）、`tests/test_v063_series_share.py`（缺链按父目录分享）
- 全套 78 项通过，前端 `vite build` 通过

### 镜像
```bash
docker pull abson2/ya-tg:v0.6.5
```

---

## v0.6.4 (2026-11-18)

修复分发规则弹窗 bug；资源库按作品聚合并重做海报墙界面；推送模板改为整剧风格；新增推送调度间隔/Cron 设置与取消分享、清空扫描结果。

### 问题（用户反馈）
- 添加分发规则弹窗点「取消」「保存」关不掉，只能点右上角 ×；刷新后又弹，保存后还提示"请填写规则名称"
- 资源库仍按单集散落呈现，没有按剧集聚合；界面杂乱、不美观，参考 STRM 刮削海报墙布局
- 手动推送成功了，但模板推送的是单集文件名（资源：The.Silent.Storm.S01E04...），要求改成整剧风格
- 追加：分发规则页要能设置 Telegram 推送的 Cron 时间间隔；资源库要能取消分享、清空当前扫描结果

### 分发规则弹窗 bug
- 根因：`el-dialog` 的 `v-model` 绑了字符串状态，Element Plus 关闭时写回 boolean false，导致内容区 `v-if` 全部失效、取消/保存按钮无法关闭弹窗
- 修复：`v-model` 改用 boolean `manageDialog`，表单类型用独立 `manageKind` 判断；保存成功直接关弹窗刷新，无多余提示

### 资源库按作品聚合 + 海报墙
- 新增 `GET /api/library/series`：按「TMDB ID → 剧集目录 fileId → 父目录名」三级 key 把同剧 N 个单集聚合成一张作品卡，带海报、年份、季/集范围、识别与分享汇总
- 资源库默认「作品视图」：紧凑竖版海报墙（2:3 海报 + 绿色已分享对勾 + 集数角标 + 标题/类型/年份/集范围），可切换单集卡片/列表视图
- 新增 `POST /api/library/clear`：一键清空资源、推送队列、审核项与扫描断点，保留 Bot/频道/规则/模板/凭证与已建分享，避免旧识别失败/重试残留干扰重扫
- 取消分享按钮：`DELETE /api/resources/{id}/share` 现在会同步清空共用同一分享的所有剧集单集的 share_url/share_id，不再残留失效链接

### 推送模板整剧风格
- 默认模板改为参考「保镖恋人」风格：🎬 标题(年份) / 📺 季 / 🔗 TMDB / ⭐ 评分 / 📐 画质 / 🎞 版本 / 💾 大小 / 📖 简介 / 🏷 标签 / 分享链接，**不再输出单集文件名**
- 新增模板变量 `{{version}}`（从文件名提取 2160p · WEB-DL · H.265 · 10bit · DoVi · Atmos 规格串）与 `{{season_label}}`（第N季）
- 存量默认模板（v0.5.x / v0.6.0 原版、用户未改过）启动时自动升级；用户自定义模板不动

### 推送调度设置
- 分发规则「链路自检」新增推送间隔（秒）与 Cron 表达式设置，Cron 为 5 段（分 时 日 月 周，北京时间），非空时优先于间隔
- 新增 `GET/POST /api/queue/schedule`，保存后立即热更新定时任务，无需重启容器；非法 Cron 给中文错误提示

### 测试
- 新增 `tests/test_v064_library_ui.py`（7 项）；全套 65 项通过，前端 `vite build` 通过

### 镜像
```bash
docker pull abson2/ya-tg:v0.6.4
```

---

## v0.6.3 (2026-11-17)

修复剧集分享粒度（此前只推单集链接），让「立即推送」可反复点击，并去掉两处多余弹窗提示。

### 问题（用户反馈）
- 剧集推送出去的分享链接全是单集，没有整剧/季目录链接
- 剧集存放分两种：未整理时每集的父目录就是整剧目录；整理后（`剧名/Season N/单集`）要上上层才是剧集目录。用户常态是已整理，但要求兼容未整理
- 「立即推送」点第一次有效，之后再点显示「已入队 0 条」，用户以为按钮坏了
- 推送失败弹模态框、规则保存后再弹提示，都是多余打扰

### 剧集目录级分享
- 扫描新增目录 fileId 链 `dir_chain_json`（`[父, 祖父, ...]`），并持久化到 `scan_cursors.dir_chains_json`，断点续扫后仍能定位层级
- 新增 `utils.series_dir_info()` 判定分享目标：未整理取直接父目录；已整理（`Season N` / `第N季`）取上上层剧名目录；无目录链（升级前老资源）退化为按单集分享
- 识别为剧集后自动打上 `share_scope/share_dir_id/share_dir_name`；电影仍按单集
- `ensure_share()` 对剧集用**剧集目录 fileId** 建分享，标题为「剧名 第N季」；同目录只建一个分享，兄弟单集复用同一 `share_url`，不再每集一条链接

### 推送与界面
- 「立即推送」对已存在队列项 ANY 状态（含 `sent`/`retry`）强制重置为 `pending` 并立即重推，不再出现「已入队 0 条」
- `publish_resource()` 返回 `skipped`：规则的目标频道/Bot 停用、目录或关键词不匹配时给出明确原因
- 推送失败由模态弹窗改为轻提示；规则保存成功后不再弹「规则已保存」，直接关弹窗并刷新列表

### 升级注意
- 老资源需先点「立即扫描」，再「按目录重识别」，才会补上目录分享标记；已有单集分享链接的资源保持幂等，不会自动重建

### 测试
- 新增 `tests/test_v063_series_share.py`（8 项）；全套 58 项通过，前端 `vite build` 通过

### 镜像
```bash
docker pull abson2/ya-tg:v0.6.3
```

---

## v0.6.2 (2026-11-17)

修复三项实测问题：发布组目录下剧集散落、分发规则管理界面杂乱、推送失败原因不可见。

### 问题（用户截图与反馈）
- 光鸭目录 `推广专用 > [高清剧集网发布 www.THDT...]` 下的 `Tian.Xian.Pei.S01E28.2007...` 等整批剧集在资源库「散落」：v0.6.1 把发布组名当作品名搜 TMDB，必然无结果；英文/拼音文件名又被当作「备用」一律转人工，永远识别不出来
- 分发规则页是「填一个表单、下面存一条」的三列布局，管理 Bot / 频道 / 规则时混乱
- 资源点「立即推送」看不到结果；审核中心与推送队列看似无内容，实际是建分享/Telegram 失败的队列项变 retry 后用户不知道卡在哪

### 识别：发布组目录不干扰作品名
- `utils.parse_media_name` 新增站点/发布组目录识别：`www.`、`.com/.net`、「发布 / 剧集网 / 影视网 / 字幕组 / 高清 / 推广 / 专用 / 分享」等目录段直接跳过，不当作品名
- 新增 `dir_unreliable` 标记：目录线索不可信而回退到文件名时，文件名（如 `Tian Xian Pei`）就是唯一可靠作品名，TMDB 命中允许自动落库；真实「中文目录剧名 + 英文单集名」组合仍按 v0.6.1 转人工，防误配保护不回退

### 界面：分发规则三卡片
- 分发规则页重构为「Bot 管理 / 频道管理 / 规则管理」三个独立卡片，各自表格显示已有项与启停状态，新增统一走弹窗，不再表单与列表混排
- 链路自检新增「最近失败原因」直接展示，retry 状态计入阻塞项，避免显示「链路完整」却收不到消息

### 推送链路可观测
- `POST /api/resources/{rid}/publish` 返回 `detail`（本次队列项的状态与错误），前端用弹窗明确显示「建分享失败 / Telegram 失败」原因，不再只显示「已入队 N 条」
- `GET /api/diagnostics` 新增 `last_error` 与 retry 阻塞提示

### 测试
- 新增 `tests/test_v062_fixes.py`（4 项）；全套通过，前端 `vite build` 通过

### 镜像
```bash
docker pull abson2/ya-tg:v0.6.2
```

---

## v0.6.1 (2026-11-17)

修复 4 个实测 bug，并按追加需求补齐「目录剧名识别 / 中文剧名 / 多语翻译搜索 / Bot 与频道独立启停」。

### 问题（用户截图与反馈）
- `谷雨 (2026)(1) > Season 1 > The.Silent.Storm.S01E04.2026.2160p...` 被识别成「寂静之噪」TMDB #314531：旧版只拿英文单集文件名去搜 TMDB，父目录里的中文剧名完全没用上
- 识别后没有推送到 TG Bot/频道：规则保存失败 → 无队列项；且保存规则不会回溯已入库资源、`auto_publish` 默认关闭会停在「人工审核」
- 「分发规则」保存弹出 `[object Object]`：前端把 FastAPI 422 的 `detail`（数组）直接 `Error()` 字符串化；真实原因是未选目标频道
- Bot 的「验证」只调 `getMe`，不发送任何 Telegram 消息；频道行在未添加频道时根本不存在
- 追加：频道、Bot 均独立运行，未配置则不启用，两者要有启用/关闭按钮
- 追加：手动识别支持输入中文标题，自动译成英/日/韩等外语标题搜索并返回带海报的电影/剧集；识别剧名优先中文

### 识别：目录名优先 + 中文剧名
- 扫描时把目录显示名链写入 `Resource.path_hint`（如 `谷雨 (2026)(1)/Season 1`），并存入 `ScanCursor.dir_paths_json` 支持断点续扫；兼容网页端/官方接口的多种目录名字段
- `parse_media_name` 新增 `_dir_clues()`：跳过 `Season 1` / `第1季` 等季目录与「电影/剧集/影视」等通用目录，取最后一个作品目录；清洗网盘重名后缀 `(1)`、年份与 TMDB 标记
- 新增输出 `primary_title` / `dir_title` / `file_title` / `alt_title` / `title_source` / `has_chinese`；季目录同样是 tv 硬线索
- TMDB 所有请求统一带 `language=zh-CN`；`_normalize` 与 `_localize_best` 优先中文展示名。中文剧名主搜命中但 TMDB 返回英文标题时做分数补偿，避免误判成「匹配存疑」
- **英文文件名只作备用且绝不自动落库**：中文剧名搜不到时，英文候选一律标 `ambiguous` 转人工，不再把剧集单集认成别的作品
- 新增 `POST /api/resources/identify-all?force=true`：批量按目录重识别，用于修正升级前已经误配的资源

### 中文标题多语翻译搜索
- `ai_assist.translate_title()`：用已配置的 OpenAI 兼容接口把中文剧名译成英/日/韩/原名；未启用 AI 或调用失败静默降级，仍按中文原名搜索
- `tmdb.search_multilingual()`：中文 + 外语标题依次搜索，按 `(类型, TMDB ID)` 去重，候选带 `poster_url`，并返回 `translated` 与 `tried`
- 新增 `POST /api/tmdb/translate-search`；手动匹配窗口默认填入中文目录剧名，展示翻译结果与尝试过的查询词，可直接凭海报选择
- 自动识别在中文剧名未命中时，先尝试 AI 译出的外语标题，最后才考虑英文文件名

### 分发链路：从「识别」走到「推送」
- `POST /api/rules` 保存规则时**回溯已入库资源**补建队列项并返回 `queued`；`auto_publish` 默认改为 `true`，关闭时明确提示需到审核中心逐条通过
- `RuleInput.channel_id` 允许缺省，后端给中文 400；前端 `api()` 新增 `errText()` 规范化 dict/数组 `detail`，不再出现 `[object Object]`；保存前校验 Bot/频道/目标频道并给引导
- 新增 `POST /api/queue/run`（立即处理队列）、`POST /api/resources/{id}/publish`（单资源入队并立即推送）、`GET /api/diagnostics`（链路自检：Bot/频道/规则/待审/失败/已推送数量与阻塞原因）
- 「分发规则」页新增链路自检面板与「立即推送队列」按钮；资源库新增「按目录重识别」「立即推送」按钮并显示目录线索

### Telegram 测试推送与独立启停
- 新增 `POST /api/bots/{id}/test-message`：Bot 行「测试推送」真实发一条 `sendMessage`；chat_id 留空自动取该 Bot 下第一个频道，未配置频道时提示手填。原「验证」改名「验证 Token」并说明不发消息
- 新增 `PUT /api/bots/{id}` / `PUT /api/channels/{id}`：Bot 与频道各自独立启停，未配置/停用即不启用该链路
- `process_queue` 遇到停用的 Bot/频道跳过队列项、不计失败、不消耗重试次数、不影响其它链路；重新启用后下一轮自动继续
- Telegram API 地址改为运行时配置（管理页「分发规则 → 链路自检」可填反代），`main` 与 `publisher` 共用；连接超时/失败给出「国内网络请配置反向代理」等可操作中文提示

### 测试
- 新增 `tests/test_v061_fixes.py`（8 项）；全套 **46 项通过**，前端 `vite build` 通过

### 镜像
```bash
docker pull abson2/ya-tg:v0.6.1
```

---

## v0.6.0 (2026-11-17)

媒体识别重构：电影/剧集分类判定、TMDB 标签打标、AI 辅助识别、手动匹配窗口，并补齐 Bot/频道的测试与删除，顺带修掉「验证连接」的 `code=120`。

### 问题
- 用户截图：`The.Silent.Storm.S01E04.2026.2160p...` 被识别成 1930 年代电影 `Cavalcade d'amour`（TMDB #279155）。旧逻辑把 `S01E04` 留在搜索词里，`/search/tv` 无结果后回退到 `/search/multi`，把电影候选当第一名直接落库
- 推送消息没有 TMDB 分类标签；模板变量只有 8 个，条件块不支持嵌套
- 识别失败只能「输一个 TMDB ID」且强制按 movie 处理，没有按类型搜索、看海报选择、标记无匹配的能力
- 「分发规则」页添加完 Bot/频道后无法验证是否可用，也没有删除入口
- 顶栏「验证连接」误调官方 OpenAPI `/openapi/v1/user/get_user_info`，短信登录凭证没有开放平台应用 ID → `code=120 msg=缺少clientId`（v0.5.3 修扫描时漏掉的同类错配）

### 识别策略（借鉴 TgtoDrive，不依赖其运行时）
来源：[walkingddd/TgtoDrive README](https://github.com/walkingddd/TgtoDrive) 的「根据 TMDB 识别作品与季集」「需要 AI 回退识别时再配置 AI 辅助识别及对应整理开关」，其 OpenAPI 含 `/api/ai-media-parser/config`、`test-connection`、`/api/organize-history/recognize-test`、`/api/media/details/tmdb/tv/{id}/season/{n}`；本地 `tgto plugin.py` 的规则 ID `tmdb_movie_<id>` / `tmdb_tv_<id>_<season>` 与「优先 ProviderIds.Tmdb 精确匹配，兜底按 名称+年份」。
- `utils.parse_media_name` 重写：输出 `type_hint`（tv/movie/unknown）、`type_source`、`clean_title`、`tmdb_id`、季集号；扩展技术噪声清理（DDP5.1 / 10bit / Season 2 / 第N集 等），兼容传入完整路径与目录线索
- `tmdb.identify` 决策链：**ID 精确匹配 → 类型硬约束（剧集绝不回退电影）→ 类型未知时 tv/movie 双路搜索 + 片名相似度/年份/热度评分 → 分数接近标 ambiguous 不自动落库 → AI 兜底**
- 支持 `(tmdb-206484)`、`{tmdb=206484}` 等 ID 标记；`detail()` 补全 `genres` / `origin_country` / `vote_average` / `original_title`
- 资源新增 `identify_status`（identified / ambiguous / failed / none / unidentified）、`identify_source`、`identify_note`、`season`、`episode`、`genres`、`origin_country`、`vote_average`、`original_title`，SQLite 轻量迁移自动补列

### 消息模板打标
- 新增模板变量 `type`、`type_label`、`origin_country`、`origin_country_code`、`genres`、`tags`、`vote_average`、`original_title`、`season`、`episode`、`episode_tag`、`media_type`
- `tags` 按「片名 · 类型 · 地区 · 分类」生成，示例 `#保镖恋人 #电影 #西班牙 #爱情 #惊悚`；默认模板含 `type={{type}} · origin_country={{origin_country}} · genres={{genres}}` 一行
- 默认模板升级：老用户若没改过 v0.5.x 默认模板，`migrate()` 自动换成新版；也可在「消息模板」页点「载入 v0.6.0 默认模板」
- `render_template` 改为递归解析，条件块 `{{#if}}...{{/if}}` 支持嵌套

### AI 辅助识别（可选）
- `ai_assist.py`：任意 OpenAI 兼容 `/chat/completions`，用户自定义 Base URL / API Key / 模型 / 补充提示词 / 超时；密钥加密落库、DB 优先 `.env` 兜底
- 只用于文件名解析、主搜索失败时提供备用片名、候选接近时消歧；季集硬证据不会被 AI 推翻，任何异常静默降级
- 端点 `GET /api/ai/status`、`POST /api/ai/config`、`POST /api/ai/test`、`POST /api/ai/parse`（识别测试）；管理页「TMDB 设置 → AI 辅助识别」

### 手动匹配（参考 LitePan）
来源：[Ponphil/LitePan](https://github.com/Ponphil/LitePan) 与 [LitePan-old](https://github.com/Ponphil/LitePan-old) 的「重新匹配元数据」窗口、按「已刮削/匹配存疑/缺失」与「电影/剧集」双维筛选、目录 `(tmdb-206484)` 标记。
- 资源库「手动」改为弹窗：类型下拉（剧集/电影）+ 片名或 TMDB ID + 搜索 + 海报候选（标题/年份/类型/TMDB ID/分类）+「应用匹配」/「无匹配」
- 后端 `GET /api/tmdb/search?q=&media_type=`（纯数字或 `tmdb-123` 按 ID 直取）、`POST /api/resources/{id}/identify`、`POST /api/resources/{id}/match-none`、`GET /api/tmdb/candidates/{id}` 返回结构化决策（含 hint / reason / ai）
- 自动识别不再瞎猜：ambiguous/failed 时前端引导跳转手动匹配；资源库可按媒体类型与识别状态筛选

### Bot / 频道 / 规则管理
- `POST /api/bots/{id}/test`：Telegram `getMe` 验证 Bot Token；`POST /api/channels/{id}/test`：真实发一条测试消息，失败给可操作提示（频道 ID / Bot 未入频道 / Token 失效 / 429 限流）
- `DELETE /api/bots/{id}`、`DELETE /api/channels/{id}`、`DELETE /api/rules/{id}`；删除 Bot/频道级联清理关联频道、分发规则与队列项，返回值明确列出清理数量，前端确认框提前展示，资源本身保留

### 修复：验证连接 code=120
- `/api/auth/openapi/verify` 按 `AuthToken.flow` 分流：短信登录用网页端 `POST /userres/v1/file/get_file_list`（pageSize=1）做轻量探测；官方 Device/PKCE 才调 `get_user_info`
- `GuangYaOpenAPI._call` 增加前置防护：网页端凭证且无开放平台 `client_id` 时直接抛带中文说明的 `code=120`，杜绝其余残留调用
- 返回 `channel` 字段，前端「验证连接」提示区分网页端与官方通道

### 测试
- 新增 `tests/test_identify_v060.py`、`tests/test_verify_channel.py`、`tests/test_manage_v060.py`；全套 **38 项通过**，前端 `vite build` 通过

### 镜像
```bash
docker pull abson2/ya-tg:v0.6.0
```

---

## v0.5.4 (2026-11-17)

修复资源库识别报 `HTTP 401 Invalid API key`，并把 TMDB 密钥改成管理页可配置。登录、扫描、建分享、推送链路不受影响。

### 问题
- 用户填了无效密钥、或把 v4 Read Access Token 当成 v3 API Key 填（旧版按字段名死板判断），TMDB 返回 `status_code:7 Invalid API key`，前端弹出原始英文 401，且资源库看起来"无法识别"
- 旧版密钥只能写 `.env` 并重启容器才能改；部分识别端点没把 `db` 传给 TMDB 模块，导致管理页配置读不到
- 明确一点：TMDB 只负责海报/简介，**publisher 只读本地已存字段、不发起 TMDB 请求**，所以 401 从未阻断资源入库、建分享和推送

### 修复
- `tmdb.py`：按值特征自动识别凭证类型（v4 `eyJ...` → Bearer；v3 32位hex → api_key），填错框也能纠正；修正两个字段填相同值时的来源误判；新增 `auth_status()` / `test_connection()` / `TmdbAuthError`
- 401/403 与未配置统一转成**可操作的中文提示**（含 v3/v4 格式说明与「测试连接」引导），不再抛原始 `TMDB 请求失败 HTTP 401`
- `runtime_config` 增加 `tmdb_api_key` / `tmdb_bearer_token` / `tmdb_language`，密钥加密落库、DB 优先 `.env` 兜底
- 新增管理端点 `GET /api/tmdb/status`、`POST /api/tmdb/config`、`POST /api/tmdb/test`；所有识别/搜索端点（candidates、identify、auto-identify、search）都传 `db`，凭证问题返回 400 而非 502
- 前端新增「TMDB 设置」页：状态展示、密钥输入、保存、测试连接、TMDB 申请链接；概览健康检查反映真实配置状态；资源库识别失败时弹窗引导跳转设置页，并说明"不影响推送"
- 文档：`.env.example` 与 README 补管理页配置说明和 401 FAQ

### 测试
- 新增 `tests/test_tmdb_config.py`：v3/v4 自动纠正、同值不误判、未配置/401 的中文提示、配置端点与 400 引导；全套 23 项通过

### 镜像
```bash
docker pull abson2/ya-tg:v0.5.4
```

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
