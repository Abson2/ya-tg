# 更新日志

本文件记录 Ya-TG 的每个正式版本。版本标签遵循 `vX.Y.Z`，每个版本都会同步发布到：
- GitHub Releases：`https://github.com/Abson2/ya-tg/releases`
- DockerHub：`abson2/ya-tg:vX.Y.Z`

> 文档分工：`README.md` 只讲功能、部署与使用；逐版本的变更与修复细节统一写在这里，不写进 README。

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
