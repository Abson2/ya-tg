# Ya-TG 功能保护清单

> 阶段一产物。整改（阶段二~七）必须逐项保证下列原有功能继续有效。
> 基线：`d91c4c10e967f5cab9fa6b4748e334f8a088ff52`。回归结果栏将在各阶段完成后更新。

## 保护原则
- 不删除任何原有功能、页面、API、配置、数据表、用户操作能力、Docker 部署能力。
- 架构调整以真实项目结构为基础，不做无意义的大规模目录迁移。
- 所有端点 URL、数据库旧字段/旧数据、配置项键名保持不变（新增可，删除/改名义禁止）。
- 行为变更（如镜像调度模式）必须先获用户确认。

---

## 1. 光鸭授权与凭证

| 功能 | 真实实现位置 | 保护要求 | 回归状态 |
| --- | --- | --- | --- |
| 短信四步登录 | `guangya_sms.py` + `main.py:641-668` | access/refresh + did 全链路保留 | 待阶段七 |
| 扫码登录（后台授权页） | `guangya_auth.py:157-215` + `main.py:594-615` | TG 无关，保留 | 待阶段七 |
| token 定时刷新 | `main.py:157-181`（60s，提前 10min） | 不得回退成每请求刷新 | 待阶段七 |
| token 竞态修复 | `guangya.py:161-192/247/284` | `_fresh_headers` + 117 刷新重试必须保留 | 待阶段七 |
| OpenAPI 分流（tgto） | `main.py:679` `/auth/openapi/verify` | sms/tgto→WebFiles，device/pkce→OpenAPI guard 保留 | 待阶段七 |
| 凭证 Fernet 加密 | `security.py` + `db.py` | 加密/解密/脱敏保留，轮换兼容 | 待阶段七 |

## 2. A→B 镜像（核心，最高保护级别）

| 功能 | 真实实现位置 | 保护要求 | 回归状态 |
| --- | --- | --- | --- |
| 作品级轮转批次 | `mirror.py:212-244/1011-1017`（works_cursor + max_works_per_run） | 阶段二若重构须先获用户确认；否则原样保留 | 待决策 |
| 分类/作品/季目录 1:1 | `mirror.py:668-753 _ensure_child` | 分类原名、`·` 约定、季目录原名、plain_mode 降级保留 | 待阶段二 |
| 批量复制 + 批量轮询 | `mirror.py:961-983` + `:370-405` | 若选低风险方案保留；严格顺序方案需用户确认 | 待决策 |
| 按名+大小回填 fileId | `mirror.py:1095-1096` + `:434-446` | 真实 HAR 证实复制响应不回传副本 ID，**此机制不得删除**，只可增强复合键 | 待阶段二 |
| unknown 先查 B 库 | `mirror.py:869-881` | 防重复复制核心，保留 | 待阶段二 |
| 目录可信性闸门 | `mirror.py:838-866`（b_verified） | 列目录不可信不清理/不重置，保留 | 待阶段二 |
| 英文名四层兜底 | `mirror.py:1174-1283` | 缓存→Resource→TMDB(6s)→AI(10s)→拼音，用户明确不动 | 保留不动 |
| 改名在扫描之前 | `main.py:1160-1169` | 顺序保留，改名失败不阻断主链路（`_safe_rename`） | 待阶段二 |
| 镜像失败定向重试 | `main.py:1274-1282`（retry files/dirs） | 只扫失败项不扫全库，保留 | 待阶段二 |
| 镜像锁 + 心跳 + 僵尸自愈 | `main.py:44-127/285-294` | v0.9.21 三项保留；B-03 残留接管问题需修复但保持对外行为 | 待阶段二 |
| 镜像后管线不群推历史 | `main.py:892-940` | run_scan→post_identify→enqueue，**绝不 process_change_events** | 待阶段二 |

## 3. 资源库 / 扫描 / 分享

| 功能 | 实现位置 | 保护要求 |
| --- | --- | --- |
| 全量/手动/异步/定时扫描 | `scanner.py` + `main.py:2091/2169/376` | 扫描失败不误删；对账门槛（complete+非resumed+seen非空）保留 |
| 作品卡片墙 | `main.py:2255 library_series` | 聚合/筛选语义不变（A-08 只改查询方式） |
| 识别（单/全部/未匹配） | `main.py:2957-3050` | TMDB zh-CN、AI 静默降级、季集线索不回退电影 |
| 分享（建/改/取消/状态） | `main.py:2565-2660` | 目录分享粒度、同目录复用 share_url、超时先核验远端避免重复建 |
| 追更/完结核对 | `main.py:2364 finish_check` | 统计口径保留（A-04 只改并发方式，限流不绕过） |

## 4. Bot / 频道 / 推送

| 功能 | 实现位置 | 保护要求 |
| --- | --- | --- |
| 多 Bot 管理（增删改启停测试） | `main.py:3126-3250/3747-3773` | 完整保留 |
| 多频道/群 + 白名单 + 权限 | `main.py:3238-3330/4434-4841` | 频道需管理员、群无需、白名单闸门保留 |
| getUpdates 轮询 | `bot_listener.py:1660-1689` | update_id 幂等去重保留；A-10 修复须保留幂等语义 |
| 推送队列 worker | `publisher.py:383-515` | 白名单闸门、目录分享粒度、逐项 commit、节流 2s、退避、429 retry_after |
| 唯一口径「由 Bot 推送」 | `publisher.py` + `main.py` | 频道/群都作 Channel 用所属 Bot token 发，保留 |
| 分发事件作品级一次推 | `publisher.py` + `distribution_events` | 变化走唯一事件，不批量补推历史 |
| 目录路由多目标 | `main.py:4217-4250` | targets 一目录绑多目标、兼容老双字段 |
| 搜索/打赏/求片/投稿四指令 | `bot_listener.py` | 会话态流程、sendPhoto 带 parse_mode=HTML 保留 |
| 发现列表删除语义 | `main.py:4780-4817` | 仅隐藏台账不动白名单/Channel/Bot 已加入群 |

## 5. 投稿 / 求片 / TgtoDrive

| 功能 | 实现位置 | 保护要求 |
| --- | --- | --- |
| 投稿链接强校验 | `contribution_links.py` | 只认 guangyapan.com/s/<shareId[_后缀]>，非光鸭不入库 |
| 投稿审核 + 光鸭转存 | `main.py:5001-5095` + `contribution_restore.py` | 审核态/转存态分离、幂等、重试、30s 续跑；**非 TgtoDrive** |
| TgtoDrive 白名单联动（求片） | `tgto.py:225-232` | 外部服务，不得内部集成；只增删本项目 kw_，conflict 护栏不覆盖他人 |
| 求片去重/TMDB 搜索 | `main.py:5117-5239` | upsert 合并计数、采纳/拒绝状态保留 |
| Bot→Bot 私聊转发 | 已删除（`bot_listener.py:947` 注释） | **不得恢复**（USER_BOT_TO_BOT_DISABLED 根因） |

## 6. 后台会话安全

| 功能 | 实现位置 | 保护要求 |
| --- | --- | --- |
| 密码换 token 登录 | `main.py:473-483` + `admin_session.py` | 浏览器只存 token 不存明文 |
| 双通道鉴权 | `main.py:136-154` | X-Admin-Session 主通道 + X-Admin-Password 兼容脚本，保留 |
| idle 120min 过期 | `admin_session.py` + `config.py` | `/auth/session` 只读不续期，业务请求才 touch |
| 会话内存态 | `admin_session.py` | 重启需重新登录（安全预期，不改为持久化除非用户要求） |

## 7. 日志 / 配置 / 部署

| 功能 | 实现位置 | 保护要求 |
| --- | --- | --- |
| 日志 30 天 + 每表 5000 条清理 | `log_retention.py` + `main.py:03:30 job` | 保留，pending/running 任务不清 |
| 配置 Fernet 加密存储 | `db.py` + `runtime_config.py` | 键名不变、masked 规则保留 |
| 消息模板预设/默认 | `main.py:3438-3500` + `db.py:247-422` | 默认模板去重、特征升级不覆盖自定义 |
| Docker 部署 | `Dockerfile` + `docker-compose.yml` | 端口 8787、卷 `./db`/`./logs`、IMAGE_TAG、时区、healthcheck 不变 |
| CI 双 job + tag 策略 | `.github/workflows/docker-publish.yml` | latest 仅 main 更新、tag 不移动、Release 由 workflow 建 |
| 数据库迁移 | `db.py:389-443` | 旧库可读、marker 幂等；任何新增列必须带默认值 |

## 8. 前端页面（tab）

| 页面 | 位置 | 保护要求 |
| --- | --- | --- |
| 概览 | `App.vue` | 真实数据来源，不造假数；不可得指标明确标注 |
| 光鸭授权 | `App.vue` | 四步向导 + 扫码备用 |
| 镜像同步 | `App.vue` | 进度/锁提示/失败清单/重试/改名，保留 |
| 分发与互动中心 | `App.vue` | a/b/c/d 模块含 c/d 并排（v0.9.20），保留 |
| 资源库 / 推送队列 / TMDB 设置 / 消息模板 / 操作审计 / 日志 | `App.vue` | 所有入口保留，UI 升级不得删按钮 |
| 任务中心（阶段六新增第 10 入口） | `App.vue` `tab === '任务中心'` | 纯聚合视图，只用已加载 ref，零新增 API、不造假数；点「查看明细」跳原页，8 类任务齐全 |

> 阶段六 UI 升级护栏：上述 9 个原 tab 一个不删（`tests/test_v104_dark_theme.py::test_all_tab_entries_preserved` + `test_v104_task_center.py::test_all_original_tabs_still_preserved` 双重守护）；批次控制 pause/resume/skip/cancel 四按钮保留；深色主题为附加层不改浅色默认。

---

## 回归执行约定（阶段七）
- 每阶段完成后跑全量 pytest（阶段五后基线 718 passed）+ 前端 build。
- 关键链路真机验证：光鸭登录、镜像一轮（复制+改名+入库+推送）、投稿审核转存、求片白名单、会话过期重登。
- 未执行的回归项必须显式标注，不得以编译/builder 成功代替功能通过。
