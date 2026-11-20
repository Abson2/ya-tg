# Ya-TG 源码审计报告（阶段一）

> 本报告严格基于 GitHub 最新实际源码审计，所有结论带文件相对路径 + 行号 + 代码片段。
> 审计期间未修改任何业务代码。README / CHANGELOG / docs 仅作背景参考，不作为缺陷证据。

## 0. 版本锁定（基线）

| 项 | 值 |
| --- | --- |
| 仓库 | https://github.com/Abson2/ya-tg |
| 分支 | `main` |
| 审计基线 Commit SHA | `d91c4c10e967f5cab9fa6b4748e334f8a088ff52` |
| 提交时间 | 2026-11-20 17:10:17 +0800 |
| 提交说明 | `fix: v0.9.21 镜像锁心跳+僵尸自愈+文案区分（纠正过时的117诊断，不动解析链路）[skip ci]` |
| 最新正式版本标签 | `v0.9.20`（v0.9.21 为源码验证 commit，未打 tag） |
| 工作区状态 | 干净（审计全程只读） |
| 审计结束复核 | `origin/main` 仍为 `d91c4c10`，基线未漂移，本报告绑定该 SHA |

源码规模：后端 21 个模块约 12700 行（main.py 5287）、前端 App.vue 3249 行、迁移/脚本/CI 约 600 行、测试 68 文件 614 例。

---

## 1. 执行摘要

1. **任务书第六部分假设的"严格顺序镜像引擎"在当前源码中并未实现**。真实实现是「作品级轮转批次（works_cursor）+ 作品内批量提交复制 + 批量轮询 + 每文件立即 commit + B 库按名大小回填防重 + 进程内锁」。任务书要求的确定性自然排序、DFS 固定任务清单、sequence_id、阶段状态机、严格顺序执行器、受控并发、批次冻结——**全部不存在**。这是本次整改的核心工作，但属于**重大架构变更 + 数据兼容风险**，按任务书第 1.4 节与「需用户决策项」要求，**必须先向用户确认范围后再实施**（详见第 6 节）。
2. **P0-1 token 快照竞态（用户上一轮诊断）已被 v0.9.15 #267 修复**：`guangya.py:161-192 _fresh_headers()` 每请求用独立短会话读最新 DB token，`:247/:284` 遇 117 强制刷新重试一次。本次审计确认该问题**当前不存在**，不列入整改。
3. 后端有 3 类真实性能/并发缺陷（N+1 查询、跨 await 持写事务、同步 ORM 阻塞事件循环），均有明确行号。
4. 安全体系总体完整（Fernet 加密、日志脱敏、无 SQL 注入/XSS/危险函数实证），但发现 1 个需动态验证的路径穿越点（C 类）与 1 个占位符口令问题（B 类）。
5. 前端数据均来自真实 API、明确"不造假数"；但 `load()` 串行拉 15+ 接口，镜像 UI 缺暂停/恢复/跳过/取消操作。
6. 测试 614 例覆盖广，但**自然排序、DFS、故障注入、1000/10000 文件性能测试四类为零覆盖**。

问题分级统计：A 类（已确认）13 项、B 类（高风险）12 项、C 类（待验证）4 项、D 类（优化）9 项。

---

## 2. 功能保护清单（整改时不得破坏）

> 完整逐项清单见 `FEATURE_PRESERVATION.md`。此处列关键红线。

| 功能域 | 真实实现位置 | 保护要求 |
| --- | --- | --- |
| A→B 镜像 | `mirror.py:609-1049 sync_rule` + `main.py:1138-1216 _do_mirror_run` | works_cursor 轮转、按名+大小回填（`:1095-1096`）、每文件立即 commit 不得回退成跨 await 持事务 |
| 英文名解析 | `mirror.py:1174-1283 _resolve_english_title` | 缓存→Resource→TMDB(6s)→AI(10s)→拼音四层兜底，用户明确不动 |
| token 竞态修复 | `guangya.py:161-192/247/284` | v0.9.15 已修，不得重复实现或回退 |
| 投稿转存 | `contribution_restore.py` + `guangya.py:443-473` | 光鸭原生 API，非 TgtoDrive；审核态/转存态分离 |
| 求片白名单 | `tgto.py:225-232 upsert_keyword` | 只增删本项目 kw_ 规则，conflict 护栏（`:251-256`）不得覆盖他人 |
| Bot 推送 | `publisher.py:383-515 process_queue` | 唯一口径「由 Bot 推送」，白名单闸门，目录分享粒度 |
| 会话登录 | `admin_session.py` + `main.py:136/473-495` | 内存 token + idle 120min + 双通道鉴权，`/auth/session` 只读不续期 |
| 数据迁移 | `db.py:389-443 migrate` | create_all + 逐列 ALTER + marker 幂等，旧库必须可读 |
| 部署 | `docker-compose.yml` 8787 端口 / `./db` `./logs` 卷 | 端口/数据目录/持久化路径不得变更 |

---

## 3. API 路由清单（128 个端点）

全部位于 `backend/app/main.py`。绝大多数带 `dependencies=ADMIN`；匿名端点：`/api/health`、`POST /api/auth/login`、`GET /api/auth/session`、`GET /{path:path}`（SPA）。完整分组清单见附录 A（本文件末尾）。

统计：GET 41、POST 62、PUT 12、DELETE 9（按方法归类，部分端点多方法）。核心业务模块：光鸭授权 6、扫描 6、资源库 8、镜像 12、TMDB/识别/AI 12、Bot/频道/规则 14、队列/审核 6、发现 9、投稿/求片 9、TgtoDrive 1。

---

## 4. 问题清单（带源码证据）

### A 类 — 已确认问题（源码直接证明）

#### A-01｜N+1：投稿列表逐条查转存任务
- **位置**：`backend/app/main.py:4990-4993`（`contributions()`）；辅助 `contribution_restore.py:71-73`
- **逻辑**：`(c, contribution_restore.restore_for(db, c.id)) for c in db.scalars(stmt).all()`，每条投稿一次独立 `SELECT`
- **成因/触发**：列表最多 500 条 → 1+N 次 SQL
- **后果**：列表接口变慢、SQLite 读锁时间拉长
- **建议**：`where(contribution_id.in_(ids))` 批量取后内存映射；返回 JSON 结构不变
- **测试**：监听 `before_cursor_execute` 计数，造 100 条断言 SQL ≤ 3

#### A-02/A-03｜N+1：队列与审核列表逐条 `db.get`
- **位置**：`main.py:3378-3382`（`queue()`）、`:3389-3393`（`review()`）
- **逻辑**：`for q in ...: r, c = db.get(Resource, q.resource_id), db.get(Channel, q.channel_id)`
- **后果**：1+2N 次查询，最常看的页面放大 SQLite 压力
- **建议**：`select(QueueItem, Resource, Channel).outerjoin(...)` 一次取出；删除的资源仍返回空串
- **测试**：造 200 队列项断言 SQL 数量恒定

#### A-04｜逐作品串行 TMDB 网络请求
- **位置**：`main.py:2376-2390`（`finish_check()`）；`tmdb.py:109-124/201-202` 限流
- **逻辑**：`for key ...: cache[tid] = await tmdb.detail(tid, "tv", db)`（同作品去重，不同作品串行）
- **后果**：最多 100 作品 × 20s 超时，接口耗时几十秒；限流拒绝被吞成 error
- **建议**：复用本地缓存 + `asyncio.gather` 受控并发（保留限流，不绕过风控）
- **测试**：mock detail 造 30 tid，断言并发度与耗时，限流仍生效

#### A-05｜重复调用 `db.get(AuthToken, 1)` 两次
- **位置**：`main.py:507`
- **逻辑**：`db.get(AuthToken, 1) and db.get(AuthToken, 1).access_token_enc`
- **后果**：轻微开销，明确重复代码
- **建议**：取一次到局部变量；**测试**：断言 `db.get` 调用 1 次

#### A-06｜跨 await 持写事务：转存任务 flush 后跨 3 次网络 await 才 commit
- **位置**：`contribution_restore.py:177-180`（写+flush）、`:187/:196/:199`（3 次网络 await）、`main.py:5031-5033`（才 commit）
- **逻辑**：`row.status="restoring"; ...; db.flush()` → `await verify_in_target()` → `await restore_share_to_dir()` → `await verify_in_target()` → 回到 main 才 commit
- **成因**：同仓库 `publisher.py:420-425/468-470/510-511`、`scanner.py:212-215` 已有「await 前 commit」正确范式，此处未复用
- **后果**：SQLite 写锁从 flush 持续到 commit，其它写请求撞 `database is locked`（`db.py:12` busy_timeout=30s）；进程被杀则本轮丢失
- **建议**：`status='restoring'` + `attempts+1` 先 `db.commit()`，再进网络；结果回来后再写并 commit；`attempts` 不得重复累加
- **测试**：mock `verify_in_target` sleep 3s + 并发写请求，断言无锁错误

#### A-07｜架构性：async 路由/job 内用同步 SQLAlchemy Session
- **位置**：`db.py:37 SessionLocal`（同步）、`:12 timeout=30`；`main.py:157-181/208-231/376-393/5001-5047/2091-2105/3668-3671/4961-4963`、`publisher.py:383-515`、`scanner.py:151-324`
- **逻辑**：`async def` 内直接 `db.get()/db.scalar()/db.commit()`（阻塞式 sqlite3 调用运行在主事件循环线程）
- **后果**：叠加 30s busy_timeout，一次写锁竞争可让 `/api/health` 停摆、定时任务饿死
- **正面证据**：全局无 `time.sleep`/同步 `requests`/`os.system` 误用；网络统一 `httpx.AsyncClient`，节流用 `asyncio.sleep`
- **建议**：短期 busy_timeout 降到 5s + `asyncio.to_thread` 包 commit/批量查询；中期迁移 async session 或把 DB 密集路由改 `def`（线程池）
- **测试**：mock 网络耗时期间并发打 `/api/health` 断言 P99 < 500ms

#### A-08｜N+1：资源卡片墙关键词筛选逐成员 `db.get`
- **位置**：`main.py:2348-2351`（`library_series()`）
- **逻辑**：`any(q.lower() in (db.get(Resource, i).name or "").lower() for i in g["ids"])`，而外层 `rows` 已持有全部 Resource
- **建议**：`by_id = {x.id: x for x in rows}` 复用；**测试**：造 300 资源 + q 查询断言无额外 SQL

#### A-09｜后台 job 静默吞异常（无日志无诊断）
- **位置**：`main.py:199-203`（`_job_bot_poll`）、`:383-393`（`_scheduled_scan`）
- **逻辑**：`except Exception: db.rollback()` 无 logger / AuditLog
- **后果**：Bot token 失效、识别抛错时运维无痕；同仓库 `:229-231/:310-312/:345-346` 有正确 log 范式
- **建议**：`except Exception as exc: db.rollback(); logger.warning(..., exc_info=True)`
- **测试**：mock `poll_once` 抛异常断言 logger 调用且 rollback

#### A-10｜消息处理失败仍标记已处理并推进 offset（任务丢失）
- **位置**：`bot_listener.py:1660-1689`（`poll_once()`）
- **逻辑**：`except ...: errors.append(...)` 后无条件 `_mark_update_processed(db, uid, bot_id)` + `max_committed=uid`；注释明确「失败也记账，避免卡死」
- **后果**：TMDB 超时/AI 失败/DB 锁等瞬时故障导致投稿/求片/搜索消息被永久丢弃，用户以为 Bot 没收到
- **建议**：区分「可重试错误」（网络/限流/DB 锁）与「业务拒答」：前者不标记已处理、不推进 offset；或落 AuditLog + 重试上限。**保留 ProcessedUpdate 幂等去重语义**
- **测试**：mock `handle_text` 首次抛 Timeout、二次成功，断言该 update 被处理两次

#### A-11｜关闭时 `scheduler.shutdown(wait=False)` 不等待在跑 job
- **位置**：`main.py:454-456`
- **后果**：容器停止时队列推送/转存/扫描被腰斩；部分已 commit 项留待下轮
- **建议**：`wait=True` + 超时上限（如 30s）避免强杀
- **测试**：job 中 sleep，发 SIGTERM 断言 job 完成日志存在

#### A-12｜日志统计多轮查询
- **位置**：`log_retention.py:116-129`（stats）、`:147-171`（prune）
- **逻辑**：7 表 × (min+max+count+pruneable_count) = 28 次查询
- **建议**：单条聚合 SQL `COUNT/SUM(CASE)/MIN/MAX`；**测试**：SQL 计数从 28 降到 7

#### A-13｜镜像引擎核心机制缺失（任务书第六部分全部未实现）
- **位置**：`mirror.py` 全文（Grep 确认无 `sort/sorted/natsort`）；`models.py:405-417 MirrorTask` 无 sequence_id/batch_id 等字段
- **详细证据与逐条假设核对见第 5 节**。这是已确认的实现差距，不是 bug；整改属重大架构变更，见第 6 节需用户决策。

### B 类 — 高风险问题（源码明显风险，需运行确认）

#### B-01｜`process_queue` 无进程内锁，手动与定时可并发
- **位置**：`main.py:3668-3671`（`queue_run`）、`publisher.py:383-401`（`process_queue`）
- **逻辑**：定时 worker（~30s）与 HTTP「立即处理一次」共享 SQLite 队列，无 `asyncio.Lock`；APScheduler `max_instances=1` 管不到 HTTP 入口
- **后果**：重复建分享、重复发 Telegram、去重映射各自独立失效
- **建议**：publisher 模块级 `asyncio.Lock`，入口非阻塞跳过 + 前端可展示的返回；或 DB 层原子 `status='processing'` 领取
- **测试**：并发调用，mock `_send_telegram` 计数断言同一项只发一次

#### B-02｜投稿转存后台续跑与手动重试未共用锁
- **位置**：`main.py:205-231`（`_restore_lock` 仅保护 job）、`:5076-5095`（手动 retry 无锁）；`contribution_restore.py:227-247`
- **后果**：同分享被提交两次转存，目标目录重复文件；DB 唯一约束只防重复建任务不防重复执行
- **建议**：锁提升到 contribution_restore 模块级，三个入口统一 `async with` + 拿锁后重读 status；或 DB 乐观锁 `status='restoring'` + lease_until
- **测试**：并发 retry 与 job，mock `restore_share_to_dir` 计数为 1

#### B-03｜镜像锁残留接管未真正持有底层锁（本次 v0.9.21 代码引入/保留的隐患）
- **位置**：`main.py:71-91`（`_TimedMirrorLock.acquire()` 残留分支）
- **逻辑**：`threading.Lock.acquire(blocking=False)` 失败后，超时接管只覆盖 `_owner/_since/_last_beat` 元数据即 `return True`，**没有获得底层锁所有权**；`threading.Lock` 无 owner 概念，多个逻辑持有者共享同一把锁，`release()`（`:93-98`）由谁调用都能释放，可能释放掉第三方持有者的锁
- **后果**：误判残留（心跳超时但原持有者仍活着）时并发镜像任务同时写库/改文件；`release()` 的 `except RuntimeError: pass` 掩盖问题
- **建议**：接管分支改为 `self._lock.release()` 后阻塞式 `self._lock.acquire(blocking=True)`；根治用带 owner token/fencing token 的锁，旧持有者写操作失效
- **兼容性**：保留「正常长任务持续 heartbeat 不被误回收」语义（STALE=1800s）
- **测试**：模拟心跳超时接管后断言 `self._lock.locked()` 为真且 owner 唯一、并发写无交叉
- **说明**：此项是上一轮 v0.9.21 修复遗留的更深问题，审计如实记录，不隐瞒

#### B-04｜`_async_scan` 纯内存态，重启丢失
- **位置**：`main.py:2113-2179`（`_async_scan` dict + `asyncio.create_task`）
- **后果**：扫描中重启，`running` 状态丢失、无失败原因（扫描断点本身由 `ScanCursor` 持久化可续）
- **建议**：状态写 `ScanTask`/`ResourceScanBatch`，前端读 DB；create_task 加 done callback 写错误

#### B-05｜`_restrict_cache` 无 TTL GC，内存缓慢增长
- **位置**：`bot_listener.py:156`、`:782-807`
- **逻辑**：`_restrict_cache[uid] = (now, ...)` 但过期项只在命中时覆盖、从不删除；对比 `_flow_gc`（`:842-846`）与 `_pick_gc`（`:877-882`）有清理
- **后果**：公开群累计用户数线性膨胀
- **建议**：复用 `_flow_gc` 模式或 `TTLCache(maxsize=...)`；**测试**：造 10 万 uid 断言字典有上界

#### B-06｜单文件 main.py 5287 行 / 128 路由 / 9 job / 全局可变状态
- **位置**：`main.py:1-5287`
- **后果**：合并冲突高、单测需 import 整个模块、全局状态互相牵连，是 A-07/B-01/B-02 的结构性根因
- **建议**：按 domain 拆 `APIRouter`（auth/scan/mirror/resources/queue/interaction/contribution），全局状态迁 service；所有端点 URL 与 `app` 对象不变；评估测试对 `main.xxx` monkeypatch 的影响

#### B-07｜按名+大小回填的身份碰撞
- **位置**：`mirror.py:434-446`、`:1095-1096`、`:838-866`
- **逻辑**：`candidates[f"{normalize_work_name(name)}:{size}"] = fid` 键不含父目录；跨季同名同大小后者覆盖前者；size=0 退化为纯名兜底
- **后果**：unknown/失败行被错误回填到其它季文件的 fileId → 映射错位或重复复制
- **建议**：复合键 `(父目录ID, 归一化名, size)` 优先、扁平键次级回退；收紧纯名兜底
- **测试**：第1季/E01.mkv 与第2季/E01.mkv 同大小，断言回填正确季

#### B-08｜unknown 回填未校验 b_verified
- **位置**：`mirror.py:838-841`（缺失判定要求 `b_verified`）对比 `:869-881`（回填不校验）
- **后果**：列目录 `verified=False`（接口异常/分页截断/超 60 目录）时可能把不在 B 库的行回填为 synced，永久跳过复制
- **建议**：回填前要求 `b_verified`，否则留待下轮；**测试**：mock 返回 `(部分candidates, False)` 断言不回填

#### B-09｜容器重启 taskId 落库窗口
- **位置**：`mirror.py:966-979`（先 `await api.copy_file()` 再 `MirrorTask` add+commit）、`main.py:261-282`（reap 只改 MirrorTask 不改 MirrorFile）
- **后果**：966 之后 979 之前重启 → taskId 永久丢失、MirrorFile 停 copying；下轮重发复制若 B-07 回填失败则产生重复文件
- **建议**：先插 MirrorTask 占位行（task_id 空）再调 copy_file，成功后回填；或 outbox/幂等键
- **测试**：mock copy_file 返回后 commit 前中断进程，重启断言无重复 B 文件

#### B-10｜CI 全局 `contents: write` + 强制公开镜像仓库
- **位置**：`.github/workflows/docker-publish.yml:9-10`、`:51-71`
- **逻辑**：顶层 `permissions: contents: write`；`curl -X PATCH -d '{"is_private":false}'` 每次构建强制公开
- **后果**：build job 获得不必要写权限（供应链风险）；私有部署场景被意外公开
- **建议**：顶层 `contents: read`，仅 release job 写权限；移除或可选化强制 public

#### B-11｜Fernet 解密失败静默回退明文 / 审计日志 detail 未脱敏
- **位置**：`security.py:26-33`（`except InvalidToken: return value`）、`main.py:129-130/178/650`（`add_audit` 直接落 `str(exc)`）
- **后果**：APP_SECRET 变更或密文损坏时把密文当明文用（隐蔽鉴权失败）；异常 URL/token 片段写入 audit_logs 长期保留
- **建议**：明文存量加可识别前缀并迁移、解密失败告警；`add_audit` 统一正则脱敏

#### B-12｜占位符管理员口令可被接受 / 容器 root 运行
- **位置**：`main.py:146-147/477`（仅拦截 `CHANGE_ME`）、`.env.example:10`（`ADMIN_PASSWORD=请设置强密码`）、`Dockerfile:1-20`（无 `USER`）
- **后果**：照示例部署即有公开弱口令；容器逃逸持 root
- **建议**：拒绝已知占位符/长度<12；Dockerfile 建非特权用户并 chown `./db`/`./logs`（飞牛 NAS 需文档说明 UID）

### C 类 — 待验证（静态无法确认，需动态测试）

#### C-01｜SPA catch-all 疑似路径穿越
- **位置**：`main.py:5278-5287`：`f = static / path; FileResponse(f if path and f.is_file() else index.html)`，路由无 ADMIN
- **说明**：Starlette 通常规范化 `..`，静态审计**不能确认**可穿越；定级 C 而非 A
- **验证**：`curl --path-as-is 'http://host:8787/../db/ya-tg.db'` 及 `%2e%2e` 编码，断言返回 index.html/404 而非 db 文件
- **预防性修复**：`f.resolve()` 后 `is_relative_to(static.resolve())` 校验

#### C-02｜`ContributionRestore status='restoring'` 崩溃后是否永久悬挂
- **位置**：`contribution_restore.py:24/76-80/177-180`、`main.py:221`（`pick_pending` 只查 pending/retry_wait）
- **验证**：转存 sleep 中 SIGKILL，重启查 status；若永久 restoring 则加「超时 restoring 重置 pending」兜底

#### C-03｜`with SessionLocal() as db` + `rollback()` 的连接释放
- **位置**：`main.py:198/217/299/326` 多处
- **说明**：SQLAlchemy 1.4/2.0 `Session.__exit__` 会 close 但不自动 commit/rollback；需确认版本行为
- **验证**：压测 job 循环监控 `engine.pool.checkedout()`

#### C-04｜PRAGMA 失败静默吞
- **位置**：`db.py:28-34`（`except Exception: pass`）
- **验证**：只读文件系统下 WAL/busy_timeout 未生效无告警；建议失败项记 warning

### D 类 — 优化建议

| 编号 | 位置 | 建议 |
| --- | --- | --- |
| D-01 | `main.py:376-387` | `_scheduled_scan(root)` 的 `root` 参数已无效（内部重取 DB），去掉或标注 |
| D-02 | `main.py:2129-2143` | `_run_async_scan` 三分支表驱动，减少漂移 |
| D-03 | `runtime_config.py:145-146/168-179` | `get_many`/`source_map` 逐 key 查（~80 key），改 `where(key.in_())` 一次取 |
| D-04 | `publisher.py:47-56` | `ensure_share` 同目录每单集查兄弟分享，加目录级 TTL 缓存（校验 share_status） |
| D-05 | `mirror.py:561-567` | 注释写 "BFS" 实际 `stack.pop()` 是 DFS，修正注释 |
| D-06 | `mirror.py:1095` | `f"{name}:{size}".rstrip(":")` 空名空 size 产生空 key，构造前过滤 |
| D-07 | `App.vue:683-719 load()` | 串行拉 15+ 接口，改 `Promise.allSettled` 并行 + 关键接口失败降级 |
| D-08 | `App.vue` 多保存按钮 | 部分按钮无 loading/防重复提交（如各 config 保存），统一 `:loading` + 提交中禁用 |
| D-09 | `config.py:12` | 默认 DB 路径 `sqlite:///./app.db` 与部署 `/app/db/ya-tg.db` 不一致，改默认或启动打印实际路径 |

---

## 5. 任务书假设 vs 真实代码（镜像引擎逐条核对）

| 任务书要求 | 真实实现 | 结论 |
| --- | --- | --- |
| 6.1 严格顺序复制默认模式 | `mirror.py:961-983`：先 `for copy_file` 全部提交，再统一 `_poll_tasks_batch`；无"前一个完成再提交下一个" | **不一致** |
| 6.2 目录自然排序 | `mirror.py:182/502/567/1068` 全部 `stack.pop()` 按网盘返回顺序；全文无 sort | **不一致** |
| 6.3 文件自然排序 | `mirror.py:574-575` 按接口顺序收集 | **不一致** |
| 6.4 DFS 遍历 | 结构上是栈 LIFO（DFS 形态），但 `:561` 注释写 "BFS"，顺序不确定 | **部分一致** |
| 6.5 固定任务清单 + sequence_id | `models.py:405-417 MirrorTask` 无 sequence_id/batch_id/parent_sequence_id/retry_count；替代是 `works_cursor` 轮转（`mirror.py:212-244/1011-1017`） | **不一致** |
| 6.6 严格顺序执行器 | 目录层级串行有（`mirror.py:780-786 break/813-819 continue/920-934`）；任务级严格顺序无 | **部分一致** |
| 6.7 复制状态机 | `MirrorFile.status` 实为 pending/copying/synced/superseded/failed；`MirrorTask.status` pending/running/done/failed/unknown，无 submitting/submitted/verifying/completed | **不一致** |
| 6.8 unknown 先查远端 | `mirror.py:869-881`：`_lookup_b_file` 命中即回填 synced 并从 retry 删除；`RETRYABLE_FILE_STATUS` 含 unknown（`:113`） | **一致** |
| 6.8 防重复复制 | 超时成功防护有（`:869-881/1100-1129/1870-1882`）、进程内锁有；重启窗口与 DB 失败窗口有缺口（见 B-09） | **部分一致** |
| 6.9 指纹/作品映射兼容 | `size:utime` fingerprint + `tmdb:ID:类型:季:集` identity 保留（`models.py:371-402`） | **一致（保留）** |
| 6.12 增量扫描不插入当前批次 | `mirror.py:884` 每轮重新 `_walk_source` 全树，新增文件直接进本轮 copy_jobs（`:946`）；无批次冻结 | **不一致** |
| 6.11 可选受控并发 | `mirror.py:961-963` 串行 `for ... await copy_file` + idle；仅状态轮询并发 | **不一致** |
| 6.10 重启正确位置恢复 | 作品级 works_cursor 有；任务级需手动 `/api/mirror/tasks/poll`，定时 job 不自动调 | **部分一致** |

**结论**：任务书描述的整套镜像引擎在当前基线不存在。现有引擎实际是「作品级轮转批次 + 作品内批量复制 + 批量轮询 + 每文件立即 commit（无跨 await 写事务）+ B 库按名大小回填防重 + 进程内锁」。这与用户长期维护中形成的稳定性设计（见 CHANGELOG v0.8.x）一致——批量化是为规避串行慢、按名回填是因真实 HAR 证实复制响应不回传副本 ID。**重构为严格顺序引擎会改变核心业务行为，必须与用户确认。**

---

## 6. 需用户决策的重大事项（暂停点）

按任务书「存在需要用户决定的重大架构变更、数据迁移、不可逆操作或功能行为变化，暂停并向用户确认」，以下必须确认后才能进入阶段二：

1. **镜像引擎是否真要重构为严格顺序模式？** 当前批量复制引擎是用户长期迭代的稳定方案。改成严格顺序 + 固定任务清单 + sequence_id 涉及：新增 4+ 个 DB 列（迁移）、改变复制调度行为、改变重试/恢复机制。确认后立即导致：现有 614 例中镜像相关测试需重写、works_cursor 机制被替代。**两种选择**：(a) 维持批量引擎，仅补自然排序 + 修复 B-07/B-08/B-09 等真实缺陷（低风险）；(b) 全量重构为任务书描述的严格顺序引擎（高风险、改动大）。
2. **是否新增"暂停/恢复/跳过/取消批次"操作？** 当前后端无暂停状态机，需新增状态字段与 API，前端新增按钮。
3. **测试规模**：10000 文件性能测试需要隔离的测试网盘或 mock 大数据源，确认用哪种（不碰生产网盘）。
4. **发版节奏**：每阶段独立 commit 已按要求执行；是否每阶段都升版本 tag，还是全部完成后统一发版？

---

## 7. 测试覆盖缺口（任务书第十七部分核对）

| 任务书要求 | 当前状态 | 证据 |
| --- | --- | --- |
| 自然排序测试 | **缺失** | 全仓库 grep `自然排序/natural sort/natsort` 零匹配 |
| DFS 遍历测试 | **缺失** | grep `DFS/深度优先` 零匹配 |
| sequence_id/任务依赖测试 | **缺失** | 字段不存在（A-13） |
| 故障注入（网盘超时/429/401/重启/DB 锁） | **部分** | 仅 `test_v071_scan_pipeline.py:154` 单点；无系统框架 |
| 1000/10000 文件性能测试 | **缺失** | grep `10000/perf/benchmark` 零匹配 |
| 幂等/重试/事务/会话/投稿转存 | 已覆盖 | `test_v0919_contribution_restore.py` 15、`test_v09172_mirror_retry.py` 10、`test_v0920_admin_session.py` 13 等 |
| 全量基线 | 614 passed | 68 测试文件、604 定义 + 10 参数展开 |

---

## 附录 A：API 路由分组清单

见本报告第 3 节统计；完整 128 端点逐条（方法/路径/行号/用途）已在审计过程中取证，将在 FEATURE_PRESERVATION.md 的功能清单中按业务域列出。所有端点 URL 在整改中必须保持不变。

---

*报告生成基线：`d91c4c10e967f5cab9fa6b4748e334f8a088ff52`。阶段一结束，未修改任何业务代码。*

---

## 8. 整改状态更新（阶段二起，原始审计结论不改写）

> 原始第 1~7 节为基线 `d91c4c10` 的审计快照，保持不动。本节记录后续验证/修复的真实结果。

### C-01｜SPA 路径穿越：已确认存在并修复（原定级 C → 升级为已确认）

- **动态验证（用户批准"允许"）**：首次本地 TestClient 因无 `/app/static`、SPA 路由未注册，结果不充分（全部 404），不作为结论。改用隔离环境：临时静态目录 + 同级 `db/ya-tg.db` 敏感哨兵文件 + 真实 uvicorn + `curl --path-as-is` 打未规范化原始路径。
- **验证结果**：`/../db/ya-tg.db`、`/..%2fdb%2fya-tg.db`、`/%2e%2e/db/ya-tg.db`、`/%2e%2e%2fdb%2fya-tg.db`、`/./../db/ya-tg.db` 全部 **HTTP 200 且返回哨兵内容 `<<<SECRET_DB_LEAKED>>>`**；`/assets/../...` 因 StaticFiles 挂载返回 404。证明 `f = static / path` + `f.is_file()` 未做 realpath 归属校验时可穿越读取静态目录外文件——**漏洞成立**（生产容器内 `/app/db/ya-tg.db` 含凭证/数据，风险真实）。
- **修复**：`main.py` 新增模块级 `safe_static_file(path, root)`，对 `(root/path).resolve()` 与 `root.resolve()` 做归属校验（`candidate == root 或 root ∈ candidate.parents`），越界/不存在/符号链接逃逸一律返回 None，SPA 路由改为 `safe_static_file(path, static) or static/index.html`。既保留正常 SPA 路由与 `index.html` 回退，又阻断 `..`、URL 编码、符号链接三类穿越。
- **验证证据**：`tests/test_c01_spa_path_traversal.py` 4 项通过——穿越 payload 全拒、正常静态文件仍可服务、符号链接逃逸拦截、真实 uvicorn + `--path-as-is` 端到端不复现泄露。
- **状态**：C-01 由"待验证" → "已确认 + 已修复 + 已测试"。

### 阶段四｜安全与风控整改（第一批，源码验证中，未发版）

> 原则：功能一致优先。凡与既有真实修复冲突的审计建议（如 busy_timeout 5s）一律不采纳，
> 如实记录冲突理由；完整 async session 迁移属架构债，本批只做可安全落地的缓解。

- **A-09 后台 job 静默吞异常——已修复**：AST 全量扫描 `backend/app/*.py`，把唯一只 `rollback()` 无日志的 4 处补齐：`main.py` `_job_bot_poll` / `_scheduled_scan`、`db.py` 旧队列收敛、`scanner.py` 写失败状态失败，统一 `logger.warning(..., exc_info=True)`。护栏 `tests/test_v102_silent_exception_logging.py` 9 项：再出现静默 rollback 即失败。原 253/316/339/357/201 等处已有日志，未重复改。
- **A-11 优雅关闭——已修**：`lifespan` 末尾 `scheduler.shutdown(wait=True)`，容器停止等待在跑 job 完成，避免队列推送/转存被腰斩。
- **A-06 跨 await 持写事务——已修**：`contribution_restore.execute_restore()` 把 `status=restoring` + attempts 累加在 await 前 `db.commit()` 落盘，不跨网络持 SQLite 写事务。
- **B-01 队列互斥——已修**：`publisher.process_queue` 原实现改名 `_process_queue_impl`，新增进程内 `asyncio.Lock` wrapper，撞锁直接跳过本轮（不并发建分享）。`test_v092_unified_push.py` 同步更新，源码检查同时覆盖 wrapper + impl 防保护失效。
- **B-02 转存互斥——已修**：后台续跑 / 审核通过即时转存 / 手动 retry 共用 `_restore_lock`；撞锁时 retry 端点返回 409、审核 approved 分支标 deferred 不阻断审核态落盘。
- **B-03 镜像锁残留接管——已根治**：v0.9.21 的接管分支只覆盖元数据、未真正取得底层 `threading.Lock` 所有权，误判残留时旧持有者迟到 `release()` 会误放新持有者的锁。改为：接管时先强制释放底层锁再阻塞式重新 `acquire`；并用 `contextvars` fencing token 标记代次，`release()`/`heartbeat()` 只接受当前代次操作，过期持有者迟到操作自动失效。保留 heartbeat 语义（STALE=1800s 不变）。`info()` 增 `fence` 诊断字段。`tests/test_v102_mirror_lock_fencing.py` 5 项 + 历史锁测试 38 项全通过。
- **B-11 审计日志脱敏——已修**：`add_audit()` 统一经 `_mask_audit_detail()`，正则脱敏敏感 kv（token/password/secret/api_key 等）+ Bearer 串，截断 4000 字。
- **B-11 Fernet 失败告警——已修**：`decrypt_secret()` 捕获 `InvalidToken` 时保留 v0.1.0 明文回退（功能一致，测试仍可喂明文），但补 `logger.warning` 告警（只记长度不记内容，绝不泄露凭证）。此前 APP_SECRET 变更/密文损坏时完全静默、把密文当明文用导致隐蔽鉴权失败。护栏 `tests/test_v102_fernet_alert.py` 2 项。
- **B-12 弱管理员口令——已修**：`_WEAK_ADMIN_PASSWORDS` 黑名单（CHANGE_ME / 请设置强密码 / admin123 / password / 123456 等），`check_admin()` 与登录端点命中返回 503；`.env.example` 改 `ADMIN_PASSWORD=CHANGE_ME`。不强加长度限制（避免破坏存量短密码与测试环境）。
- **B-10 CI 最小权限 + Docker 非 root——已修**：workflow 顶层 `permissions: contents: read`，写权限仅 release job 单独授予；强制公开 DockerHub 仓库改为 opt-in（`vars.DOCKERHUB_FORCE_PUBLIC == 'true'`）。Dockerfile 加 `gosu` + 固定 UID/GID 10001 的 `yatg` 用户，`ENTRYPOINT` 以 root 启动仅为 chown 挂载卷（兼容存量 NAS 卷属主）后 `gosu yatg` 降权跑 uvicorn；飞牛 NAS 文档补 rootless 场景手动 `chown` 说明。`tests/test_v102_ci_docker_hardening.py` 5 项。
- **A-07 事件循环阻塞——部分缓解（架构债保留）**：审计建议的「busy_timeout 降到 5s」**明确不采纳**——v0.8.8 把 5s 提到 30s + WAL 是为修复真实 `database is locked`，回退会重现故障。`/api/health` 本就是同步 `def`（FastAPI 走线程池），不受 async DB 阻塞；真正可安全落地的是把两个纯同步 DB job（僵尸自愈扫描 / 日志清理）用 `asyncio.to_thread` 挪出事件循环，Session 在线程内创建（SQLite 安全）。其余 50 个 async 路由多为网络 await + 短 DB 操作，完整 async session 迁移属架构级重构，本批不做以保证功能一致。`tests/test_v102_eventloop_nonblocking.py` 4 项（含真实并发计时器验证 + 守住 30s 不回退）。
- **A-10 消息失败仍推进 offset——已修**：新增 `is_retryable_poll_error()` 纯函数区分瞬时故障（网络超时/限流/DB 锁/429/5xx → 可重试）与业务拒答（非法链接/配置缺失 → 不可重试）。`poll_once` 内层 handle_text 捕获到可重试错误时重抛给外层：可重试且未达上限（5 次）→ 不标记 ProcessedUpdate、不推进 offset、`break` 停止本轮后续 update，下轮同 offset 重投靠幂等跳过已成功项；达上限按毒消息记账放行避免永久卡死。`tests/test_v102_bot_retryable.py` 4 项（含审计指定的「首次 Timeout 二次成功，update 被处理两次」场景）。
- **C-02 转存 restoring 悬挂——已修**：`contribution_restore.reap_stale_restoring()` 把超过 30 分钟仍停在 restoring 的行重置为 retry_wait 并记自愈原因，`pick_pending()` 扫描前先调用（在 `_restore_lock` 内，不与在跑转存并发）；时间比较走 `as_utc()` 规避 SQLite naive/aware 坑。正常在跑任务持续推进 updated_at 不受影响。`tests/test_v102_restore_stale_reap.py` 4 项。
- **阶段四第 8 项「外部服务统一限流/熔断」——评估后不新增抽象层，如实记录**：源码核查确认 5 个外部服务已有各自成熟限流：`guangya.py` 429 读 Retry-After + 指数退避 + `RateLimiter`、`tmdb.py` 按场景独立限流（`TmdbRateLimited`）、`bot_listener.py` 对 TG 409/429/5xx 分类退避且不推进 offset、`publisher.py` 429 不回退 + 队列 `backoff_seconds`、`tgto.py` 降级安全。审计 A/B/C 类未把此项列为具体缺陷。新增全局熔断层会重复现有逻辑、改变已调优的每服务行为，违反「功能一致」原则且无具体失败案例驱动，故本批不动；若后续真机出现某服务未被现有逻辑覆盖的限流故障，再按该具体服务补点，不做预防性大改。

- **仍未处理（后续批次/架构债，非本批遗漏）**：A-04 逐作品串行 TMDB（属性能非安全，且并发会触发限流，保持串行）；B-04 `_async_scan` 内存态重启丢失（需新表，阶段五考虑）；B-06 单文件 main.py 5287 行（需大拆分，触及全部功能，风险过高本批不动）；B-07 按名+大小回填身份碰撞（用户明确不动的回填设计）；B-08/B-09 镜像边界细节（真机验证后再定）；C-03/C-04 连接释放与 PRAGMA（实测无泄漏，观察项）。
