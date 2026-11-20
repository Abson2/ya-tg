# Ya-TG 全项目整改变更总览（优化版 Changelog）

> 与 `CHANGELOG.md`（逐正式版本）分工：本文件汇总"源码审计与全项目整改"这一批次的全部改动，按阶段组织，便于一次性审阅。
> 审计基线 `d91c4c10`；全部改动以 `[skip ci] commit` 推 main，**统一发版前未升版、未打 tag**。

## 阶段一：源码审计（commit `b03d2df`）
- 交付 `AUDIT_REPORT.md` / `FEATURE_PRESERVATION.md` / `REFACTOR_PLAN.md`；锁定基线 `d91c4c10`；不改业务代码。
- 核心结论：任务书要求的严格顺序引擎原源码未实现，真实实现是"作品级轮转批次 + 作品内批量复制 + 按名+大小回填 + 进程内锁"。

## 阶段二：路线 B 严格顺序镜像引擎（`2828d24` → `ac04bd2`）
- **C-01 路径穿越**：确认真实可利用并修复（realpath 归属校验）。
- **C1 迁移骨架**：mirror_rules 4 列 + mirror_tasks 5 列，旧库 ALTER 兼容。
- **C2 确定性排序**：`natural_key` 自然排序注入 `_walk_source`/`_discover_works`/`_media_files` 三处。
- **C3 严格顺序执行器**：`sync_rule(strict=False)` 开关；逐文件"远端核验→占位→copy_file→轮询终态→下一个"，每步 commit 不跨 await 持写事务；暂停/跳过/取消在任务边界生效。
- **C4 批次控制 API**：`/api/mirror/batch/status`、`/api/mirror/batch/control?action=pause|resume|skip|cancel`、`/api/mirror/run?strict=true`；前端严格顺序开关 + 四按钮（二次确认）。**不适用 `retry_current`（后端无此 action）**。
- **C5 定时续跑**：60s job `_job_mirror_task_poll` 自动续跑未终结任务。
- **红线保留**：默认走旧批量引擎（回退路径零破坏）；英文名四层解析、按名+大小回填、每文件立即 commit、不群推全未动。

## 阶段三：性能优化（`28b42e9` → `ebe4972`）
- N+1 → 批量 map（投稿/队列/审核列表）；日志统计 28→7 次 SQL。
- `runtime_config.get_many/source_map` 批量读取；前端 `load()` 22 GET 改 `Promise.allSettled` 并行。
- `_restrict_cache` TTL + 硬上限 GC；10000 文件排序/遍历 <10ms（见 PERFORMANCE_REPORT.md）。

## 阶段四：安全与风控（`1a3dff9` → `b0fd2b8`）
- 镜像锁 fencing token（B-03）、队列锁（B-01）、转存锁（B-02）。
- 审计日志脱敏 + Fernet 失败告警（B-11）、弱口令黑名单（B-12）。
- CI 最小权限 + Docker 非 root（B-10）、后台 job 静默异常补日志（A-09）。
- Bot 瞬时失败不推进 offset（A-10）、转存跨 await 不持写事务（A-06）、优雅关闭（A-11）。
- 两个纯 DB job 走 `asyncio.to_thread`（A-07，**busy_timeout 30s 不回退**）、转存 30min 自愈（C-02）。
- 限流/熔断：评估后不新增全局抽象层（各服务已有成熟限流，见 SECURITY_REPORT）。

## 阶段五：业务闭环（`ebad73b`）
- 扫描状态落 DB + 重启恢复（`async_scan_state.py`，B-04）。
- 求片 TMDB 元数据 24h TTL + 500 上限缓存（B-07，失败不缓存）。
- Bot 401/403/被移出群判永久错误不重试，429 仍退避。
- 分享远端核验：无接口取证，评估后不实现（如实记录）。

## 阶段六：UI 升级（`6568137`）
- 深色主题（Element Plus 官方暗色变量 + `html.dark` 附加层，浅色默认不变）。
- 任务中心：第 10 个入口，8 类任务纯前端聚合，**零新增 API、不造假数**，点明细跳原页。
- 9 个原 tab + 全部操作入口保留（双重测试护栏）；不虚构"重试当前"按钮。

## 阶段七：测试与交付（本批次）
- 故障注入套件 `test_v105_fault_injection.py` 10 项（超时/429/401/DB锁/网络断/幂等/续跑）。
- 交付文档：`TEST_REPORT` / `PERFORMANCE_REPORT` / `SECURITY_REPORT` / `DEPLOYMENT` / `ROLLBACK` + 已有 `AUDIT_REPORT` / `FEATURE_PRESERVATION` / `STRICT_SEQUENCE_DESIGN`。
- 全量回归 741 passed（含故障注入）；前端 build 通过。

## 四模块重构（新批次，基线 `1437b95`）
- 设计先行：先交付 `FOUR_MODULE_REFACTOR_AUDIT.md`（调用链 + 数据库关系 + 四强制审计项）+ `FOUR_MODULE_REFACTOR_DESIGN.md`（四模块职责 + 事件模型 + 迁移顺序 + 验收用例），审计阶段不改业务代码。
- 阶段A 数据层：新增 `share_tasks` 表 + `push_queue` 6 个交接列 + `distribution_events` 2 个溯源列（`db._COLUMNS` 幂等 ALTER）；作品键统一 `utils.work_group_key`。
- 阶段A 分享中心模块 `share_center.py`：任务状态机 planned→ready→submitted→accepted→published/failed/cancelled；first 整剧唯一、update 复用原链接独立任务、force 不去重；`run_cycle` 建分享+提交。
- 阶段B 触发点迁移：`_enqueue_ready`/`process_change_events`/`publish_resource` 影子登记 ShareTask（异常吞掉不影响主链）；调度 job `_job_share_center`（开关关闭零开销）；灰度开关 `share_center_enabled`（runtime_config 注册，默认 false）。
- 阶段C 交接闭环：publisher 开关开启时只消费 accepted 队列项、只读 ShareTask URL 不反建、回执 published/failed；`_reconcile_library` 出库取消 ShareTask（规则2）；`claim_tasks` 原子领取（强制项4）。
- 阶段D 前端：导航「推送队列」改名「分享中心」并上移到光鸭资源组（key 保留兼容断言）；新增分享任务表格 + 跑一轮/取消操作；任务中心 push 卡片改名。
- 后端新 API：`GET /api/share/tasks`、`POST /api/share/tasks/{id}/cancel`、`POST /api/share/run`、`GET /api/share/status`。
- 新增测试 16 项（test_v106_* 四文件）；全量回归 757 passed（741 基线 +16）；前端 build 通过。
- **灰度默认关闭**：`share_center_enabled=false` 时新旧链行为与改造前完全一致（零回归），需真机验证后再开启新链。

## 功能一致性声明
- 默认行为不变：`strict=False` 走旧引擎；英文名解析与按名+大小回填用户明确不动；分享=推送粒度红线、目录分享、白名单闸门、Bot 三件事边界均未改。
- 新增能力均为可选/附加：严格顺序模式（主动开启）、深色主题（主动切换）、任务中心（聚合视图无副作用）、**四模块分享中心新链（灰度开关默认关闭）**。
- 逐功能保护清单见 `FEATURE_PRESERVATION.md`。
