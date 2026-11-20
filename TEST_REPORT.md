# Ya-TG 全项目整改测试报告（阶段七）

> 基线：整改全批次 `[skip ci]` 提交后、统一发版前。本报告以**真实测试结果**为准，不虚构通过率、不以 build 成功代替功能通过。
> 测试环境：隔离 venv（Python 3.13），SQLite 临时库，FakeAPI mock 网盘/TG，**不碰生产网盘、不发真实网络请求**。

## 1. 全量回归结果

| 阶段 | commit | 测试数 | 说明 |
| --- | --- | --- | --- |
| 审计基线 d91c4c10 | — | 614 | 阶段一审计，未改业务代码 |
| C-01 路径穿越修复 | `2828d24` | 618 | +4 穿越端到端 |
| 阶段二 C1-C5 | `30807f7`→`ac04bd2` | 645 | 严格顺序引擎+迁移+排序+批次API+续跑 |
| 阶段三 N+1/批量/缓存 | `28b42e9`→`ebe4972` | 655 | 查询优化+前端并行 |
| 阶段四安全风控 | `1a3dff9`→`b0fd2b8` | 707 | 锁 fencing/脱敏/弱口令/Docker 加固 |
| 阶段五业务闭环 | `ebad73b` | 718 | 扫描状态落DB/TMDB缓存/Bot错误分级 |
| 阶段六 UI 升级 | `6568137` | 731 | 深色主题+任务中心+故障注入前置 |
| 阶段七故障注入 | 待提交 | 741 | +10 故障注入/幂等/续跑测试 |

**最终全量：741 passed**（阶段七提交后）。

**稳定性抽查（如实记录，已修复）**：阶段七期间连续跑全量，其中 1 次出现 `test_v098_tmdb_candidates.py` 3 项偶发失败（`test_want_pick_lands_tmdb_fields` / `test_contribute_pick_then_link` / `test_pick_cancel_and_skip_clear_session`）。单独跑该文件 14/14、与相关 bot 测试组连跑 3 次均 68/68，判定为**共享 temp DB + 全局会话状态在整包顺序下的测试隔离问题，非产品功能回归**（该测试文件本次整改未改产品代码）。**修复**：给 v098 的 `session` fixture 补齐 `_flow_sessions` / `_AUTO_CLEARED_TOKENS` 清理，与同域 v0910/v0912/v0916 对齐（仅测试代码，不碰产品）。修复后连续 2 次全量均 **741 passed**，偶发消除。

## 2. 分域覆盖

### 2.1 严格顺序镜像引擎（阶段二）
- `test_v100_natural_sort.py`：natural_key 数字顺序/混合文本、`_media_files`/`_discover_works`/`_walk_source` 三处自然序注入。
- `test_v100_strict_sequence.py`：严格顺序（下一 copy 必须等上一 poll-done）、自然序复制、每文件 commit + 序列落库、暂停边界、跳过/取消、未知状态保留。
- `test_v100_mirror_batch.py`：批次 ID 唯一、开/收批次、控制状态机、序列进度与续跑、旧库空 task_status 兼容。
- `test_v100_batch_api.py`：status 无规则/空闲、control 四 action、非法 action 400、前端控件静态检查。
- `test_v100_restart_resume.py`：续跑 job 注册/lock busy 跳过/空闲执行。
- `test_v100_strict_migration.py`：新列声明、旧库迁移保数据、新库列存在。

### 2.2 故障注入（阶段七，`test_v105_fault_injection.py`，10 项）
- **超时/网络断**：`httpx.TimeoutException`/`ConnectError`/`NetworkError`/`asyncio.TimeoutError` 归可重试。
- **429/5xx**：429/500/502/503 及 "too many requests" 文案归可重试。
- **DB 锁**：`OperationalError(database is locked/busy)` 归可重试。
- **401/业务拒答**：401/403/非法链接/配置缺失归不可重试（不卡 offset）。
- **执行器注入**：copy 超时→单行 failed 不中断整批；远端已存在（按名+大小）→幂等跳过；终态未知→任务复位 pending 供续跑、不误标 done；配置缺失→记 failed+error 不崩。
- **续跑/状态机**：paused 批次游标保留 + resume 恢复 running；非法 action 拒绝；`retry_current` 不在后端 action 集合（防止前端虚构按钮）。

### 2.3 安全风控（阶段四/五）
- `test_c01_spa_path_traversal.py`：真实 uvicorn + `--path-as-is` 端到端，穿越 payload 全拒、正常文件可服务、符号链接逃逸拦截。
- `test_v102_mirror_lock_fencing.py`：fencing token 代次、残留接管真持锁、过期 release/heartbeat 失效。
- `test_v102_queue_lock.py` / `test_v102_restore_lock.py`：队列/转存进程内互斥。
- `test_v102_weak_password.py` / `test_v102_audit_mask.py` / `test_v102_fernet_alert.py`：弱口令拒绝、审计脱敏、解密失败告警。
- `test_v102_ci_docker_hardening.py`：CI 权限最小化 + Docker 非 root。
- `test_v102_bot_retryable.py` / `test_v103_bot_error_classification.py`：瞬时故障不推进 offset、永久错误分级。
- `test_v102_restore_stale_reap.py` / `test_v103_async_scan_state.py`：悬挂转存自愈、扫描状态重启恢复。
- `test_v102_silent_exception_logging.py`：AST 护栏，静默 rollback 复现即失败。

### 2.4 UI 护栏（阶段六）
- `test_v104_dark_theme.py`：9 原 tab 全保留、批次四按钮保留、主题开关、附加层非改写、5 移动断点、危险确认+防重。
- `test_v104_task_center.py`：9 原 tab 保留、任务中心入口、8 类齐全、只用已加载 ref 无新 API、每类标注来源、明细跳转、响应式+深色。

## 3. 前端构建
`npm run build`：1450 modules transformed，产物 CSS 379.75 kB / JS 1,107.89 kB；仅 chunk >500 kB 既有警告（单文件 App.vue 结构，非本次引入），不影响构建与运行。

## 4. 未执行 / 待真机验证（明确标注，不冒充通过）
- **strict 严格顺序模式真机端到端**：仅隔离 mock 验证过事件序列/顺序/控制，**未在真实网盘跑完整作品**。需在隔离测试网盘跑一轮（复制+改名+入库+分享+推送）确认与旧引擎功能一致。
- **关键链路真机逐项**：光鸭登录、真实 A/B 镜像、投稿审核转存、求片白名单写入、会话过期重登、暂停/恢复/跳过/取消按钮实机操作。
- **分享远端核验**：无「列已有分享」接口取证，未实现（见 SECURITY_REPORT / AUDIT_REPORT 评估结论）。
- 上述项**不得以单元测试或 build 成功替代**；真机验证由部署后在测试环境执行，结果回填本报告后再发版。

## 5. 复现命令
```bash
PY=/Users/axs/.workbuddy/binaries/python/envs/default/bin/python
$PY -m pytest tests/ -q
$PY tests/perf_strict_baseline.py
cd frontend && /Users/axs/.workbuddy/binaries/node/versions/22.22.2-6/bin/npm run build
```
