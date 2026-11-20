# Ya-TG 安全整改报告（阶段四 + C-01）

> 原则：功能一致优先。凡与既有真实修复冲突的审计建议（如 busy_timeout 5s）一律不采纳并如实记录理由；无接口取证/无失败案例的项不猜实现。
> 详细逐项取证见 `AUDIT_REPORT.md` 第 8 节；本报告为对外交付的安全结论摘要。

## 1. 已确认并修复的漏洞

### C-01 SPA 静态路径穿越（原定级 C → 确认可利用，已修复）
- **验证**：隔离环境真实 uvicorn + `curl --path-as-is` 打未规范化路径，`/../db/ya-tg.db`、`/..%2f`、`/%2e%2e` 等 **HTTP 200 且返回哨兵内容**，证明 `f = static / path` + `f.is_file()` 可穿越读取静态目录外文件。生产容器内 `/app/db/ya-tg.db` 含凭证/数据，风险真实。
- **修复**：`main.py::safe_static_file()` 对 `(root/path).resolve()` 做 realpath 归属校验，越界/不存在/符号链接逃逸一律拒绝，SPA 路由统一走该函数并保留 `index.html` 回退。
- **证据**：`tests/test_c01_spa_path_traversal.py` 4 项端到端通过。

### B-11 审计日志敏感信息泄露（已修复）
`add_audit()` 统一经 `_mask_audit_detail()`，正则脱敏 token/password/secret/api_key 等敏感 kv 与 Bearer 串，截断 4000 字；`decrypt_secret()` 捕获 Fernet `InvalidToken` 补告警（只记长度不记内容），保留明文回退以兼容存量。

### B-12 弱管理员口令（已修复）
`_WEAK_ADMIN_PASSWORDS` 黑名单（CHANGE_ME/请设置强密码/admin123/password/123456/admin/test/root 等），登录与鉴权命中返回 503；`.env.example` 默认改 `CHANGE_ME`。不强加长度限制以免破坏存量短密码与测试环境。

### B-10 CI 权限过大 + 容器 root 运行（已修复）
workflow 顶层 `permissions: contents: read`，写权限仅 release job 单独授予；强制公开 DockerHub 仓库改 opt-in。`Dockerfile` 加固定 UID/GID 10001 的 `yatg` 用户 + `gosu`，entrypoint 以 root 仅做卷 chown 后降权跑 uvicorn（兼容存量 NAS 卷属主）。

## 2. 已修复的并发/一致性风险

| 编号 | 风险 | 修复 | 证据 |
| --- | --- | --- | --- |
| B-03 | 镜像锁残留接管未真正持锁，旧持有者迟到 release 误放新锁 | 强制释放后阻塞式重新 acquire + contextvars fencing token 标记代次，过期 release/heartbeat 失效 | `test_v102_mirror_lock_fencing.py` |
| B-01 | 手动/定时队列推送并发重复建分享 | `process_queue` 加进程内 `asyncio.Lock`，撞锁跳过本轮 | `test_v102_queue_lock.py` |
| B-02 | 投稿转存三入口并发重复转存 | 共用 `_restore_lock`，撞锁 retry 返回 409 / 审核标 deferred 不阻断 | `test_v102_restore_lock.py` |
| A-06 | 跨 await 持 SQLite 写事务 | `restoring` 状态 await 前 commit 落盘 | 阶段四源码验证 |
| C-02 | 转存 restoring 进程中断悬挂 | 超 30min 重置 retry_wait 自愈 | `test_v102_restore_stale_reap.py` |
| A-11 | 容器停止腰斩在跑任务 | `scheduler.shutdown(wait=True)` | 阶段四源码验证 |
| A-09 | 后台 job 静默吞异常 | 4 处静默 rollback 补日志 + AST 护栏 | `test_v102_silent_exception_logging.py` |

## 3. Bot/消息可靠性
- **A-10**：新增 `is_retryable_poll_error()` 区分瞬时故障（网络/429/5xx/DB锁 → 可重试不推进 offset，5 次上限）与业务拒答（401/非法链接/配置缺失 → 记账放行不卡死）。
- **阶段五分级**：401/403/Bot 被移出群/blocked/chat not found 判永久错误直接 failed 不重试，省配额避免队列假活；429/网络仍退避。
- **阶段七故障注入**：`test_v105_fault_injection.py` 10 项覆盖超时/429/401/DB锁/网络断/幂等/续跑，全部通过。

## 4. 评估后不采纳（如实记录，非遗漏）
- **A-07 busy_timeout 降到 5s**：不采纳。v0.8.8 把 5s 提到 30s + WAL 是为修复真实 `database is locked`，回退会重现故障。改用 `asyncio.to_thread` 把两个纯 DB job 挪出事件循环缓解。`test_v102_eventloop_nonblocking.py` 含"守住 30s 不回退"断言。
- **分享远端核验**：不实现。`GuangYaShareClient` 无「列已有分享」接口，shareId 带随机后缀无法从 fileId 推导；猜接口违反"不猜接口"红线。现有分享幂等 + 作品级去重 + 队列锁已覆盖主要重复场景。
- **完整 async session 迁移**：架构级重构触及全部路由，本批不做以保证功能一致。

## 5. 待闭环 / 观察项
- Token 曾明文入对话，建议轮换（非本次代码可修复）。
- 投稿转存跨账号成功真机样本、TgtoDrive 白名单真机写入仍待实测。
- strict 模式真机端到端安全边界（顺序/锁/续跑）需测试环境实跑确认。
