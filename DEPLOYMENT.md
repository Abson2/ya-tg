# Ya-TG 部署与升级说明（整改版）

> 部署方式不变：直接拉取 DockerHub 官方镜像，无需源码、无需构建。本次整改未引入新的部署依赖、端口、卷或环境变量。

## 1. 标准部署

把 `docker-compose.yml` 与 `.env` 放在同一目录：

```bash
docker compose pull
docker compose up -d
```

- 镜像：`abson2/ya-tg:${IMAGE_TAG:-latest}`
- 端口：`${WEB_BIND:-0.0.0.0}:${WEB_PORT:-8787}:8787`
- 卷：`./db:/app/db`（数据库）、`./logs:/app/logs`（日志）、`./data:/app/data`（Telegram 登录会话，v2.1.8 起）
- 时区：`Asia/Shanghai`；数据库：`sqlite:////app/db/ya-tg.db`
- 健康检查：`/api/health`，30s 间隔 / 3 次失败
- 日志限额：`json-file` 10m × 3

## 2. 本次整改对部署的影响

| 变更 | 影响 | 操作 |
| --- | --- | --- |
| DB 新列（阶段二 C1：mirror_rules 4 列 / mirror_tasks 5 列） | 旧库启动时自动 `ALTER` 兼容，旧数据保留 | 无需手动迁移，启动即完成 |
| Docker 非 root（阶段四 B-10） | entrypoint 以 root 仅做一次卷 `chown`（UID/GID 10001）后 `gosu` 降权 | 首次升级若 NAS 卷属主异常，按下方"飞牛 NAS"手动 chown |
| 管理后台会话（v0.9.20） | 会话为进程内存态 | 重启容器后需重新登录（预期行为） |
| strict 严格顺序引擎 | 默认关闭（`sync_rule(strict=False)`），旧批量引擎不变 | 需主动在镜像同步页打开"严格顺序"开关才启用 |

## 3. 飞牛 NAS / 非 root 卷属主
若升级后容器报数据库/日志目录权限错误（自定义 NAS 卷属主非 root）：

```bash
mkdir -p ./db ./logs ./data
chown -R 10001:10001 ./db ./logs ./data
docker compose restart
```

entrypoint 已尝试自动 chown，多数场景无需手动；仅特殊权限环境执行上述命令。

> `./data` 存 Telegram 登录会话（等同账号凭据）。目录不存在或属主不对时，
> 运行期建目录会 `PermissionError` 且错误被吞掉——表现为「阅读量」页永远显示
> 「未就绪」而没有任何提示。其它功能不受影响。

## 4. 升级步骤（从 v0.9.20 / v0.9.21 源码验证版）
1. **备份**当前 `./db` 与 `./logs`（见 ROLLBACK.md）；`./data` 存登录会话凭据，
   按需一并备份（不备份只是升级后要重新走一次验证码登录）。
2. `.env` 设目标 `IMAGE_TAG=<本次发版版本>`（统一发版后填入实际版本号）。
3. `docker compose pull && docker compose up -d`。
4. 观察启动日志与 `/api/health`；确认 DB 迁移无报错。
5. 重新登录管理后台（会话内存态）。
6. **真机关键链路验证**（见 TEST_REPORT 第 4 节）通过后再投入生产使用：光鸭登录、镜像一轮、投稿转存、求片白名单、会话过期、批次控制四按钮。
7. strict 模式建议先在隔离测试网盘跑通一轮，再决定是否在生产启用。

## 5. 配置项核对
- `ADMIN_PASSWORD`：不得为弱口令（黑名单命中返回 503），务必改为强密码。
- TMDB / AI 配置：英文名解析四层兜底依赖；缺失不影响主流程但待翻译项会标注原因。
- 投稿转存：需在「分发与互动中心」配置开关 + 目标目录 fileId（v0.9.19 起走光鸭原生 API，不经 TgtoDrive）。
- 会话未活跃有效期：`ADMIN_IDLE_MINUTES`（默认 120 分钟）。
