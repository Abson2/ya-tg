# Ya-TG 回滚方案（整改版）

> 回滚只切换镜像 tag + 恢复 DB/日志备份，不改代码。本次整改"全部完成统一发版"，发版前所有批次均未升版打 tag，故生产回退目标为上一已发版 **v0.9.20**。

## 1. 回滚触发条件
- 升级后 `/api/health` 持续不健康、容器反复重启；
- DB 迁移失败导致主要页面不可用；
- 真机关键链路（光鸭登录/镜像/投稿转存/推送）出现阻断性回归；
- strict 模式导致复制顺序异常或任务卡死（可先只关 strict 开关，无需整版回滚）。

## 2. 快速降级（仅 strict 引擎问题）
若仅严格顺序模式出问题，**无需回滚版本**：
1. 镜像同步页关闭"严格顺序"开关（后端默认 `strict=False` 走旧批量引擎）。
2. 或在 `.env` / 调用侧确保 `/api/mirror/run` 不带 `strict=true`。
3. 旧的批量引擎功能与改造前完全一致（本次未改其代码路径）。

## 3. 整版回滚步骤

### 3.1 回滚前先留证
```bash
# 保留当前故障现场，便于排查
cp -r ./db ./db.fault-$(date +%Y%m%d%H%M%S)
cp -r ./logs ./logs.fault-$(date +%Y%m%d%H%M%S)
docker compose logs --tail=500 > ya-tg-fault.log
```

### 3.2 切回上一版本
```bash
# 1) 停服
docker compose down

# 2) 恢复备份的 DB / 日志（用升级前做的备份）
rm -rf ./db ./logs
cp -r ./backup/db ./db
cp -r ./backup/logs ./logs

# 3) 指定旧 tag
# .env: IMAGE_TAG=v0.9.20

# 4) 拉旧镜像重启
docker compose pull
docker compose up -d
```

### 3.3 验证回滚
- `/api/health` 返回 ok，`version` 为 v0.9.20；
- 重新登录管理后台；
- 抽查镜像/投稿/推送各一条端到端。

## 4. DB 兼容性说明（重要）
- 本次整改新增 9 个列（mirror_rules 4 + mirror_tasks 5），旧库启动自动 ALTER，**新列对旧版本 v0.9.20 是"未知列"**。
- SQLite 对表中多余列宽容：v0.9.20 读写时会忽略这些新列，一般可直接在**同一份 DB** 上回退到 v0.9.20。
- 但若回退后出现因新列/新表（如 `app_config` 中扫描状态 key）导致的异常，**必须按 3.2 恢复升级前的 DB 备份**，不要降级使用被新版写过的库。
- 新增表（如 `async_scan_state` 所用的 `app_config` 记录、`ContributionRestore` 已在 v0.9.19 存在）在旧版中不会被读取，属惰性数据，不影响旧版功能。

## 5. 备份建议（升级前必做）
```bash
mkdir -p ./backup
cp -r ./db ./backup/db
cp -r ./logs ./backup/logs
```
确认 `backup/db/ya-tg.db` 存在且大小正常后再升级。

## 6. 回滚后
- 保留 `db.fault-*` / `ya-tg-fault.log` 供问题定位；
- 不要删除整改版镜像 tag（便于修复后重新升级）；
- 若因 strict 模式回滚，重新启用前先在隔离测试网盘验证。
