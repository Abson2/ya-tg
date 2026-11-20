#!/bin/sh
# 阶段四 B-12：以非特权用户运行容器，同时兼容已有的 bind mount 目录。
# 背景：./db 与 ./logs 是宿主机挂载目录，历史部署的属主可能是 NAS 用户/旧版 root。
# 容器启动时仍以 root 进入本脚本，先把这两个目录统一 chown 给 yatg，再 gosu 降权
# 启动主进程——既消除容器 root 运行风险，又不破坏存量卷的写入（升级不丢数据）。
# 飞牛 NAS 若启用 rootless/user-namespace 映射，容器内 root 无权 chown 宿主目录，
# 需在宿主机先执行 `chown -R 10001:10001 ./db ./logs`（见飞牛 NAS 部署文档）。
set -e

chown -R yatg:yatg /app/db /app/logs 2>/dev/null || true

exec gosu yatg "$@"
