#!/bin/sh
# 阶段四 B-12：以非特权用户运行容器，同时兼容已有的 bind mount 目录。
# 背景：./db 与 ./logs 是宿主机挂载目录，历史部署的属主可能是 NAS 用户/旧版 root。
# 容器启动时仍以 root 进入本脚本，先把这两个目录统一 chown 给 yatg，再 gosu 降权
# 启动主进程——既消除容器 root 运行风险，又不破坏存量卷的写入（升级不丢数据）。
# 飞牛 NAS 若启用 rootless/user-namespace 映射，容器内 root 无权 chown 宿主目录，
# 需在宿主机先执行 `chown -R 10001:10001 ./db ./logs`（见飞牛 NAS 部署文档）。
#
# v2.1.8 #241：新增 /app/data（Telegram 登录会话目录）。
# 它的存在很关键——主进程以 UID 10001 运行，而 /app 属主是 root，
# 若目录不存在，运行期 mkdir 会 PermissionError；异常被吞掉后
# 登录功能会**静默失败**（页面一直显示「未就绪」且不报错）。
# 这里确保目录存在并把属主交给 yatg。
set -e

mkdir -p /app/data 2>/dev/null || true

chown -R yatg:yatg /app/db /app/logs /app/data 2>/dev/null || true

exec gosu yatg "$@"
