FROM node:22-alpine AS ui
WORKDIR /ui
COPY frontend/package*.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ .
RUN npm run build

FROM python:3.12-slim
LABEL org.opencontainers.image.title="Ya-TG" \
      org.opencontainers.image.description="光鸭网盘扫描、TMDB 识别与 Telegram 多频道分发"
# v2.1.67（内存）：限制 glibc 的 malloc arena 数量。
# 默认每条「新线程首次分配」都可能开一个新 arena（上限 8 × CPU 核数），
# 每个 arena 独立持有自己的空闲页、各自只在自己的顶层空闲时才归还——多线程
# 交错分配后 RSS 会成倍虚高。本进程 12 个线程、绝大多数时间在等网络/磁盘，
# 限制到 2 个 arena 的锁竞争代价可忽略，而碎片显著收敛。
# 必须在进程启动前生效，所以放 ENV（entrypoint 的 gosu 不会清掉环境变量）。
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TZ=Asia/Shanghai \
    MALLOC_ARENA_MAX=2
WORKDIR /app
# 阶段四 B-12：加 gosu 供 entrypoint 降权；固定 UID/GID 10001 便于 NAS 侧授权。
RUN apt-get update && apt-get install -y --no-install-recommends tzdata gosu && rm -rf /var/lib/apt/lists/*
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/app ./app
COPY --from=ui /ui/dist ./static
# v2.1.8 #241：/app/data 存 Telegram 登录会话（等同账号凭据，只落本地）。
# 主进程经 gosu 以 UID 10001 运行，而 /app 本身属主是 root——若不预先建好
# 并 chown 这个目录，运行期 mkdir 会 PermissionError（且异常被吞掉 →
# 登录功能静默失败）。这里显式建目录 + chown，compose 侧再挂卷持久化。
RUN groupadd -g 10001 yatg && useradd -u 10001 -g yatg -M -s /bin/sh yatg \
    && mkdir -p /app/db /app/logs /app/data \
    && chown -R yatg:yatg /app/db /app/logs /app/data
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
EXPOSE 8787
# entrypoint 以 root 启动（只为 chown 挂载卷），随后 gosu 降权到 yatg 再跑 uvicorn。
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8787"]
