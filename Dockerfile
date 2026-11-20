FROM node:22-alpine AS ui
WORKDIR /ui
COPY frontend/package*.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ .
RUN npm run build

FROM python:3.12-slim
LABEL org.opencontainers.image.title="Ya-TG" \
      org.opencontainers.image.description="光鸭网盘扫描、TMDB 识别与 Telegram 多频道分发"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TZ=Asia/Shanghai
WORKDIR /app
# 阶段四 B-12：加 gosu 供 entrypoint 降权；固定 UID/GID 10001 便于 NAS 侧授权。
RUN apt-get update && apt-get install -y --no-install-recommends tzdata gosu && rm -rf /var/lib/apt/lists/*
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/app ./app
COPY --from=ui /ui/dist ./static
RUN groupadd -g 10001 yatg && useradd -u 10001 -g yatg -M -s /bin/sh yatg \
    && mkdir -p /app/db /app/logs \
    && chown -R yatg:yatg /app/db /app/logs
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
EXPOSE 8787
# entrypoint 以 root 启动（只为 chown 挂载卷），随后 gosu 降权到 yatg 再跑 uvicorn。
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8787"]
