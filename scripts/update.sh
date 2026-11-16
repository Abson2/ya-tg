#!/usr/bin/env bash
# NAS 上从 DockerHub 更新 Ya-TG
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "缺少 .env，请先 cp .env.example .env 并填写。"
  exit 1
fi

docker compose -f docker-compose.hub.yml pull
docker compose -f docker-compose.hub.yml up -d
echo "已更新。查看状态：docker compose -f docker-compose.hub.yml ps"
