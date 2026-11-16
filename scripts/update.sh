#!/usr/bin/env bash
# 更新 Ya-TG 到指定版本或最新版（在 docker-compose.yml 所在目录执行）
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "缺少 .env，请先 cp .env.example .env 并填写。"
  exit 1
fi

if [ -n "${1:-}" ]; then
  echo "切换镜像版本到 $1"
  if grep -q '^IMAGE_TAG=' .env; then
    sed -i.bak "s/^IMAGE_TAG=.*/IMAGE_TAG=$1/" .env
    rm -f .env.bak
  else
    echo "IMAGE_TAG=$1" >>.env
  fi
fi

docker compose pull
docker compose up -d --remove-orphans
echo "已更新。当前版本：$(grep '^IMAGE_TAG=' .env || echo 'IMAGE_TAG=latest（默认）')"
echo "查看状态：docker compose ps"
