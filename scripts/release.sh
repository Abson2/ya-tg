#!/usr/bin/env bash
# Ya-TG 首次发布：初始化 git 仓库并推送 main + version tag 到 GitHub
set -euo pipefail

VERSION="$(grep -m1 -o 'version="[0-9.]*"' backend/app/main.py | head -1 | cut -d'"' -f2 || true)"
VERSION="${VERSION:-0.3.0}"
TAG="v${VERSION}"

cd "$(dirname "$0")/.."

if ! command -v git >/dev/null 2>&1; then
  echo "未找到 git，请先安装：xcode-select --install（macOS）"
  exit 1
fi

if [ -z "$(git config user.name || true)" ] || [ -z "$(git config user.email || true)" ]; then
  echo "请先配置 git 身份："
  echo '  git config --global user.name "你的名字"'
  echo '  git config --global user.email "你的邮箱"'
  exit 1
fi

if [ -d .git ]; then
  echo "检测到已有 git 仓库，跳过初始化。"
else
  read -rp "GitHub 仓库 HTTPS 地址（例如 https://github.com/yourname/ya-tg.git）: " REPO
  if [ -z "$REPO" ]; then echo "仓库地址不能为空"; exit 1; fi
  git init -b main
  git remote add origin "$REPO"
fi

git add -A
if git diff --cached --quiet; then
  echo "没有需要提交的变更。"
else
  git commit -m "release: Ya-TG ${VERSION}"
fi

echo "推送 main 到 GitHub…"
git push -u origin main

if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "标签 $TAG 已存在，跳过打标。"
else
  git tag -a "$TAG" -m "Ya-TG ${VERSION}"
fi
echo "推送标签 $TAG…"
git push origin "$TAG"

echo "完成。请在 GitHub 仓库 Settings → Secrets and variables → Actions 添加："
echo "  DOCKERHUB_USERNAME / DOCKERHUB_TOKEN"
echo "然后到 Actions 页确认镜像构建推送成功。"
