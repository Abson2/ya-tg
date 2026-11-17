#!/usr/bin/env python3
"""Extract one version section from CHANGELOG.md for use as a GitHub Release body."""
import re
import sys
from pathlib import Path

DOCKERHUB_USER = "abson2"
IMAGE = "ya-tg"


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: changelog_for_release.py <tag> [changelog]", file=sys.stderr)
        return 2
    tag = sys.argv[1].strip()
    path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).resolve().parents[1] / "CHANGELOG.md"
    text = path.read_text(encoding="utf-8")

    # Match "## v0.3.0 ..." up to the next "## v" heading or end of file.
    pattern = re.compile(
        rf"^##\s+{re.escape(tag)}\b[^\n]*\n(?P<body>.*?)(?=^##\s+v|\Z)",
        re.MULTILINE | re.DOTALL,
    )
    m = pattern.search(text)
    body = m.group("body").strip() if m else f"（CHANGELOG 中暂无 {tag} 的条目）"

    # Strip a trailing "### 镜像" section: we render our own pull instructions below.
    body = re.sub(r"\n###\s+镜像\s*\n.*", "", body, flags=re.DOTALL).strip()

    version = tag.lstrip("v")
    notes = f"""{body}

---

## 拉取这个版本

镜像固定版本标签，历史版本不会被覆盖：

```bash
docker pull {DOCKERHUB_USER}/{IMAGE}:{tag}
```

按需修改下面的 `docker-compose.yml`，把 `IMAGE_TAG` 设为 `{tag}` 即可部署指定版本：

```yaml
services:
  ya-tg:
    image: {DOCKERHUB_USER}/{IMAGE}:{tag}
    container_name: ya-tg
    restart: unless-stopped
    ports:
      - "8787:8787"
    env_file: [.env]
    environment:
      TZ: Asia/Shanghai
      DATABASE_URL: sqlite:////app/db/ya-tg.db
    volumes:
      - ./db:/app/db
      - ./logs:/app/logs
```

完整部署步骤见 [README](https://github.com/Abson2/ya-tg#readme)。
"""
    print(notes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
