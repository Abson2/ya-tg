#!/usr/bin/env python3
"""从浏览器 HAR 里提取接口结构，自动脱敏后生成可安全分享的摘要。

用法：
    python3 scripts/har_extract.py 光鸭.har
    python3 scripts/har_extract.py 光鸭.har --only copy,rename,delete,create
    python3 scripts/har_extract.py 光鸭.har -o 接口清单.md --host guangyapan.com

默认只保留光鸭域名的请求，输出 method / URL / 请求体 / 响应体。
Authorization、Cookie、token、手机号等一律替换为 <redacted>；
fileId / taskId / parentId 等业务 ID 保留，因为写接口的实现需要它们。

v0.8.1：
- 支持 content.encoding=base64 的响应（JS bundle / 部分 JSON 常这样存）；
- 从解码后的前端 JS 里额外汇总 /userres/v1/ 路径（只有路径，**不是请求体证据**）；
- 清洗控制字符、跳过图片/字体等二进制响应体，保证输出是可读文本；
- 生成后做一次「疑似凭证残留」自检，命中时在文件头打醒目警告。

**原始 HAR 含登录凭证，不要提交到仓库；只分享本脚本生成的脱敏摘要。**
"""
from __future__ import annotations

import argparse
import base64
import binascii
import json
import re
import sys
from pathlib import Path

SENSITIVE_KEY = re.compile(
    r"authorization|cookie|set-cookie|token|access_token|refresh_token|did|"
    r"sign|secret|password|api[_-]?key|smid|x-client-id|phone|mobile|"
    r"verify[_-]?code|sms[_-]?code|session",
    re.I,
)
JS_API_PATH = re.compile(r"/userres/v1/[A-Za-z0-9_/]+")
BINARY_MIME = re.compile(
    r"image/|font/|audio/|video/|octet-stream|application/zip|wasm|application/pdf",
    re.I,
)
LEAK_PATTERNS = [
    ("JWT", re.compile(r"eyJ[A-Za-z0-9._\-]{20,}")),
    ("手机号", re.compile(r"\b1[3-9]\d{9}\b")),
    ("Bearer 凭证", re.compile(r"Bearer\s+(?!<redacted>)\S{20,}")),
]
REDACTED = "<redacted>"


def scrub(value, key: str = ""):
    """递归脱敏：按字段名命中，或按值形态（Bearer / JWT / 手机号）命中。"""
    if isinstance(value, dict):
        # HAR 的 headers 是 [{"name": ..., "value": ...}]，需按 name 单独处理
        if "name" in value and "value" in value and SENSITIVE_KEY.search(str(value["name"])):
            return {"name": value["name"], "value": REDACTED}
        return {k: scrub(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [scrub(x, key) for x in value]
    if key and SENSITIVE_KEY.search(key):
        return REDACTED
    if isinstance(value, str):
        value = re.sub(r"(?i)Bearer\s+\S+", "Bearer " + REDACTED, value)
        value = re.sub(r"eyJ[A-Za-z0-9._\-]{10,}", REDACTED + "-jwt", value)
        value = re.sub(r"\b1[3-9]\d{9}\b", REDACTED + "-phone", value)
    return value


def clean_url(url: str) -> str:
    return re.sub(r"((?:token|access_token|code|sign|did|smid|api[_-]?key)=)[^&\s]+",
                  r"\1" + REDACTED, url or "", flags=re.I)


def clean_text(text: str) -> str:
    """去掉 NUL 等不可打印控制字符，避免摘要被编辑器当成二进制文件。"""
    return "".join(
        ch for ch in text
        if ch in "\n\t\r" or (32 <= ord(ch) < 127) or ord(ch) >= 160
    )


def content_text(content: dict) -> str:
    """HAR 的 content.text 可能是 base64；解码并清洗后再交给 parse_body / JS 路径扫描。"""
    text = content.get("text") or ""
    if text and str(content.get("encoding") or "").lower() == "base64":
        try:
            text = base64.b64decode(text).decode("utf-8", errors="replace")
        except (binascii.Error, ValueError):
            return ""
    return clean_text(text)


def parse_body(text: str, mime: str = ""):
    if not text:
        return ""
    if "json" in (mime or "").lower() or text.lstrip().startswith(("{", "[")):
        try:
            return scrub(json.loads(text))
        except (ValueError, TypeError):
            pass
    return scrub(text)


def fmt(value, limit: int = 4000) -> str:
    if not value:
        return "（空）"
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False, indent=2)
    else:
        text = clean_text(str(value))
    if len(text) > limit:
        text = text[:limit] + f"\n…（已截断，原始长度 {len(text)} 字符）"
    return f"```json\n{text}\n```" if isinstance(value, (dict, list)) else f"```\n{text}\n```"


def extract(path: Path, host: str, only: list[str]) -> tuple[str, int, int]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    entries = raw.get("log", {}).get("entries", []) if isinstance(raw, dict) else []
    lines = [
        "# 接口记录摘要（已脱敏）",
        "",
        f"- 来源文件：`{path.name}`",
        f"- 域名过滤：`{host}`",
        f"- 关键词过滤：{', '.join(only) if only else '无'}",
        "- 敏感字段（Authorization / Cookie / token / 手机号 / JWT）已替换为 `<redacted>`",
        "- 业务 ID（fileId / taskId / parentId 等）为接口实现所需，予以保留",
        "- `content.encoding=base64` 的响应已自动解码；图片/字体等二进制响应体不输出内容",
        "",
    ]
    kept, total, decoded = 0, len(entries), 0
    js_paths: set[str] = set()
    for entry in entries:
        req = entry.get("request") or {}
        resp = entry.get("response") or {}
        url = clean_url(str(req.get("url") or ""))
        if host and host.lower() not in url.lower():
            continue
        if only and not any(k.lower() in url.lower() for k in only):
            continue
        kept += 1
        method = str(req.get("method") or "GET").upper()
        headers = scrub(req.get("headers") or [])
        post = req.get("postData") or {}
        body = parse_body(post.get("text") or "", post.get("mimeType") or "")
        content = resp.get("content") or {}
        mime = str(content.get("mimeType") or "")
        is_b64 = str(content.get("encoding") or "").lower() == "base64"
        raw_text = content_text(content)
        if is_b64:
            decoded += 1
        # 前端 JS bundle 里常藏着完整接口清单；只有路径，不是请求体证据
        js_paths.update(JS_API_PATH.findall(raw_text))
        if BINARY_MIME.search(mime):
            resp_body = "（二进制响应体，按安全策略不输出内容）"
        else:
            resp_body = parse_body(raw_text, mime)
        lines += [
            f"## {kept}. {method} {url}",
            "",
            f"- 响应状态：`{resp.get('status', '?')}` / `{mime or '?'}`"
            + ("（base64，已解码）" if is_b64 else ""),
            "- 请求头：",
            "",
            fmt(headers, 1200),
            "",
            "- 请求体：",
            "",
            fmt(body),
            "",
            "- 响应体：",
            "",
            fmt(resp_body),
            "",
        ]
    if not kept:
        lines += ["**没有命中任何请求**。检查 `--host` 是否写对，或 HAR 是否用「Save all as HAR with content」导出（不含 content 的 HAR 没有响应体）。", ""]
    else:
        lines += ["---", "", f"共扫描 {total} 条请求，命中并输出 {kept} 条，解码 {decoded} 条 base64 响应。", ""]

    if js_paths:
        lines += [
            "## 前端 JS 中出现的 `/userres/v1/` 接口路径",
            "",
            f"共 {len(js_paths)} 个（从解码后的响应文本里提取，**按路径去重**）：",
            "",
        ]
        lines += [f"- `{p}`" for p in sorted(js_paths)]
        lines += [
            "",
            "> **这些只是 JS 字符串，不是请求/响应证据。** 没有实际抓到请求体的接口，"
            "不能据此猜测参数去实现；请在网页端执行对应操作后再抓一次 HAR。",
            "",
        ]
    return "\n".join(lines), total, kept


def leak_check(text: str) -> list[str]:
    hits = []
    for label, pat in LEAK_PATTERNS:
        n = len(pat.findall(text))
        if n:
            hits.append(f"{label} ×{n}")
    return hits


def main() -> int:
    ap = argparse.ArgumentParser(description="HAR 接口结构脱敏提取")
    ap.add_argument("har", help="HAR 文件路径")
    ap.add_argument("-o", "--output", help="输出 markdown 路径（默认与 HAR 同目录的 har_summary.md）")
    ap.add_argument("--host", default="guangyapan.com", help="只保留 URL 含该字符串的请求；传空字符串可看全部")
    ap.add_argument("--only", default="", help="额外按 URL 关键词过滤，逗号分隔，如 copy,rename,delete")
    args = ap.parse_args()

    src = Path(args.har).expanduser()
    if not src.is_file():
        print(f"找不到文件：{src}", file=sys.stderr)
        return 1
    out = Path(args.output).expanduser() if args.output else src.with_name("har_summary.md")
    only = [x.strip() for x in args.only.split(",") if x.strip()]
    try:
        text, total, kept = extract(src, args.host, only)
    except (ValueError, UnicodeDecodeError) as exc:
        print(f"解析失败（确认是 JSON 格式的 .har）：{exc}", file=sys.stderr)
        return 1

    hits = leak_check(text)
    if hits:
        text = ("> ⚠️ **自检发现疑似未脱敏内容（" + "、".join(hits)
                + "）**，请人工复核并删除后再分享，不要直接发给任何人。\n\n") + text
    out.write_text(text, encoding="utf-8")
    print(f"完成：扫描 {total} 条，命中 {kept} 条")
    print(f"输出：{out}")
    if hits:
        print(f"⚠️ 疑似未脱敏内容：{'、'.join(hits)}，已在文件头标注，请复核后再分享")
    else:
        print("自检通过：未发现 JWT / 手机号 / Bearer 凭证残留。")
    print("提示：业务 ID 会保留，凭证类字段已脱敏；原始 HAR 不要入库。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
