"""修已推送的 Telegram 卡片里的分享链接（v2.1.60 候选）。

背景（真机 2026-11-28）
  存量分享重建把每部作品的分享都换成了作品级新链接，库里 `share_url` 已改指新链，
  但**群里/订阅里已经发出去的那几百条卡片**，按钮里还是旧链接，点开 `code=201 已失效`。

口径（真机探针确认，不是猜）
  · 默认 `tg_link_style=button`：正文末尾那行纯文本链接会被 `share_link_presentation`
    **剥掉**，链接实际落在 `📥 获取资源` 按钮上 —— 所以绝大多数消息只需改**按钮**，
    正文一个字都不用动（改正文才是最大的风险源）。
  · 卡片可能有图（`sendPhoto` → 改 `caption`）也可能没图（改 `text`），
    但改按钮用 `editMessageReplyMarkup` 与消息类型无关，**统一首选**。
  · 少部分历史卡片是 plain 样式，链接在正文里 → 单独归 `body` 类，不在这一版自动改
    （改正文要重排实体，容易被 html 转义啃坏；先只报数，等确认再做）。

怎么认「哪条消息旧了」
  正文与按钮里的分享链接都解析出 share id，与该作品**当前**的 share id 比对；
  不一致的**不直接判旧**——同一部作品可能同时存在两条合法分享（重建/重试各建了一条），
  id 不同但点开是同一个作品目录。所以不一致时先探测：根目录那一项是不是该作品的目录？
  是 → `ok_other`（能用，不动）；打不开 → `stale`（要改）；探测失败 → `probe_error`（不改，报出来）。
  这一层是必须的：早一版只比 id，把 3 条**本来就好**的卡片判成要改。

用法（容器内跑，改的是线上群消息）
    python /tmp/fpl.py                       # 只读：读回消息、出清单，不改任何东西
    python /tmp/fpl.py --limit 5             # 只看 5 条
    python /tmp/fpl.py --chat -1003708133864 # 只看某个群
    python /tmp/fpl.py --apply               # 真的改按钮（默认 1.2s 一条，防 429）

安全
  · 默认 dry-run，`--apply` 才调写接口；
  · 只改分享链接这一个字段，按钮文案、`👍 想看` 回调、正文、媒体一律原样保留；
  · 单条失败只记账继续下一条；429 按 `retry_after` 退避；
  · 绝不伪造「已修」：接口返回什么就记什么，失败原因原样进 errors。
"""
import argparse
import asyncio
import json
import re
import sys
from collections import Counter, defaultdict
from types import SimpleNamespace

import httpx
from sqlalchemy import select

from app import runtime_config, share_center_v2 as sc2, tg_stats, utils
from app.db import SessionLocal
from app.guangya import GuangYaWebFiles
from app.models import Bot, Channel, QueueItem, Resource, SCPublishTask
from app.security import decrypt_secret

# 分享链接：https://www.guangyapan.com/s/<id>_<token>
SHARE_RE = re.compile(r"https?://(?:www\.)?guangyapan\.com/s/(\d+)_([A-Za-z0-9]+)")

META_COLS = ("id", "file_id", "name", "media_title", "media_type", "tmdb_id",
             "poster_path", "share_dir_id", "share_dir_name", "parent_id",
             "path_hint", "season", "episode")

# 改不动的原因，按 Telegram 原话归类（不翻译成自造名词）
_GONE_HINTS = ("message to edit not found", "message not found", "MESSAGE_ID_INVALID")
_OLD_HINTS = ("message can't be edited", "message cannot be edited", "too old")
_SAME_HINTS = ("message is not modified",)
_NO_TEXT_HINTS = ("there is no text in the message to edit",)


def share_id_in(url: str) -> str:
    """链接里的分享 id；不是分享链接返回空串。"""
    m = SHARE_RE.search(str(url or ""))
    return m.group(1) if m else ""


def share_ids_in_text(text: str) -> list[str]:
    """正文里出现的分享 id（可能多个，按出现顺序去重）。"""
    out, seen = [], set()
    for m in SHARE_RE.finditer(str(text or "")):
        sid = m.group(1)
        if sid not in seen:
            seen.add(sid)
            out.append(sid)
    return out


def markup_to_dict(markup) -> list[list[dict]]:
    """Telethon 的 reply_markup → Bot API 的 inline_keyboard 结构（原样搬运）。"""
    rows = []
    for row in getattr(markup, "rows", None) or []:
        cells = []
        for b in getattr(row, "buttons", None) or []:
            cell = {"text": str(getattr(b, "text", "") or "")}
            url = getattr(b, "url", None)
            data = getattr(b, "data", None)
            if url:
                cell["url"] = str(url)
            elif data:
                cell["callback_data"] = data.decode() if isinstance(data, bytes) else str(data)
            cells.append(cell)
        if cells:
            rows.append(cells)
    return rows


def retarget_markup(rows: list[list[dict]], current_url: str) -> tuple[list[list[dict]], int]:
    """把按钮里的分享链接换成当前链接，其它按钮原样不动。返回 (新键盘, 改了几个)。"""
    changed = 0
    out = []
    for row in rows:
        cells = []
        for cell in row:
            c = dict(cell)
            if share_id_in(c.get("url", "")) and str(c.get("url")) != current_url:
                c["url"] = current_url
                changed += 1
            cells.append(c)
        out.append(cells)
    return out, changed


def classify(body_ids: list[str], button_ids: list[str], current_id: str) -> str:
    """给一条消息定性：ok / stale_button / stale_body / no_link。

    `stale_body` 优先于 `stale_button` —— 正文里的旧链接比按钮更容易被误点
    （按钮至少还长得像按钮，正文是裸链接），必须单独报出来。
    """
    if not body_ids and not button_ids:
        return "no_link"
    if body_ids and any(s != current_id for s in body_ids):
        return "stale_body"
    if button_ids and any(s != current_id for s in button_ids):
        return "stale_button"
    return "ok"


def collect_rows(db) -> list[dict]:
    """把「已发送 + 有 message_id」的队列行整理成待检查清单。

    刻意用 Core 的 table.select()：`do_orm_execute` 钩子会给 ORM 查询追加
    「排除归档行」，而**已经发到群里的消息不会因为库行归档就消失**。
    """
    qt = QueueItem.__table__
    rows = db.execute(select(qt.c.id, qt.c.resource_id, qt.c.channel_id,
                             qt.c.telegram_message_id, qt.c.sent_at, qt.c.status)
                      .where(qt.c.telegram_message_id != "")).all()

    chats = {r.id: SimpleNamespace(id=r.id, chat_id=str(r.chat_id or ""),
                                   bot_id=int(r.bot_id or 0), name=str(r.name or ""))
             for r in db.execute(select(Channel.__table__.c.id, Channel.__table__.c.chat_id,
                                        Channel.__table__.c.bot_id,
                                        Channel.__table__.c.name)).all()}
    bots = {r.id: SimpleNamespace(id=r.id, name=str(r.name or ""), token=str(r.token or ""))
            for r in db.execute(select(Bot.__table__.c.id, Bot.__table__.c.name,
                                       Bot.__table__.c.token)).all()}

    rt = Resource.__table__
    wk_of, members = {}, defaultdict(list)
    for r in db.execute(select(*[rt.c[c] for c in META_COLS])).all():
        m = SimpleNamespace(**dict(zip(META_COLS, r)))
        wk = utils.work_group_key(m)
        wk_of[m.id] = wk
        members[wk].append(m)

    cur = {}
    tasks = db.execute(select(SCPublishTask.__table__.c.work_key,
                              SCPublishTask.__table__.c.share_url,
                              SCPublishTask.__table__.c.created_at)
                       .where(SCPublishTask.__table__.c.share_url != "")).all()
    for t in sorted(tasks, key=lambda x: (str(x.created_at or ""), x.share_url)):
        cur[str(t.work_key or "")] = str(t.share_url)

    out = []
    for r in rows:
        ch = chats.get(r.channel_id)
        wk = wk_of.get(r.resource_id)
        if ch is None or not ch.chat_id or not wk:
            continue
        url = cur.get(wk, "")
        members_of_work = members.get(wk) or []
        meta = utils.work_display_meta(members_of_work)
        try:
            dir_id, _title = sc2._share_target(db, wk, meta, members_of_work[0])
        except Exception:
            dir_id = ""
        out.append({"queue_id": int(r.id), "chat_id": ch.chat_id, "channel": ch.name,
                    "bot_id": ch.bot_id, "message_id": str(r.telegram_message_id),
                    "sent_at": r.sent_at, "work_key": wk, "current_url": url,
                    "current_id": share_id_in(url), "dir_id": str(dir_id or ""),
                    "title": meta.get("title", "")})
    return out


async def read_messages(client, rows: list[dict]) -> dict:
    """按 (chat_id, message_id) 把消息读回来。返回 {(chat, mid): Message|None}。"""
    by_chat = defaultdict(list)
    for r in rows:
        by_chat[r["chat_id"]].append(r["message_id"])
    got = {}
    for chat_id, mids in by_chat.items():
        uniq = list(dict.fromkeys(mids))
        for i in range(0, len(uniq), 100):     # 一次拉太多容易超时，切片
            chunk = uniq[i:i + 100]
            try:
                msgs = await client.get_messages(int(chat_id), ids=[int(m) for m in chunk])
            except Exception as exc:
                print(f"  · {chat_id} 读消息失败：{type(exc).__name__} {str(exc)[:120]}")
                msgs = []
            for mid, m in zip(chunk, msgs):
                got[(chat_id, str(mid))] = m
            await asyncio.sleep(0.4)
    return got


def inspect(rows: list[dict], msgs: dict) -> list[dict]:
    """只读判定：把每条消息归类，不动任何东西。"""
    out = []
    for r in rows:
        m = msgs.get((r["chat_id"], r["message_id"]))
        if not r["current_id"]:
            r2 = dict(r, verdict="no_share", need=0)
            out.append(r2)
            continue
        if m is None:
            out.append(dict(r, verdict="gone", need=0))
            continue
        body_ids = share_ids_in_text(getattr(m, "message", "") or "")
        kb = markup_to_dict(getattr(m, "reply_markup", None))
        button_ids = [share_id_in(c.get("url", "")) for row in kb for c in row
                      if share_id_in(c.get("url", ""))]
        verdict = classify(body_ids, button_ids, r["current_id"])
        _, need = retarget_markup(kb, r["current_url"])
        out.append(dict(r, verdict=verdict, need=need, has_media=bool(
            getattr(m, "media", None)), edited=bool(getattr(m, "edit_date", None))))
    return out


async def refine(db, checked: list[dict], msgs: dict) -> list[dict]:
    """id 不一致的再核一层：**能打开且指向本作品目录就不改**。

    真机 2026-11-28：`雪花女神龙`/`陆小凤与花满楼`/`琅琊榜之风起长林` 三条卡片的按钮
    与库里那条是**两条不同的合法分享**（建分享重试各建了一条），点开都是同一个作品目录。
    只比 id 会把这 3 条判成要改，白改还白丢一次编辑标记。所以不一致时探一次：
      · 根目录那一项 == 本作品目录 → `ok_other`（能用，不动）
      · 打不开（201/200/空）        → 保持 `stale_button`（该改）
      · 探测本身失败（网络/token）  → `probe_error`（没结论就**不改**，只报出来）
    只在有分歧时才调网盘接口，一致时一次都不调。
    """
    wf = GuangYaWebFiles(db)
    cache: dict[str, str] = {}
    for r in checked:
        if r["verdict"] != "stale_button":
            continue
        m = msgs.get((r["chat_id"], r["message_id"]))
        if m is None:
            continue
        kb = markup_to_dict(getattr(m, "reply_markup", None))
        urls = [c.get("url") for row in kb for c in row if share_id_in(c.get("url", ""))]
        usable = broken = 0
        for u in urls:
            sid = share_id_in(u)
            if sid == r["current_id"]:
                continue
            got = cache.get(sid)
            if got is None:
                try:
                    tok = await wf.share_access_token(u.rstrip("/").rsplit("/", 1)[-1])
                    files = await wf.share_root_files(tok, max_items=5)
                    got = str(files[0].get("file_id")) if files else "empty"
                except Exception:
                    got = "dead"
                cache[sid] = got
                await asyncio.sleep(0.5)
            if got == r["dir_id"] and r["dir_id"]:
                usable += 1
            elif got in ("dead", "empty"):
                broken += 1
        if broken:
            continue                      # 明确打不开 → 留着改
        r["verdict"] = "ok_other" if usable else "probe_error"
    return checked


async def _post(base: str, token: str, method: str, payload: dict) -> tuple[bool, str]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(f"{base}/bot{token}/{method}", json=payload)
    except Exception as exc:
        return False, f"连接失败：{type(exc).__name__} {str(exc)[:120]}"
    try:
        data = resp.json()
    except Exception:
        data = {"ok": False, "description": resp.text[:200]}
    if resp.status_code == 429:
        wait = (data.get("parameters") or {}).get("retry_after") or 3
        try:
            await asyncio.sleep(float(wait) + 0.5)
        except Exception:
            pass
        return False, f"429 限流（retry_after={wait}）"
    if resp.status_code < 400 and data.get("ok"):
        return True, ""
    return False, str(data.get("description") or data)[:200]


def bucket(err: str) -> str:
    """把 Telegram 的原话归成一类，方便统计时分辨「改不了」和「真出错」。"""
    low = err.lower()
    if any(k.lower() in low for k in _SAME_HINTS):
        return "unchanged"
    if any(k.lower() in low for k in _OLD_HINTS):
        return "too_old"
    if any(k.lower() in low for k in _GONE_HINTS):
        return "gone"
    if any(k.lower() in low for k in _NO_TEXT_HINTS):
        return "wrong_method"
    return "error"


async def apply_fix(db, rows: list[dict], msgs: dict, *, sleep: float = 1.2) -> dict:
    """只改按钮链接。正文里有旧链接（stale_body）一律不动，只在结果里报数。"""
    base = runtime_config.telegram_base(db)
    token_of = {}
    for r in rows:
        if r["bot_id"] not in token_of:
            bot = db.get(Bot, r["bot_id"])
            token_of[r["bot_id"]] = decrypt_secret(bot.token) if bot else ""
    stats = Counter()
    errors: list[str] = []
    for r in rows:
        if r["verdict"] not in ("stale_button",):
            stats[r["verdict"]] += 1
            continue
        m = msgs.get((r["chat_id"], r["message_id"]))
        if m is None:
            stats["gone"] += 1
            continue
        kb, need = retarget_markup(markup_to_dict(getattr(m, "reply_markup", None)),
                                   r["current_url"])
        if not need or not kb:
            stats["unchanged"] += 1
            continue
        ok, err = await _post(base, token_of.get(r["bot_id"], ""), "editMessageReplyMarkup",
                              {"chat_id": r["chat_id"], "message_id": int(r["message_id"]),
                               "reply_markup": {"inline_keyboard": kb}})
        if ok:
            stats["fixed"] += 1
            print(f"  ✓ {r['chat_id']}/{r['message_id']} {r['title'][:24]} → 按钮改指新链接")
        else:
            stats[bucket(err)] += 1
            errors.append(f"{r['chat_id']}/{r['message_id']} {r['title'][:20]}: {err}")
            print(f"  ✗ {r['chat_id']}/{r['message_id']} {err}")
        await asyncio.sleep(sleep)
    return {"counts": dict(stats), "errors": errors}


async def run(args) -> int:
    with SessionLocal() as db:
        rows = collect_rows(db)
        if args.chat:
            rows = [r for r in rows if r["chat_id"] == args.chat]
        if args.limit:
            rows = rows[:args.limit]
        print(f"待检查的已推送消息：{len(rows)}（{len({r['chat_id'] for r in rows})} 个聊天）")

        client = tg_stats._get_client(db)
        await tg_stats._ensure_authorized(client, db)
        me = await client.get_me()
        print(f"读取身份：{getattr(me, 'username', None) or getattr(me, 'id', None)}"
              f"（bot={getattr(me, 'bot', None)}）")

        msgs = await read_messages(client, rows)
        checked = inspect(rows, msgs)
        checked = await refine(db, checked, msgs)

        tally = Counter(r["verdict"] for r in checked)
        print(f"判定：需要改按钮 {tally['stale_button']}｜正文里是旧链接 {tally['stale_body']}｜"
              f"已经是新链接 {tally['ok']}｜另一条同样有效的分享 {tally['ok_other']}｜"
              f"探不出结论 {tally['probe_error']}｜没有链接 {tally['no_link']}｜"
              f"消息读不到 {tally['gone']}｜作品无可用分享 {tally['no_share']}")
        for r in checked:
            if r["verdict"] in ("stale_body", "no_link", "no_share", "probe_error"):
                print(f"  · [{r['verdict']}] {r['chat_id']}/{r['message_id']} "
                      f"{r['title'][:24]}（待定）")
        sample = [r for r in checked if r["verdict"] == "stale_button"][:5]
        for r in sample:
            print(f"  · [stale_button] {r['chat_id']}/{r['message_id']} {r['title'][:24]} "
                  f"改 {r['need']} 个按钮")

        if not args.apply:
            print("\n（dry-run，未改任何消息；加 --apply 才执行）")
            return 0

        stats = await apply_fix(db, checked, msgs, sleep=args.sleep)
        print("\n结果：" + json.dumps(stats, ensure_ascii=False)[:900])
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="真的改消息（默认只看清单）")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 条消息")
    ap.add_argument("--chat", type=str, default="", help="只处理这个 chat_id")
    ap.add_argument("--sleep", type=float, default=1.2, help="每条之间的间隔秒数")
    return asyncio.run(run(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
