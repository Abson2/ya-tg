"""存量分享重建（v2.1.58）：把「我的分享」里按季/按集建的老链接统一收成作品级。

背景（真机 2026-11-28，v2.1.57 存量清理之后仍未解决的三个残留）
  ① 274 部作品的老分享标题已改成作品名，但**内容仍是单集**（改标题改不了被封的那一集）；
  ② `work:书剑恩仇录` / `work:开封府` / `work:我不是大师` 保留的那条分享
     **已失效**（`get_share_access_token` → `code=201`）——清理按「保留最早」选中了死链，
     真正可用的链接被当成重复删掉了，三部现在没有可用分享；
  ③ 一部分老分享 `get_share_page_files_list` 返回空 data（代码里的
     `SHARE_CONTENT_UNAVAILABLE`），外部点开看不到文件。

判定口径（全部复用 v2.1.57 已落地的实现，不另写一套）
  · 目标目录与标题：`share_center_v2._share_target()`（排除季目录 + 作品级标题）
  · 「这条分享已经是作品级」的**唯一**判据：分享根目录恰好一个条目，
    且 `file_id == 目标目录 id`、`is_dir` 为真；
  · 一部作品只要有一条已经是作品级 → 保留它（随手改名成作品名），其余删除；
    一条都没有 → 按作品目录**新建**一条，库内引用全部改指新链接，旧链接删除。

用法（在容器内跑，需要 v2.1.58 镜像里的 `_share_target`）
    python scripts/rebuild_work_shares.py                  # 只出计划（默认，不改任何东西）
    python scripts/rebuild_work_shares.py --apply           # 真执行
    python scripts/rebuild_work_shares.py --apply --limit 5 # 先拿 5 部试水

安全
  · 默认 dry-run；`--apply` 才调网盘接口；
  · 每次接口调用之间 sleep（默认 1.2s），避免触发风控；
  · 单部作品失败只记进 errors 继续下一部，绝不中断整轮；
  · 删除只在「新链接已经建好并已回写库」之后才做，不会出现两部作品都没链的中间态。
"""
import argparse
import asyncio
import json
import sys
from collections import defaultdict
from types import SimpleNamespace

from app import share_center_v2 as sc2
from app import utils
from app.config import settings
from app.db import SessionLocal
from app.guangya import GuangYaShareClient, GuangYaWebFiles, create_share_parsed
from app.models import Resource, SCPublishTask

# 作品级展示元数据需要哪些列（v2.1.56 教训：绝不整表实体化 ORM 行）
META_COLS = ("id", "file_id", "name", "media_title", "media_type", "tmdb_id",
             "poster_path", "share_dir_id", "share_dir_name", "parent_id",
             "path_hint", "season", "episode")


def share_id_of(url: str) -> str:
    """https://www.guangyapan.com/s/<id>_<token> → <id>（update/delete 只吃数字段）。"""
    tail = str(url or "").rstrip("/").rsplit("/", 1)[-1]
    return tail.split("_", 1)[0].strip()


def share_tail_of(url: str) -> str:
    """完整尾部 <id>_<token>——`get_share_access_token` 要求完整，给数字段会报 112。"""
    return str(url or "").rstrip("/").rsplit("/", 1)[-1]


def load_members(db) -> dict:
    """work_key → 实时资源行（12 列投影 + SimpleNamespace，不实例化 ORM 实体）。

    用 ORM 的列 select（不是 Core 的 table.select）——只有 ORM 语句才会命中
    `db.py` 里那条「排除归档行」的 `do_orm_execute` 钩子，归档作品不参与重建。
    """
    from sqlalchemy import select
    stmt = select(*[getattr(Resource, c) for c in META_COLS])
    grouped: dict[str, list] = defaultdict(list)
    for r in db.execute(stmt).all():
        m = SimpleNamespace(**dict(zip(META_COLS, r)))
        grouped[utils.work_group_key(m)].append(m)
    return grouped


class Prober:
    """把一条分享归类：dead / empty / work_level / other。"""

    def __init__(self, wf, target_dir: str):
        self.wf = wf
        self.target_dir = str(target_dir or "")

    async def __call__(self, url: str) -> str:
        try:
            tok = await self.wf.share_access_token(share_tail_of(url))
        except Exception:
            return "dead"
        try:
            files = await self.wf.share_root_files(tok, max_items=5)
        except Exception:
            return "empty"
        if len(files) == 1 and files[0].get("is_dir") and \
                str(files[0].get("file_id") or "") == self.target_dir:
            return "work_level"
        return "other"


def build_plan(db, members_by_key: dict) -> list[dict]:
    """只读：算出每部作品「留哪条 / 删哪几条 / 要不要新建」。不改任何东西。"""
    tasks_by_key: dict[str, list] = defaultdict(list)
    for t in db.query(SCPublishTask).filter(SCPublishTask.share_url != "").all():
        tasks_by_key[str(t.work_key or "")].append(t)

    plan = []
    for wk, tasks in tasks_by_key.items():
        tasks.sort(key=lambda t: (str(t.created_at or ""), int(t.id or 0)))
        urls, seen = [], set()
        for t in tasks:
            u = str(t.share_url or "").strip()
            if u and u not in seen:
                seen.add(u)
                urls.append(u)
        if not urls:
            continue
        members = members_by_key.get(wk) or []
        if not members:
            plan.append({"work_key": wk, "urls": urls, "action": "skip",
                         "reason": "没有实时资源行（作品已不在库里）", "dir_id": "", "title": ""})
            continue
        meta = utils.work_display_meta(members)
        dir_id, title = sc2._share_target(db, wk, meta, members[0])
        if not dir_id:
            plan.append({"work_key": wk, "urls": urls, "action": "skip",
                         "reason": "定不出作品目录", "dir_id": "", "title": title})
            continue
        plan.append({"work_key": wk, "urls": urls, "action": "probe",
                     "dir_id": str(dir_id), "title": title, "keep": "", "drop": []})
    return plan


def decide(plan: list[dict], verdicts: dict) -> list[dict]:
    """把探测结果折成动作：留一条作品级 / 新建一条。"""
    for item in plan:
        if item["action"] != "probe":
            continue
        vs = [(u, verdicts.get(u, "other")) for u in item["urls"]]
        keep = next((u for u, v in vs if v == "work_level"), "")
        item["keep"] = keep
        item["drop"] = [u for u, _ in vs if u != keep] if keep else [u for u, _ in vs]
        item["rename"] = bool(keep)
        item["verdicts"] = [v for _, v in vs]
        item["action"] = "keep" if keep else "create"
    return plan


async def apply_plan(db, plan: list[dict], *, sleep: float = 1.2) -> dict:
    client = GuangYaShareClient(db)
    stats = {"kept": 0, "created": 0, "renamed": 0, "deleted": 0,
             "rows_repointed": 0, "skipped": 0, "errors": []}
    for item in plan:
        if item["action"] == "skip":
            stats["skipped"] += 1
            print(f"  · 跳过 {item['work_key']}：{item['reason']}")
            continue
        wk, title, dir_id = item["work_key"], item["title"], item["dir_id"]
        new_url, new_id = "", ""
        if item["action"] == "keep":
            new_url = item["keep"]
            new_id = share_id_of(new_url)
            stats["kept"] += 1
            try:
                await client.update(new_id, title, settings.share_duration)
                stats["renamed"] += 1
            except Exception as exc:
                stats["errors"].append(f"rename {wk}: {str(exc)[:120]}")
            await asyncio.sleep(sleep)
        else:
            try:
                new_id, new_url = await create_share_parsed(client, dir_id, title,
                                                            settings.share_duration)
            except Exception as exc:
                stats["errors"].append(f"create {wk}: {str(exc)[:120]}")
                await asyncio.sleep(sleep)
                continue
            if not new_url:
                stats["errors"].append(f"create {wk}: 接口未返回链接")
                await asyncio.sleep(sleep)
                continue
            stats["created"] += 1
            print(f"  + {wk} → 新建 {new_id}（{title}）")
            await asyncio.sleep(sleep)

        # ① 库内引用改指新链接（先回写，后删旧链，绝不出现「两头都空」）
        for t in db.query(SCPublishTask).filter(SCPublishTask.work_key == wk,
                                                SCPublishTask.share_url != "").all():
            if str(t.share_url) != new_url:
                t.share_url = new_url
                stats["rows_repointed"] += 1
        try:
            for r in db.query(Resource).filter(Resource.share_url.in_(item["drop"])).all():
                r.share_url, r.share_id = new_url, new_id
                r.share_status = "active"
                r.share_duration = settings.share_duration
                stats["rows_repointed"] += 1
            db.commit()
        except Exception as exc:
            db.rollback()
            stats["errors"].append(f"db {wk}: {str(exc)[:120]}")
            continue

        # ② 删旧链（含已失效的，删失败只记账不中断）
        for url in item["drop"]:
            try:
                await client.delete(share_id_of(url))
                stats["deleted"] += 1
            except Exception as exc:
                stats["errors"].append(f"delete {share_id_of(url)}: {str(exc)[:100]}")
            await asyncio.sleep(sleep)
    return stats


async def run(args) -> int:
    with SessionLocal() as db:
        members_by_key = load_members(db)
        plan = build_plan(db, members_by_key)
        todo = plan[:args.limit] if args.limit else plan
        print(f"有分享记录的作品：{len(plan)}｜本次处理：{len(todo)}")

        wf = GuangYaWebFiles(db)
        verdicts: dict[str, str] = {}
        if args.skip_probe:
            # 只想知道「哪些作品、什么目录、什么名字」时用，完全不碰网盘接口
            for item in todo:
                if item["action"] == "probe":
                    item["action"] = "probe-skipped"
                    item["drop"] = list(item["urls"])
        else:
            for item in todo:
                if item["action"] != "probe":
                    continue
                prober = Prober(wf, item["dir_id"])
                for u in item["urls"]:
                    verdicts[u] = await prober(u)
                    await asyncio.sleep(0.6)
            decide(todo, verdicts)

        keep = [i for i in todo if i["action"] == "keep"]
        create = [i for i in todo if i["action"] == "create"]
        undecided = [i for i in todo if i["action"] == "probe-skipped"]
        drop = sum(len(i.get("drop") or []) for i in todo)
        stat: dict[str, int] = defaultdict(int)
        for i in todo:
            for v in i.get("verdicts") or []:
                stat[v] += 1
        if undecided:
            print(f"作品数：{len(todo)}｜未探测（--skip-probe，只列目录与标题）{len(undecided)}")
        else:
            print(f"判定：作品级可留 {len(keep)}｜需要新建 {len(create)}｜待删旧链 {drop}")
            print(f"　　　分享体检：作品级 {stat['work_level']}｜单集/多项 {stat['other']}｜"
                  f"内容不可读 {stat['empty']}｜已失效 {stat['dead']}")
        for i in todo[:8]:
            if i["action"] == "skip":
                print(f"  · 跳过 {i['work_key']}：{i['reason']}")
            elif i["action"] == "probe-skipped":
                print(f"  {i['work_key']} → 作品名「{i['title']}」，目录 {i['dir_id']}，"
                      f"现有 {len(i['urls'])} 条链接")
            else:
                print(f"  {i['work_key']} → {'保留 ' + i['keep'][-18:] if i.get('keep') else '新建'}"
                      f"，删 {len(i.get('drop') or [])} 条，名字「{i['title']}」，目录 {i['dir_id']}")
        if args.skip_probe and args.apply:
            print("\n--skip-probe 只用于看计划，不能和 --apply 一起用")
            return 2
        if not args.apply:
            print("\n（dry-run，未改任何东西；加 --apply 才执行）")
            return 0
        stats = await apply_plan(db, todo)
        print("\n结果：" + json.dumps(stats, ensure_ascii=False)[:900])
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="真正执行（默认只看计划）")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 部作品")
    ap.add_argument("--skip-probe", action="store_true",
                    help="不探测网盘，只列「作品→目录→名字」（快，只看计划用）")
    return asyncio.run(run(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
