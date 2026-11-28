"""B 库空目录壳清理（v2.1.66）：把「已归档 + B 库实时确认为空」的作品目录壳从网盘删掉。

背景（真机 2026-11-28，用户选「1+2+3」= 仅空目录 + 手动 dry-run 先行 + 脚本发版）
  · 诉讼女王等无效作品：资源已归档（clean_state=cleaned / clean_reason='B库内容已移除，归档资源库记录'）、
    B 库文件也被全镜像 C 类删净，但**作品目录 + 季目录的空壳留在网盘**（用户看到「文件夹还在」）。
  · 根因：C 类删除只调 api.delete_files(b_ids) 删文件；网盘 delete_file 虽支持传目录 id
    （HAR 实证 w.har #37/39：create_dir 出的 resType=2 目录被 delete_file 成功删除），
    但项目里没有任何「识别空壳目录 → 删它」的上层动作，且这些作品 mirror_media=0、
    C 类遍历 select(MirrorMedia) 根本扫不到它们。

安全底线（红线，改脚本前先读）
  1. 候选**只能**来自 resources 里 clean_state='cleaned' 且 clean_reason 精确匹配的归档行，
     按 share_dir_id 去重。绝不扫描「还在用」的目录，绝不凭文件名/标题猜。
  2. 「空」的判据 = 递归列举整棵子树（最多 MAX_DEPTH 层）后**文件数恰好为 0**。
     任一子目录列举抛错 / 超深 / 有 resType 不是 1/2 的未知项 → 整部 skip，绝不删。
  3. 实测反证（2026-11-28）：完美世界已归档但 B 库递归还有 200 个文件、
     不为人知的故事归档后又被重新同步进 8 个文件——两者都因为「有文件」被自动跳过。
     「已归档」≠「可删」，必须每次实时列举。
  4. 删除顺序后序：先删最深的子目录、再删父目录；每删一个目录前**重新列举确认它仍为空**，
     防止脚本运行期间有新文件写入导致误删。
  5. 默认 dry-run，只打印计划；`--apply` 才调 delete_file。单目录失败只记录继续，不中断整轮。
  6. 每步之间 sleep（默认 1.2s）防风控；`--limit` 限制处理数量；`--work` 按标题过滤。

用法（容器内跑，需 v2.1.66 镜像）
    python scripts/purge_empty_archived_dirs.py                      # 只看计划（默认，零写）
    python scripts/purge_empty_archived_dirs.py --apply --limit 2    # 先删 2 部试水
    python scripts/purge_empty_archived_dirs.py --apply              # 全量执行
"""
import argparse
import asyncio
import json
import sys
from collections import OrderedDict

from app.db import SessionLocal
from app.guangya import GuangYaWebFiles

# 归档行判定：精确匹配清理原因，绝不扩大到别的 clean_reason
ARCHIVED_REASON = "B库内容已移除，归档资源库记录"
MAX_DEPTH = 4           # 作品目录 → 季目录 → （最多再两层），超深一律 skip
LIST_PAGE_SIZE = 200


def load_candidates(db, *, work_filter: str = "") -> list[dict]:
    """从已归档资源里取 distinct share_dir_id 作为候选（只读，不实例化 ORM 实体）。

    按 dir_id 去重；同一 dir 可能对应多条归档资源。work_filter 用 share_dir_name
    模糊匹配（标题是「诉·讼·女·王」这种带间隔符的目录名，用 media_title 匹配不到）。
    """
    from sqlalchemy import text
    rows = db.execute(text("""
        SELECT share_dir_id, share_dir_name, path_hint, MIN(media_title) AS media_title,
               COUNT(*) AS archived_rows
        FROM resources
        WHERE clean_state = 'cleaned' AND clean_reason = :reason
          AND share_dir_id IS NOT NULL AND TRIM(share_dir_id) <> ''
        GROUP BY share_dir_id, share_dir_name, path_hint
        ORDER BY share_dir_id
    """), {"reason": ARCHIVED_REASON}).all()
    out = []
    for r in rows:
        d = dict(r._mapping)
        name = str(d.get("share_dir_name") or "")
        if work_filter and work_filter not in name and work_filter not in str(d.get("media_title") or ""):
            continue
        out.append({
            "dir_id": str(d["share_dir_id"]),
            "dir_name": name,
            "path_hint": str(d.get("path_hint") or ""),
            "media_title": str(d.get("media_title") or ""),
            "archived_rows": int(d.get("archived_rows") or 0),
        })
    return out


async def list_children(wf: GuangYaWebFiles, parent_id: str) -> list[dict]:
    """列一层，返回原始条目 list；失败抛 WebFileError 由上层处理（绝不吞成「空」）。"""
    data = await wf.list_files(parent_id=parent_id, page=0, page_size=LIST_PAGE_SIZE)
    return list(data.get("list") or [])


def _is_dir(item: dict) -> bool:
    return str(item.get("resType")) == "2"


def _is_file(item: dict) -> bool:
    return str(item.get("resType")) == "1"


async def probe_tree(wf: GuangYaWebFiles, root_id: str) -> dict:
    """递归判定整棵子树是否为空壳。

    返回 {"empty": bool, "dirs": [id,...]（后序，子在前父在后）,
          "files": n, "unknown": n, "error": str|""}。
    任何列举异常 / 深度超限 / 未知 resType → empty=False 且带 error/unknown，绝不删。
    """
    dirs_post: list[str] = []
    state = {"files": 0, "unknown": 0, "error": "", "too_deep": False}

    async def walk(pid: str, depth: int) -> bool:
        if depth > MAX_DEPTH:
            state["too_deep"] = True
            state["error"] = f"超过 MAX_DEPTH={MAX_DEPTH}，跳过不删"
            return False
        try:
            items = await list_children(wf, pid)
        except Exception as exc:
            state["error"] = f"列举 {pid} 失败：{str(exc)[:80]}"
            return False
        ok = True
        for it in items:
            if _is_file(it):
                state["files"] += 1
            elif _is_dir(it):
                child = str(it.get("fileId") or "")
                if not child:
                    state["unknown"] += 1
                    continue
                if not await walk(child, depth + 1):
                    ok = False
            else:
                state["unknown"] += 1
        dirs_post.append(pid)
        return ok

    ok = await walk(root_id, 0)
    empty = bool(ok and not state["error"] and not state["too_deep"]
                 and state["files"] == 0 and state["unknown"] == 0)
    return {
        "empty": empty,
        "dirs": dirs_post,
        "files": state["files"],
        "unknown": state["unknown"],
        "too_deep": state["too_deep"],
        "error": state["error"],
    }


async def confirm_dir_empty(wf: GuangYaWebFiles, dir_id: str) -> bool:
    """删前二次确认：这一层仍无子项（文件/目录都没有）。有内容立即返回 False。"""
    try:
        items = await list_children(wf, dir_id)
    except Exception:
        return False
    return not items


async def wait_task(wf: GuangYaWebFiles, task_id: str, *, tries: int = 12, gap: float = 1.0) -> bool:
    """轮询 delete_file 的 taskId：status=2 且 detail.code 非 0 视为失败；status=3 失败。"""
    for _ in range(tries):
        try:
            st = await wf.get_task_status(task_id)
        except Exception:
            await asyncio.sleep(gap)
            continue
        status = st.get("status")
        detail = st.get("detail") or {}
        code = int(detail.get("code") or 0)
        if str(status) == "3":
            return False
        if str(status) == "2":
            return code == 0
        await asyncio.sleep(gap)
    return False


async def run(args) -> int:
    db = SessionLocal()
    wf = GuangYaWebFiles(db)
    try:
        candidates = load_candidates(db, work_filter=args.work)
        if args.limit > 0:
            candidates = candidates[: args.limit]
        print(f"候选归档目录：{len(candidates)} 个（来源：resources 已归档 + clean_reason 精确匹配）")

        plan = []
        for c in candidates:
            p = await probe_tree(wf, c["dir_id"])
            item = dict(c)
            item.update({
                "empty": p["empty"], "files": p["files"], "unknown": p["unknown"],
                "error": p["error"], "dirs": p["dirs"],
            })
            plan.append(item)
            flag = "空壳→可删" if p["empty"] else "跳过"
            print(f"  [{flag}] {c['media_title'][:20]:20} dir={c['dir_id']} "
                  f"递归文件={p['files']} 未知项={p['unknown']} "
                  f"目录数={len(p['dirs'])} {p['error']}")
            await asyncio.sleep(args.sleep)

        deletable = [p for p in plan if p["empty"]]
        skipped = [p for p in plan if not p["empty"]]
        print(f"\n汇总：可删空壳 {len(deletable)}｜跳过 {len(skipped)}")
        if not args.apply:
            print("（dry-run，未删任何目录；加 --apply 才执行）")
            return 0

        deleted_dirs = 0
        failed = []
        for p in deletable:
            # 后序：子目录先删、父目录后删；每删一个前重新确认仍为空
            for dir_id in p["dirs"]:
                await asyncio.sleep(args.sleep)
                if not await confirm_dir_empty(wf, dir_id):
                    failed.append(f"{p['media_title']} 目录 {dir_id} 删前有内容/列举失败，已跳过")
                    break
                try:
                    task_id = await wf.delete_files([dir_id])
                except Exception as exc:
                    failed.append(f"{p['media_title']} 目录 {dir_id} delete_file 失败：{str(exc)[:80]}")
                    break
                ok = await wait_task(wf, task_id)
                if not ok:
                    failed.append(f"{p['media_title']} 目录 {dir_id} 删除任务 {task_id} 未确认成功")
                    break
                deleted_dirs += 1
                print(f"  已删 {p['media_title'][:20]} dir={dir_id}")
        print(f"\n执行结果：删除目录 {deleted_dirs} 个｜失败 {len(failed)} 项")
        for f in failed[:20]:
            print("  !!", f)
        print("注意：本脚本只删网盘空目录壳，不改库内归档行（归档历史保留，符合红线）。")
        return 1 if failed else 0
    finally:
        db.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="B 库空目录壳清理（已归档 + 实时确认为空才删）")
    ap.add_argument("--apply", action="store_true", help="真正删除（默认 dry-run 只看计划）")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 个候选目录")
    ap.add_argument("--work", type=str, default="", help="只看/处理标题或目录名含此关键词的")
    ap.add_argument("--sleep", type=float, default=1.2, help="每次网盘调用间隔秒数（防风控）")
    return asyncio.run(run(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
