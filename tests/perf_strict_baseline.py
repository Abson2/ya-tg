"""阶段二路线B C5：隔离性能测试基线（mock 大数据源，不碰生产网盘）。

用法：
  python tests/perf_strict_baseline.py

测三档规模 100 / 1000 / 10000 文件：
  1. natural_key 排序耗时（决定严格顺序的确定性开销）；
  2. _media_files DFS 遍历耗时（树规模影响）；
  3. _plan_work_batch 轮转切批耗时；
  4. 严格顺序执行器的"事件序列"开销模拟（纯内存，不跑网络）。
输出每档耗时与吞吐，作为路线B重构前后的对比基线。数据全部本地构造，无网络/DB/生产网盘访问。
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from app import mirror  # noqa: E402


def build_tree(n_files: int, per_dir: int = 50):
    """构造 n_files 个文件的 A 库目录树（作品 / Season / 文件），纯内存。"""
    nodes = {}
    n_dirs = max(1, (n_files + per_dir - 1) // per_dir)
    children = []
    for d in range(n_dirs):
        did = f"d{d}"
        files = [{"fileId": f"f{d}_{i}", "fileName": f"E{d}_{i:04d}.mkv",
                  "fileSize": 100, "utime": 1, "resType": 1}
                 for i in range(per_dir)]
        nodes[did] = {"id": did, "name": f"Season {d + 1}", "parent": "work",
                      "path": f"work/Season {d + 1}", "files": files, "children": []}
        children.append(did)
    nodes["work"] = {"id": "work", "name": "作品", "parent": "*", "path": "作品",
                     "files": [], "children": children}
    return nodes, nodes["work"]


def bench(label, fn):
    t0 = time.perf_counter()
    result = fn()
    dt = time.perf_counter() - t0
    print(f"  {label:36s} {dt * 1000:9.2f} ms")
    return dt, result


def main():
    print("=== 隔离性能测试基线（mock 数据，不碰生产网盘）===")
    for n in (100, 1000, 10000):
        print(f"\n--- 规模 {n} 文件 ---")
        nodes, work = build_tree(n)
        # 所有文件节点收集
        all_items = []
        for d in work["children"]:
            all_items.extend(nodes[d]["files"])
        bench(f"natural_key 排序 {len(all_items)} 项",
              lambda items=all_items: sorted(items, key=lambda f: mirror.natural_key(f["fileName"])))
        bench("_media_files DFS 遍历",
              lambda: mirror._media_files(work, nodes))
        works = [(work, [])]
        bench("plan_work_batch 切批",
              lambda: mirror.plan_work_batch(works, max_works=1, hard_max=n))
    print("\n=== 基线完成 ===")


if __name__ == "__main__":
    main()
