"""v1.5.2 P0 验收：手动推送「(f.skipped || []) is not iterable」契约覆盖 bug。

真机 2026-11-21：手动推送「道士下山」报 TypeError。根链条：
  - main.publish_resource 初始化 skipped=[]（字符串数组，前端要展开它）；
  - publisher.process_queue 锁冲突分支返回 {"skipped": True}（bool）；
  - 端点 `return {..., "skipped": skipped, **result}` 的 **result 在后覆盖数组；
  - 前端 `[...(d.skipped || [])]` 展开 bool → TypeError。

三层修复：publisher 换键名 queue_busy / main 契约顺序+pop 内部标志+reason 进
detail / 前端 Array.isArray 归一化。本文件逐层验收。

覆盖：
  1. 真实占锁调用 process_queue：返回 queue_busy、绝不含 skipped 键；
  2. main.py publish_resource 返回段：result 先展开、自身 skipped 后写、pop、
     reason 归并 detail；
  3. 前端 skipped/detail 归一化守卫。
"""
import asyncio
import os
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]


# ---------- 1. 行为：锁冲突返回不再污染 skipped ----------

def test_process_queue_busy_returns_queue_busy_without_skipped():
    from app import publisher

    async def run():
        # 先占住锁，模拟「另一次队列处理进行中」
        await publisher._queue_lock.acquire()
        try:
            return await publisher.process_queue(limit=5)
        finally:
            publisher._queue_lock.release()

    res = asyncio.run(run())
    assert res.get("queue_busy") is True, res
    assert res.get("sent") == 0 and res.get("failed") == 0
    assert res.get("reason"), "锁冲突必须带可读 reason"
    assert "skipped" not in res, "锁冲突绝不能返回 bool skipped——会覆盖端点数组契约"


# ---------- 2. main.py 返回契约：静态守卫 ----------

def test_publish_resource_response_contract():
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    i = src.index("async def publish_resource(")
    j = src.index("async def ", i + 10)
    seg = src[i:j]

    assert "skipped = []" in seg, "skipped 仍是给前端的数组契约"
    assert "result = await process_queue(limit=50)" in seg

    # 关键：先展开 result 到 payload，再 pop 内部 skipped，最后自身字段后写。
    assert "payload = dict(result)" in seg
    assert 'payload.pop("skipped", None)' in seg, "必须摘掉内部 skipped 标志，防止覆盖数组"
    assert "return {**payload, \"ok\": True" in seg
    # 旧的危险写法必须消失
    assert '"skipped": skipped, **result' not in seg

    # reason 并进 detail，使锁冲突在前端显示成普通提示
    assert 'payload.pop("reason", "")' in seg
    assert '"status": "skipped", "error": reason' in seg


def test_publish_resource_skipped_stays_list_even_with_busy_result():
    """模拟返回段逻辑：即便 result 带 bool skipped/reason，最终 skipped 仍是数组。"""
    result = {"sent": 0, "failed": 0, "shares_created": 0,
              "queue_busy": True, "reason": "另一次队列处理进行中，本轮跳过",
              "skipped": True}  # 万一将来有人又往 result 塞 skipped
    skipped = ["规则A 的目标频道已停用"]
    detail = []

    payload = dict(result)
    reason = str(payload.pop("reason", "") or "")
    payload.pop("skipped", None)
    if reason:
        detail = [*detail, {"channel_id": 0, "status": "skipped", "error": reason}]
    out = {**payload, "ok": True, "queued": 1, "detail": detail, "skipped": skipped}

    assert isinstance(out["skipped"], list), "skipped 必须恒为数组，前端才能展开"
    assert out["skipped"] == skipped
    assert any("另一次队列处理进行中" in str(x.get("error", "")) for x in out["detail"])
    assert out.get("queue_busy") is True
    # 前端展开不再抛异常
    reasons = [*out["skipped"], *[x["error"] for x in out["detail"]]]
    assert len(reasons) == 2


# ---------- 3. 前端兜底：归一化守卫 ----------

def test_frontend_skipped_normalization():
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    i = app.index("async function publishResource(")
    j = app.index("async function ", i + 10)
    seg = app[i:j]
    assert "Array.isArray(d.skipped)" in seg, "skipped 必须做数组归一化"
    assert "Array.isArray(d.detail)" in seg, "detail 也必须防御"
    assert ".filter(Boolean)" in seg, "空 error 必须过滤，避免显示空白提示"
    assert "...(d.skipped || [])" not in seg, "旧的直接展开写法必须消失"
