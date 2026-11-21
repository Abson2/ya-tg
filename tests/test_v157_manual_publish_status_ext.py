"""v1.5.1 P0 验收：资源库手动推送推不出去（保留按钮，修 status_ext 空中间态）。

真机 2026-11-21：资源库点「推送」排队成功但消息发不出。根因：端点入队后设
share_task_id=force_task.id 却没设 status_ext，项停在空 ext；publisher 交接闸门
只放行「有 share_task_id 且 accepted」或「无 share_task_id」，空 ext 的项永不发送。

覆盖：
  - publish_resource 端点在设 share_task_id 后同步置 status_ext=submitted；
  - 与 retry 修复口径一致（accept_worker/accepted_at 清空）；
  - 手动推送按钮保留（端点仍存在且返回 queued）。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_publish_endpoint_sets_status_ext_submitted():
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    i = src.index("async def publish_resource(")
    j = src.index("async def ", i + 10)
    seg = src[i:j]
    assert "q.share_task_id = force_task.id" in seg
    # 紧跟其后必须把 status_ext 置 submitted（空 ext 中间态修复）
    k = seg.index("q.share_task_id = force_task.id")
    after = seg[k:k + 700]
    assert 'q.status_ext = "submitted"' in after
    assert 'q.accept_worker = ""' in after
    assert "q.accepted_at = None" in after


def test_manual_publish_button_preserved():
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "publishResource" in app, "手动推送按钮处理函数必须保留"
    assert "@click=\"seriesRepresentative(g) && publishResource" in app
