"""v1.5.1 P0 验收：资源库手动推送推不出去（保留按钮，修可消费中间态）。

真机 2026-11-21：资源库点「推送」排队成功但消息发不出。根因：端点入队后设
share_task_id=force_task.id 却没设 status_ext，项停在空 ext；publisher 交接闸门
只放行「有 share_task_id 且 accepted」或「无 share_task_id」，空 ext 的项永不发送。

v2.1.21 起这条修法换了形态，**但同一条约束仍然成立**：
手动推送不再登记旧链 share_tasks（那套已废弃，写了三个卡片也读不到），
队列项改为 `share_task_id = 0` —— 走 publisher 的「非分享中心项」通道，
天然可消费，不会落回空 ext 的死角。任务记录改由 v2 的 force 任务承接。

覆盖：
  - 队列项必须处于「publisher 能直接消费」的形态（share_task_id=0）；
  - 必须登记 v2 force 任务（否则推送任务/历史/队列三个卡片看不见）；
  - 必须真的驱动一次队列，否则「点推送」不会立即发；
  - 手动推送按钮保留（端点仍存在且返回 queued）。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_publish_endpoint_keeps_queue_item_consumable():
    src = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
    i = src.index("async def publish_resource(")
    j = src.index("async def ", i + 10)
    seg = src[i:j]
    # v2.1.21：不再挂旧链（旧链 id 会让 publisher 要求 accepted 才发 → 永不发送）。
    assert "q.share_task_id = 0" in seg, "队列项必须可被 publisher 直接消费"
    assert "shadow_register(db" not in seg, "手动推送不得再写旧表 share_tasks"
    # 登记 v2 force 任务：三个卡片的可见性全靠这一步
    assert "register_force_tasks" in seg
    # 必须真的驱动一次队列，否则点了不立即发
    assert "await process_queue(" in seg


def test_manual_publish_button_preserved():
    app = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")
    assert "publishResource" in app, "手动推送按钮处理函数必须保留"
    # v2.1.61：写法从 `seriesRepresentative(g) && publishResource(...)` 改为直接调用
    # ——那是「查不到代表资源就短路」的缺陷（678 部里绝大多数点推送没反应）。
    # 本用例守护的不变量只有一条：作品卡片的推送按钮与处理函数都还在。
    assert '@click="publishResource(seriesRepresentative(g))"' in app
