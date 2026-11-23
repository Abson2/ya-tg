"""v2.1.11 需求1：B 库被用户手动清空后的「重整」端到端验收。

用户口径（原文）：
  「当资源库扫描时发现：B 库或个别子目录被清空时，应该如何清理现有日志、
    流程、关联任务、映射等，重新梳理下流程，重点是镜像同步、资源库、
    分享中心等推送、分享任务等
    （重新镜像同步任务执行不联动，仍然等镜像同步任务开始，
      场景是用户手动清理了存量文件，准备重新整理）」

本文件锁死的行为：

  A. 四层状态一次性归位
     ① 镜像映射层：文件映射重置为待同步但**保留 A 库锚点**；影视映射归零；
        目录映射置「待重建」并打 [pruned] 标记。
     ② 镜像任务层：未终结的复制任务终态化。
     ③ 资源库层：资源行与推送队列行**归档**（不物理删除）且不出现在面板。
     ④ 分享中心层：未终结的分享任务取消，当前分享记录作废。
     ⑤ 日志层：镜像规则状态/进度归零，页面回到「尚未同步」。

  B. 红线
     - 清理类操作**只归档不物理删除**（历史行与执行记录必须保留）。
     - **绝不触发镜像同步**：不调用 sync_rule、不启动任何镜像任务。
       重建等下一次定时镜像同步任务自然开始。
     - 归档行不参与任何业务查询，但按唯一键重建时**复用归档行**，
       绝不撞 UNIQUE 约束。

  C. 范围
     - scope=library 整个 B 库清空。
     - scope=dir 只重整指定子目录，其它作品不动。

  D. 预览
     - dry_run=true 只出清单，不写库。
"""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from app.db import SessionLocal, migrate
from app.main import app
from app.models import (Channel, MirrorDir, MirrorFile, MirrorMedia, MirrorRule,
                        MirrorTask, QueueItem, Resource, ShareRecord, ShareTask,
                        include_archived)

migrate()
_client = TestClient(app)
HEADERS = {"X-Admin-Password": "testpass"}
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def clean():
    """清干净镜像/资源/队列/分享相关表，保证每个用例独立。"""
    with SessionLocal() as db:
        for model in (MirrorTask, MirrorFile, MirrorMedia, MirrorDir, MirrorRule,
                      ShareTask, ShareRecord, QueueItem, Resource, Channel):
            db.query(model).delete()
        db.commit()
    yield


def _seed(scope="library"):
    """造一份「B 库还在、但马上要被用户手动清空」的现场。

    返回各层主键，便于断言。
    """
    with SessionLocal() as db:
        rule = MirrorRule(name="默认镜像", source_root_id="A", target_root_id="B",
                           last_status="success", last_message="上一轮跑完了",
                           last_pipeline_json='{"stage":"done","running":false,"copied":9}',
                           batch_status="idle", batch_id="b1")
        db.add(rule)
        db.commit()
        rid = rule.id

        ch = Channel(name="c", chat_id="1", bot_id=1, enabled=True)
        db.add(ch)
        db.commit()
        cid = ch.id

        media = MirrorMedia(rule_id=rid, work_key="work:test", title_standard="测试剧",
                            title_mirror="测试剧", source_dir_id="src1",
                            target_dir_id="tgt1", status="synced")
        db.add(media)
        db.commit()
        mid = media.id

        mf = MirrorFile(media_id=mid, rule_id=rid, identity="tmdb:1:tv:1:1",
                        source_file_id="srcf1", target_file_id="BFILE1",
                        file_name="S01E01.mkv", fingerprint="100:200", status="synced",
                        target_name="Show.S01E01.mkv", rename_status="done")
        db.add(mf)
        db.commit()
        fid = mf.id

        d = MirrorDir(rule_id=rid, source_dir_id="src1", parent_target_id="",
                      name_standard="测试剧", kind="work", category_path="剧集",
                      target_dir_id="tgt1", status="created")
        db.add(d)
        db.commit()
        did = d.id

        t = MirrorTask(rule_id=rid, media_id=mid, task_id="tk1", status="running",
                       task_status="submitted", batch_id="b1", sequence_id=1)
        db.add(t)
        db.commit()
        tid = t.id

        res = Resource(file_id="BFILE1", name="S01E01.mkv", res_type=1,
                       path_hint="测试剧/Season 1", share_dir_name="测试剧",
                       identify_status="identified", media_title="测试剧",
                       share_status="active", share_id="sh1", share_url="http://x")
        db.add(res)
        db.commit()
        resid = res.id

        q = QueueItem(resource_id=resid, channel_id=cid, status="pending")
        db.add(q)
        db.commit()
        qid = q.id

        st = ShareTask(work_key="work:test", kind="first", resource_id=resid,
                       dedup_key="work:test", status="planned")
        db.add(st)
        db.commit()
        stid = st.id

        sr = ShareRecord(work_key="work:test", resource_id=resid, share_id="sh1",
                         share_url="http://x", share_status="active", current_flag=1)
        db.add(sr)
        db.commit()
        srid = sr.id

    return {"rule": rid, "media": mid, "file": fid, "dir": did, "task": tid,
            "resource": resid, "queue": qid, "share_task": stid,
            "share_record": srid, "channel": cid}


def _restructure(**body):
    payload = {"scope": "library", "dry_run": False}
    payload.update(body)
    return _client.post("/api/mirror/restructure", json=payload, headers=HEADERS)


# ---------------------------------------------------------------------------
# A. 四层状态一次性归位
# ---------------------------------------------------------------------------

def test_restructure_resets_all_four_layers(clean):
    ids = _seed()
    r = _restructure()
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["ok"] is True and d["dry_run"] is False

    with SessionLocal() as db:
        # ① 镜像映射层
        mf = db.get(MirrorFile, ids["file"])
        assert mf.status == "pending", "文件映射必须重置为待同步"
        assert mf.target_file_id == "" and mf.target_name == ""
        assert mf.rename_status == ""
        # A 库锚点必须保留——清了下轮同步就再也复制不回来
        assert mf.source_file_id == "srcf1", "A 库锚点必须保留"
        assert mf.file_name == "S01E01.mkv"
        assert mf.fingerprint == "100:200"

        media = db.get(MirrorMedia, ids["media"])
        assert media.status == "pending", "影视映射状态必须归零"

        dd = db.get(MirrorDir, ids["dir"])
        assert dd.status == "missing", "目录映射必须置为待重建"
        assert dd.target_dir_id == ""
        assert "[pruned]" in (dd.error or ""), "目录必须打上 [pruned] 标记"

        # ② 镜像任务层
        t = db.get(MirrorTask, ids["task"])
        assert t.task_status == "cancelled", "未终结的复制任务必须终态化"

        # ③ 资源库层：归档而非删除
        # 注意：归档行被全局过滤后 db.get() 也返回 None（业务视图里它已消失），
        # 要确认「行还在表里」必须走归档旁路查询。
        with include_archived():
            res = db.get(Resource, ids["resource"])
            assert res is not None, "资源行必须保留（只归档不物理删除）"
            assert res.clean_state == "cleaned"
            assert res.cleaned_at is not None
            assert res.share_status == "not_created"
            q = db.get(QueueItem, ids["queue"])
            assert q is not None, "队列行必须保留"
            assert q.clean_state == "cleaned"
            assert q.status == "cancelled", "归档队列行必须终态化，避免被 worker 领走"
        # 归档行不再出现在业务查询里
        visible = db.scalars(select(Resource).where(
            Resource.id == ids["resource"])).all()
        assert visible == [], "归档行必须从业务查询中隐藏"

        # ④ 分享中心层
        st = db.get(ShareTask, ids["share_task"])
        assert st.status == "cancelled", "未终结的分享任务必须取消"
        sr = db.get(ShareRecord, ids["share_record"])
        assert int(sr.current_flag or 0) == 0, "当前分享记录必须作废"
        assert sr is not None, "分享记录历史行必须保留"

        # ⑤ 日志层
        rule = db.get(MirrorRule, ids["rule"])
        assert rule.last_status == "never"
        assert rule.last_message == ""
        assert rule.last_pipeline_json == ""
        assert rule.batch_id == ""


def test_restructure_does_not_delete_rows(clean):
    """红线：只归档不物理删除——历史行与执行记录必须保留。"""
    ids = _seed()
    _restructure()
    with SessionLocal() as db:
        for model, key in ((Resource, "resource"), (QueueItem, "queue"),
                           (ShareTask, "share_task"), (ShareRecord, "share_record"),
                           (MirrorFile, "file"), (MirrorMedia, "media"),
                           (MirrorDir, "dir"), (MirrorTask, "task")):
            with include_archived():
                row = db.get(model, ids[key])
            assert row is not None, f"{model.__name__} 行被物理删除了，违反归档红线"


# ---------------------------------------------------------------------------
# B. 红线：绝不触发镜像同步
# ---------------------------------------------------------------------------

def test_restructure_never_triggers_sync(clean, monkeypatch):
    """清理动作绝不调用 sync_rule、绝不启动镜像任务。"""
    _seed()
    from app import mirror as mirror_mod

    called = []

    def _boom(*a, **kw):
        called.append("sync_rule")
        raise AssertionError("重整端点绝不能触发镜像同步")

    monkeypatch.setattr(mirror_mod, "sync_rule", _boom)
    # main.py 里若引用了 mirror.sync_rule，走模块属性同样被拦截
    monkeypatch.setattr("app.main.mirror.sync_rule", _boom, raising=False)

    r = _restructure()
    assert r.status_code == 200, r.text
    assert called == [], "重整端点调用了镜像同步"


def test_restructure_refuses_while_batch_running(clean):
    """批次正在跑时拒绝重整——否则会毁掉正在进行的同步。"""
    ids = _seed()
    with SessionLocal() as db:
        rule = db.get(MirrorRule, ids["rule"])
        rule.batch_status = "running"
        db.commit()
    r = _restructure()
    assert r.status_code == 409, r.text
    assert "批次" in r.json().get("detail", "")


# ---------------------------------------------------------------------------
# C. 范围：scope=dir 只动指定子目录
# ---------------------------------------------------------------------------

def test_restructure_scope_dir_only_touches_that_dir(clean):
    """只重整指定子目录，别的作品一动不动。"""
    ids = _seed()
    # 造第二个作品（另一个子目录）
    with SessionLocal() as db:
        rule = db.get(MirrorRule, ids["rule"])
        m2 = MirrorMedia(rule_id=rule.id, work_key="work:other", title_standard="别的剧",
                         source_dir_id="src2", target_dir_id="tgt2", status="synced")
        db.add(m2)
        db.commit()
        mid2 = m2.id
        f2 = MirrorFile(media_id=mid2, rule_id=rule.id, identity="tmdb:2:tv:1:1",
                        source_file_id="srcf2", target_file_id="BFILE2",
                        file_name="Other.S01E01.mkv", status="synced")
        db.add(f2)
        db.commit()
        fid2 = f2.id
        r2 = Resource(file_id="BFILE2", name="Other.S01E01.mkv", res_type=1,
                      path_hint="别的剧/Season 1", share_dir_name="别的剧",
                      identify_status="identified")
        db.add(r2)
        db.commit()
        rid2 = r2.id

    r = _restructure(scope="dir", source_dir_ids=["src1"])
    assert r.status_code == 200, r.text
    with SessionLocal() as db:
        # 指定子目录：已重整
        assert db.get(MirrorFile, ids["file"]).status == "pending"
        with include_archived():
            assert db.get(Resource, ids["resource"]).clean_state == "cleaned"
        # 另一个子目录：完全不动
        assert db.get(MirrorFile, fid2).status == "synced", "别的子目录不该被动"
        assert db.get(MirrorMedia, mid2).status == "synced"
        with include_archived():
            assert db.get(Resource, rid2).clean_state == "", "别的子目录不该被归档"


def test_restructure_scope_dir_requires_ids(clean):
    _seed()
    r = _restructure(scope="dir", source_dir_ids=[])
    assert r.status_code == 400
    assert "目录" in r.json().get("detail", "")


def test_restructure_scope_invalid(clean):
    _seed()
    r = _restructure(scope="whatever")
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# D. 预览
# ---------------------------------------------------------------------------

def test_restructure_dry_run_changes_nothing(clean):
    ids = _seed()
    r = _restructure(dry_run=True)
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["dry_run"] is True
    assert "preview" in d
    with SessionLocal() as db:
        assert db.get(MirrorFile, ids["file"]).status == "synced", "预览不得改数据"
        assert db.get(Resource, ids["resource"]).clean_state == "", "预览不得归档"
        assert db.get(MirrorRule, ids["rule"]).last_status == "success"


# ---------------------------------------------------------------------------
# E. 归档行的唯一键闭环（重建不撞 UNIQUE 约束）
# ---------------------------------------------------------------------------

def test_rebuild_after_archive_reuses_row_no_unique_violation(clean):
    """B 库重建时 file_id 不变（回收站恢复场景）也必须能重建，不撞唯一约束。

    这条曾经是真事故：归档行仍占着 resources.file_id 的唯一键，
    重建时直接 INSERT 新行 → `UNIQUE constraint failed` → 整批扫描崩。
    """
    ids = _seed()
    _restructure()
    with SessionLocal() as db:
        with include_archived():
            assert db.get(Resource, ids["resource"]).clean_state == "cleaned"

    # 模拟重建：同一 file_id 再次入库
    from app.scanner import _upsert_resource
    with SessionLocal() as db:
        row, created, _ch = _upsert_resource(
            db, {"fileId": "BFILE1", "fileName": "S01E01.mkv", "ext": ".mkv",
                 "fileSize": 100, "utime": 200}, "p1")
        db.commit()
        assert created is False, "命中归档行应复用，不该新建"
        assert row.id == ids["resource"], "必须复用同一行（历史行保留）"
        assert row.clean_state == "", "复用后归档标记必须清掉"
        assert row.revision >= 1, "内容变化应记入版本号"


def test_archive_filter_hides_rows_from_business_queries(clean):
    """归档行不出现在资源库面板（全局过滤生效）。"""
    ids = _seed()
    _restructure()
    with SessionLocal() as db:
        assert db.scalars(select(Resource)).all() == [], "归档行必须被全局过滤隐藏"
        assert db.scalars(select(QueueItem)).all() == [], "归档队列行必须被全局过滤隐藏"
        # 但归档行仍在表里（历史可查）
        from app.models import include_archived
        with include_archived():
            assert len(db.scalars(select(Resource)).all()) == 1
            assert len(db.scalars(select(QueueItem)).all()) == 1
