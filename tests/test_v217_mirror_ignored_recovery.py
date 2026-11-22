"""v2.1.4 镜像「清空待处理清单」永久拉黑修复专项验收。

用户真机反馈：「清空待处理清单」把 10 部作品标记为 english_source='ignored'，
导致后续重新同步**永远不再翻译**这些作品。

根因链：
  clear 端点 → english_source='ignored'
  → _resolve_english_title 对 ignored 短路（force=False 时 return "", "ignored"）
  → TMDB / AI / 拼音三级兜底全部跳过
  → english_title 永远为空
  → push_gate 的「文件名已翻译为英文」门禁永远不通过 → 该作品永远进不了推送队列

本轮修法（双保险）：
  ① 后端：sync_rule 发现 A 库出现新文件时，自动解除 ignored 标记（语义收敛为
     「本轮没有新内容时跳过」，不是永久拉黑）；
  ② 后端：clear 端点补 rule_id 过滤（不再误清其它规则的作品）；
  ③ 后端：counts.works_without_english 与待处理清单口径对齐（排除 ignored），
     另加 works_english_ignored 透明计数；
  ④ 前端：二次确认明确告知后果 + 按钮不再随清单为空消失 + 已忽略数量可见。

覆盖：
  1. 后端：clear 端点按 rule_id 过滤，不误清其它规则；
  2. 后端：clear 端点返回 note 说明恢复路径；
  3. 后端：A 库出现新文件时自动解除 ignored 标记；
  4. 后端：没有新文件时保持 ignored（不重复解析，避免打爆 TMDB / AI）；
  5. 后端：ignored 短路行为不变（无新内容仍跳过）；
  6. 后端：counts 口径对齐 + works_english_ignored 计数；
  7. 前端契约：二次确认说明后果 + 按钮常驻 + 已忽略计数展示。
"""
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

from app import mirror  # noqa: E402  必须在 sys.path 注入之后导入

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"

# A/B 库目录树 ID：FakeAPI 与 MirrorRule 必须用同一套 ID。
# 两边不一致时（例如规则填 "A-root"、FakeAPI 只认 "A_ROOT"），_walk_source
# 会走成空树 → works 为空 → sync_rule 的作品主循环整轮不执行 →
# 「解除 ignored」分支永远进不去（测试红或假绿都源于此）。
A_ROOT, A_WORK = "A_ROOT", "A_W1"
B_ROOT, B_WORK = "B_ROOT", "B_W1"


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v214ign_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'ign.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _rule(db, *, root=A_ROOT, target=B_ROOT):
    """建一条镜像规则。

    默认根目录 ID 必须与 FakeAPI 目录树一致（见文件顶部 A_ROOT/B_ROOT 注释）：
    规则填的 ID 对不上 FakeAPI 认识的 ID 时，_walk_source 走成空树、
    works 为空，sync_rule 的作品主循环整轮不执行，「解除 ignored」分支
    永远进不去 —— 用例就会以「假绿」或莫名失败收场。
    """
    from app.models import MirrorRule
    r = MirrorRule(source_root_id=root, target_root_id=target, enabled=True)
    db.add(r)
    db.commit()
    return r


def _media(db, rule, *, work="work:测试剧", title="测试剧", english_source="",
           english_title=""):
    """建一条影视映射行。

    english_source 默认空串 = 「待翻译（未被忽略）」——这才是「清空待处理清单」
    要作用的目标状态。需要 ignored 语义时由调用方显式传 english_source="ignored"。
    """
    from app.models import MirrorMedia
    m = MirrorMedia(rule_id=rule.id, work_key=work, title_standard=title,
                    title_mirror=title, media_type="tv",
                    source_dir_id="s1", target_dir_id="t1",
                    english_title=english_title, english_source=english_source)
    db.add(m)
    db.commit()
    return m


# ---------------------------------------------------------------------------
# 1-2. clear 端点：rule_id 过滤 + 恢复提示
# ---------------------------------------------------------------------------

def test_clear_untranslated_scoped_to_rule(env):
    """clear 端点只清当前规则的作品，不误清其它规则。"""
    from app import main as m
    S, _ = env
    with S() as db:
        r = _rule(db, root="A1", target="B1")
        other = _rule(db, root="A2", target="B2")
        _media(db, r, work="work:本规则", title="本规则")
        _media(db, other, work="work:别的规则", title="别的规则")

        # 直接调业务函数（绕过 HTTP 依赖），验证只清当前规则
        import inspect
        src = inspect.getsource(m.mirror_clear_untranslated)
        assert "MirrorMedia.rule_id == rule.id" in src, "clear 端点必须按 rule_id 过滤"

        # 前置条件：端点通过 _mirror_rule() 取「当前规则」（库里第一条），
        # 本用例假定它就是 r。若哪天实现变了，这里会明确报错而不是让下面的
        # 断言以「看错对象」的方式误判。
        assert m._mirror_rule(db).id == r.id, "前置假设失效：当前规则不再是 r"

        m.mirror_clear_untranslated(db)
        from app.models import MirrorMedia
        mine = db.scalar(select(MirrorMedia).where(MirrorMedia.work_key == "work:本规则"))
        theirs = db.scalar(select(MirrorMedia).where(MirrorMedia.work_key == "work:别的规则"))
        assert mine.rule_id == r.id and theirs.rule_id == other.id
        assert mine.english_source == "ignored"
        assert theirs.english_source == "", "其它规则的作品不得被误清"


def test_clear_untranslated_returns_note(env):
    """clear 端点返回恢复路径说明（前端要展示）。"""
    from app import main as m
    S, _ = env
    with S() as db:
        r = _rule(db)
        _media(db, r)
        out = m.mirror_clear_untranslated(db)
        assert out["ok"] is True
        assert out["cleared"] == 1
        assert "note" in out and out["note"], "必须返回恢复路径说明"
        assert "重新解析" in out["note"] or "恢复" in out["note"]


# ---------------------------------------------------------------------------
# 3-4. sync_rule：新文件自动解除 ignored（端到端，真实调用 sync_rule）
# ---------------------------------------------------------------------------

# A 库目录树：A_ROOT → 作品目录 → 视频文件。
# 作品目录名带 {tmdb-xxx} 标记，_discover_works 才能认出是影视作品。
# A_ROOT / A_WORK 见文件顶部常量（与 FakeAPI、_rule 共用同一套 ID）。
WORK_NAME = "侠客行 (2001) {tmdb-135646}"
FNAME = "侠客行 第01集 2160p.WEB-DL.H.265.mkv"
A_FILE = {"fileId": "F1", "resType": 1, "fileName": FNAME,
          "fileSize": 100, "utime": 1, "parentId": A_WORK}


class FakeAPI:
    """A 库固定一棵目录树；B 库作品目录按 b_files 模拟实际内容。

    契约（与 guangya.GuangYaWebFiles 对齐）：
      list_files      → {"list": [...], "total": N}，page>0 必须返回空列表
      create_dir      → 必须含非空 "file_id"
      copy_file       → 返回 taskId 字符串
      get_task_status → {"status": 2, ...} 表示完成
      delete_files    → prepass C 类可能调用，实现以免 AttributeError
    """
    b_files: list = []
    copies: list = []
    created: list = []
    a_files: list = []
    deleted: list = []

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls, *, a_files=None, b_files=None):
        cls.a_files = [dict(f) for f in (a_files if a_files is not None else [A_FILE])]
        cls.b_files = [dict(f) for f in (b_files or [])]
        cls.copies, cls.created, cls.deleted = [], [], []

    async def list_files(self, parent_id, page=0, page_size=50):
        if page > 0:
            return {"list": [], "total": 0}
        if parent_id == A_ROOT:
            return {"list": [{"fileId": A_WORK, "resType": 2,
                              "fileName": WORK_NAME}], "total": 1}
        if parent_id == A_WORK:
            return {"list": [dict(f) for f in self.a_files],
                    "total": len(self.a_files)}
        if parent_id == B_ROOT:
            return {"list": [{"fileId": B_WORK, "resType": 2,
                              "fileName": WORK_NAME}], "total": 1}
        if parent_id == B_WORK:
            return {"list": [dict(f) for f in self.b_files],
                    "total": len(self.b_files)}
        return {"list": [], "total": 0}

    async def create_dir(self, parent_id, dir_name):
        self.created.append((parent_id, dir_name))
        return {"file_id": f"B_DIR_{len(self.created)}", "name": dir_name,
                "parent_id": parent_id, "raw": {}}

    async def copy_file(self, file_ids, parent_id):
        self.copies.append((list(file_ids), parent_id))
        for _fid in file_ids:
            self.b_files.append({"fileId": f"BF{len(self.b_files) + 1}", "resType": 1,
                                 "fileName": FNAME, "fileSize": 100, "utime": 1,
                                 "parentId": parent_id})
        return f"TASK{len(self.copies)}"

    async def get_task_status(self, task_id):
        return {"status": 2, "detail": {}, "raw": {"status": 2}}

    async def delete_files(self, file_ids):
        self.deleted.append(list(file_ids))
        return "DELTASK"


def _noop_sleep():
    async def _sleep(*_a, **_kw):
        return None
    return _sleep


def _sync(db, rule, **kw):
    kw.setdefault("max_polls", 1)
    kw.setdefault("poll_interval", 0)
    return asyncio.run(mirror.sync_rule(db, rule, **kw))


def _seed_ignored_media(db, rule):
    """预置一条被标记 ignored 的作品映射行。

    关键：work_key 必须等于 sync_rule 算出的值
    （"work:" + normalize_work_name(A 库作品目录名)），否则 sync_rule 查不到这一行、
    会新建一行 english_source="" 的记录，解除逻辑永远不会触发。
    """
    from app.models import MirrorMedia
    from app.utils import normalize_work_name
    m = MirrorMedia(rule_id=rule.id,
                    work_key="work:" + normalize_work_name(WORK_NAME),
                    title_standard=WORK_NAME, title_mirror=WORK_NAME,
                    media_type="tv", source_dir_id=A_WORK, target_dir_id=B_WORK,
                    category_path="武侠", english_title="", english_source="ignored",
                    status="pending")
    db.add(m)
    db.commit()
    return m


def test_sync_unignores_media_when_new_file_appears(env):
    """端到端：A 库出现新文件时，sync_rule 自动解除 ignored 标记。"""
    from app import mirror
    S, _ = env
    with S() as db:
        r = _rule(db)
        m = _seed_ignored_media(db, r)
        # A 库这一集尚未同步过 → 必然是「本轮的新文件」
        FakeAPI.reset(a_files=[dict(A_FILE)], b_files=[])

        with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
             patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
            out = _sync(db, r)

        db.expire_all()
        row = db.get(type(m), m.id)
        assert out.get("unignored") == 1, f"应统计解除 1 部，实际 {out.get('unignored')}"
        assert row.english_source == "", "新文件出现后必须解除 ignored"


def test_sync_keeps_ignored_when_no_new_file(env):
    """端到端：A 库没有新文件（指纹命中已同步记录）→ 保持 ignored，不重复解析。"""
    from app import mirror
    from app.models import MirrorFile
    S, _ = env
    with S() as db:
        r = _rule(db)
        m = _seed_ignored_media(db, r)
        # 预置一条已同步记录，指纹与 A 库文件一致 → 本轮无新文件
        db.add(MirrorFile(media_id=m.id, rule_id=r.id, identity="tmdb:135646:tv:1:1",
                          source_file_id="F1", target_file_id="BF1",
                          file_name=FNAME, size=100, utime=1,
                          fingerprint="100:1", status="synced"))
        db.commit()
        FakeAPI.reset(a_files=[dict(A_FILE)],
                      b_files=[{"fileId": "BF1", "resType": 1, "fileName": FNAME,
                                "fileSize": 100, "utime": 1, "parentId": "B_W1"}])

        with patch("app.mirror.GuangYaWebFiles", FakeAPI), \
             patch("app.mirror.asyncio.sleep", new=_noop_sleep()):
            out = _sync(db, r)

        db.expire_all()
        row = db.get(type(m), m.id)
        assert not out.get("unignored"), "没有新文件时不得解除标记"
        assert row.english_source == "ignored", "没有新文件时应保持 ignored"


def test_sync_rule_source_has_unignore_branch():
    """sync_rule 源码里必须有「新文件 → 解除 ignored」的分支。

    断言写成对源码片段的结构化检查，而不是死抠字面量：
      实际实现是 `str(getattr(media, "english_source", "") or "") == "ignored"`，
      而不是字面的 `english_source == "ignored"`，按字面量断言会误报。
    """
    src = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")
    assert "has_new_source_file" in src, "缺少新文件标记"
    # ignored 判定：允许 str(...) 包裹 / getattr 取值等不同写法
    assert '== "ignored"' in src, "缺少 ignored 判定"
    assert "english_source" in src, "缺少 english_source 引用"
    # 解除动作：写入空串并落库
    assert 'english_source=""' in src, "缺少解除 ignored 的赋值"
    assert "解除忽略标记" in src, "缺少解除逻辑"


def test_resolve_english_title_still_skips_ignored(env):
    """ignored 短路行为不变：本轮没有新内容时仍跳过解析（不清单反复解析）。"""
    import asyncio
    from app import mirror as mr
    S, _ = env
    with S() as db:
        r = _rule(db)
        m = _media(db, r, english_source="ignored")
        eng, source = asyncio.run(mr._resolve_english_title(db, m, force=False))
        assert eng == ""
        assert source == "ignored"


# ---------------------------------------------------------------------------
# 6. counts 口径
# ---------------------------------------------------------------------------

def test_counts_align_with_pending_list(env):
    """counts.works_without_english 与待处理清单口径一致（都排除 ignored）。"""
    from app import main as m
    S, _ = env
    with S() as db:
        r = _rule(db)
        _media(db, r, work="work:待翻译", title="待翻译", english_source="")
        _media(db, r, work="work:已忽略", title="已忽略", english_source="ignored")
        db.commit()

        res = m.mirror_status(db=db)
        counts = res["counts"]
        assert counts["works_without_english"] == 1, "只统计未忽略的待翻译作品"
        assert counts["works_english_ignored"] == 1, "已忽略作品单独计数"
        # 待处理清单同样排除 ignored
        works = [x["work"] for x in res["untranslated_works"]]
        assert "待翻译" in works
        assert "已忽略" not in works


# ---------------------------------------------------------------------------
# 7. 前端契约
# ---------------------------------------------------------------------------

def test_frontend_clear_confirm_explains_consequence():
    """二次确认必须明确说明后果与恢复路径。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "会发生什么" in app, "二次确认缺少「会发生什么」"
    assert "不会发生什么" in app, "二次确认缺少「不会发生什么」"
    assert "怎么恢复" in app, "二次确认缺少「怎么恢复」"
    assert "清空待处理清单" in app


def test_frontend_clear_button_never_vanishes():
    """按钮不再随清单为空自动消失（否则清空后无法手动恢复）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert 'v-if="mirrorStatus.untranslated_works?.length" plain @click="clearUntranslated"' \
        not in app, "「清空待处理清单」按钮仍会在清单为空时消失"
    assert '!mirrorStatus.untranslated_works?.length' in app, "按钮应改为禁用而非隐藏"


def test_frontend_shows_ignored_count():
    """已忽略作品数量透明展示。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "works_english_ignored" in app, "未消费已忽略计数"
    assert "已忽略解析" in app, "缺少已忽略统计卡"
