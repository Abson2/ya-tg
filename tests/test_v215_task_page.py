"""v2.1.4 推送任务页专项验收（用户口径）：

  （1）筛选中有 failed 等用户看不懂的标签 → 状态筛选必须全中文；
  （2）PC 端右边留空较多、内容拥挤在左侧 → 页面满宽；
  （3）增加推送渠道展示频道、群，上方可筛选频道 / 群 / 全部；
  （4）分享内容最前面和详情内页需要展示 TMDB 海报缩略图。

覆盖：
  1. 任务列表返回推送渠道（频道名 / 群名），区分 channel 与 group；
  2. 任务详情同样返回渠道与海报；
  3. 海报按作品聚合键解析（版本表 → 实时资源表回查）；
  4. 前端：状态筛选全中文、不再直出内部枚举名；
  5. 前端：渠道列 + 频道/群筛选；
  6. 前端：表格首列与详情内页都有海报缩略图；
  7. 前端：页面满宽。
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v214task_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'task.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _seed(db, *, chat_id="-100123", target_type="group", channel_name="影视频道",
          poster="/abc.jpg", work_key="work:测试剧"):
    """造一条任务：清单项（渠道）+ 任务 + 资源库版本（海报）。"""
    from app.models import (Bot, Channel, Resource, SCLibraryVersion,
                            SCPublishTask, SCPushList, SCPushListItem)
    bot = Bot(name="b", token="t")
    db.add(bot)
    db.flush()
    db.add(Channel(bot_id=bot.id, name=channel_name, chat_id=chat_id, enabled=True,
                   kind="channel" if target_type == "channel" else "group"))
    pl = SCPushList(list_key="pl:1", name="清单", status="active")
    db.add(pl)
    db.flush()
    item = SCPushListItem(list_id=pl.id, work_key=work_key, library_version_id=0,
                          category_key="genre:剧情", target_type=target_type,
                          bot_id=bot.id, chat_id=chat_id, dedup_key="d:1")
    db.add(item)
    db.flush()
    task = SCPublishTask(list_item_id=item.id, work_key=work_key, kind="first",
                         status="planned", link_state="unknown")
    db.add(task)
    db.add(SCLibraryVersion(work_key=work_key, version_no=1,
                            metadata_json='{"title": "测试剧", "poster_path": "%s"}' % poster))
    db.commit()
    return task


# ---------------------------------------------------------------------------
# 1. 推送渠道
# ---------------------------------------------------------------------------

def test_task_list_returns_channel_target(env):
    """任务列表返回推送渠道（群）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        task = _seed(db, chat_id="-100999", target_type="group", channel_name="影迷群")
        res = sc2.api_tasks(db=db)
        item = next(i for i in res["items"] if i["id"] == task.id)
        assert item["group_names"] == ["影迷群"]
        assert item["channel_names"] == []
        assert "群" in item["target_summary"]
        assert "影迷群" in item["target_summary"]


def test_task_list_returns_channel_only(env):
    """频道目标只出现在频道列，不混进群。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        task = _seed(db, chat_id="@mychannel", target_type="channel", channel_name="影视频道")
        res = sc2.api_tasks(db=db)
        item = next(i for i in res["items"] if i["id"] == task.id)
        assert item["channel_names"] == ["影视频道"]
        assert item["group_names"] == []
        assert "频道" in item["target_summary"]


def test_task_detail_returns_channel_and_poster(env):
    """详情端点同样返回渠道与海报。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        task = _seed(db, chat_id="-100777", target_type="group", channel_name="追剧群",
                     poster="/poster123.jpg")
        res = sc2.api_task_detail(task.id, db)
        item = res["item"]
        assert item["group_names"] == ["追剧群"]
        assert item["cover"] == "/poster123.jpg"
        assert item["title"] == "测试剧"


def test_task_list_returns_poster_path(env):
    """任务列表返回海报相对路径（前端拼 TMDB 图片地址）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        task = _seed(db, poster="/cover.jpg")
        res = sc2.api_tasks(db=db)
        item = next(i for i in res["items"] if i["id"] == task.id)
        assert item["cover"] == "/cover.jpg"


def test_task_list_poster_empty_when_missing(env):
    """没有海报时返回空串（前端不渲染缩略图，不占位）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        task = _seed(db, poster="")
        res = sc2.api_tasks(db=db)
        item = next(i for i in res["items"] if i["id"] == task.id)
        assert item["cover"] == ""


def test_task_list_uses_neutral_ref(env):
    """任务返回中性引用名（页面不出现后端字段名）。"""
    from app import share_center_v2 as sc2
    S, _ = env
    with S() as db:
        task = _seed(db)
        res = sc2.api_tasks(db=db)
        item = next(i for i in res["items"] if i["id"] == task.id)
        assert item["ref"] == "work:测试剧"


# ---------------------------------------------------------------------------
# 2. 前端契约
# ---------------------------------------------------------------------------

def test_frontend_status_filter_is_chinese():
    """状态筛选下拉必须全中文，不得直出内部枚举名。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "scTaskStatusOptions" in app, "缺少中文状态选项"
    assert "scTaskStatusLabel(st)" in app, "状态选项未走中文映射"
    # 旧写法：直接拼 st + ' (' + n + ')'
    assert "'(' + n + ')'" not in app, "状态筛选仍在直出内部枚举名"
    # 中文标签齐全
    for label in ("待发布", "待推送", "建链中", "推送中", "已成功", "失败", "已取消"):
        assert label in app, f"缺少中文状态标签：{label}"


def test_frontend_has_channel_column_and_filter():
    """新增推送渠道列 + 频道/群筛选。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "推送渠道" in app, "缺少推送渠道列"
    assert "scTaskChannelFilter" in app, "缺少渠道筛选"
    assert 'value="channel"' in app and 'value="group"' in app, "缺少频道/群筛选项"
    assert "channel_names" in app and "group_names" in app, "未消费渠道字段"
    assert "scTaskViewRows" in app


def test_frontend_task_table_has_poster():
    """任务表格首列展示 TMDB 海报缩略图。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "sc-media-thumb" in app, "缺少缩略图样式"
    assert "image.tmdb.org/t/p/w185" in app, "缩略图未使用 TMDB 图片地址"
    assert "s.row.cover" in app, "未消费封面字段"
    # 表格首列是作品列（含缩略图），不再是裸的「任务编号」列
    assert 'label="作品"' in app, "表格首列应为作品列"


def test_frontend_task_detail_has_poster():
    """任务详情内页展示海报缩略图。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "sc-detail-poster" in app, "详情抽屉缺少海报"
    assert "image.tmdb.org/t/p/w342" in app, "详情海报未使用 TMDB 图片地址"
    assert "scDetailTask.cover" in app, "详情未消费封面字段"


def test_frontend_task_page_full_width():
    """推送任务页满宽（与发布规则页一致）。

    v2.1.5 真机回归：旧的 .sc-board-full 满宽修饰类定义在 .sc-board 之前
    （同特异度 → 后者胜出），满宽规则完全失效 → 三页在真机呈「缩小版」。
    v2.1.5 修法：三页移出两列 Grid，由 .sc-subpage 独占整行满宽。
    """
    app = APP_VUE.read_text(encoding="utf-8")
    assert "sc-subpage" in app, "缺少满宽子页容器"
    assert ".sc-subpage{display:block;width:100%;min-width:0}" in app, \
        "满宽子页容器样式缺失"
    assert ".sc-board-full{" not in app, \
        "旧的 .sc-board-full 满宽修饰类仍在（层叠顺序会再次失效）"
    assert 'class="sc-board sc-board-full"' not in app, \
        "三页仍挂在 .sc-board 两列 Grid 里"
    assert ".sc-share-layout,.sc-task-layout{display:block}" in app, "两页应单栏满宽"


def test_frontend_task_detail_no_backend_field_leak():
    """推送任务页模板不得直出**内部枚举 / 内部键**。

    ① 注释里的字段名不算泄漏（不渲染到页面），先剥掉 HTML 注释；
    ② ``scOverview.link_state_counts`` 这类是**前端响应式对象的属性访问**
       （后端 JSON 驼峰键 → 前端 ref 同名字段），属既有合法模式，不算泄漏；
    ③ 描述性字段（cover / channel_names / group_names / target_summary / ref）
       是给页面用的中性展示字段，不属于「内部枚举码」，允许出现。
    """
    import re
    app = APP_VUE.read_text(encoding="utf-8")
    start = app.index("<!-- 设计稿推送任务：搜索 + 统计条 + 任务表 + 右侧详情面板 -->")
    end = app.index("<!-- 任务详情抽屉：真实端点 /sc/tasks/{id} -->")
    block = app[start:end]
    stripped = re.sub(r"<!--.*?-->", "", block, flags=re.S)
    for banned in ("work_key", "link_state", "poster_path",
                   "target_type", "chat_id", "list_item_id", "dedup_key"):
        hits = [m for m in re.finditer(re.escape(banned), stripped)
                if not re.search(r"sc[A-Za-z]+\." + re.escape(banned),
                                 stripped[max(0, m.start() - 24):m.end()])]
        assert not hits, f"推送任务页模板直出内部字段：{banned}"
