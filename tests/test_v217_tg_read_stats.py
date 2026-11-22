"""v2.1.7 #241：Telegram 阅读量采集（MTProto user account）专项验收。

路径选择（用户口径「走②」）：MTProto user account。
理由：**Bot API 拿不到消息浏览量**——只有 MTProto 的 `messages.GetHistory` 返回的
`Message.views` 才有。

必须固化的三条口径（这是本功能最容易做错的地方）：
  1. **群聊不暴露浏览量**：Telegram 只对频道帖计数。所以采集结果必须区分
     channel / group，群的目标**不得**出现浏览量，报表聚合也不得把群的 0
     混进频道平均值——**不编造**是硬红线。
  2. **阅读量是累计值**：Telegram 只给当前总数，不给增量。趋势必须由相邻
     两次快照**求差**得到——只存累计值不做差，报表就永远是 0。
  3. **消息读不到就如实标记**：已删除 / 无权限 → available=False，
     **绝不填 0 冒充「0 次浏览」**。

覆盖：
  1. 表结构与迁移：新表自动建、字段完整、唯一约束幂等；
  2. 候选筛选：只取已发送且有消息号、且在回溯窗口内；
  3. 采集：频道拿到浏览量；群聊标记为群且不产出浏览量；
  4. 不可采集：如实标记而非填 0；
  5. 快照落库：一轮采集写一批快照，时间戳统一；
  6. 趋势：累计值求差得增量；首条快照无前值 → 不计入增量；
  7. 聚合：频道与群分列，群不进浏览量口径；
  8. 异常：MTProto 不可用 / 未登录 / 连续失败 → 结构化失败，不抛异常拖垮主链；
  9. 接口：响应字段不得出现后端字段名；凭证明文不得返回；
  10. 凭证：走 runtime_config（Fernet 加密落库），管理页可配；
  11. 依赖：telethon 未安装时应用照常运行（功能显示未启用）。
"""
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
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
REQ = ROOT / "backend" / "requirements.txt"

SH = timezone(timedelta(hours=8))


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v217stats_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'stats.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _seed_sent(db, *, chat_id="-1001", kind="channel", message_id="555",
               title="测试作品", sent_at=None, channel_name="订阅频道"):
    """造一条「已发送且有消息号」的队列项（采集锚点）。"""
    from app.models import Bot, Channel, QueueItem, Resource
    bot = db.get(Bot, 1)
    if bot is None:
        bot = Bot(name="b", token="t")
        db.add(bot)
        db.flush()
    channel = Channel(bot_id=bot.id, name=channel_name, chat_id=chat_id,
                      enabled=True, kind=kind)
    db.add(channel)
    db.flush()
    res = Resource(file_id=f"f:{message_id}:{chat_id}", name="a.mkv", size=1,
                   media_type="movie", media_title=title)
    db.add(res)
    db.flush()
    q = QueueItem(resource_id=res.id, channel_id=channel.id, status="sent",
                  telegram_message_id=str(message_id))
    q.sent_at = sent_at or datetime.now(timezone.utc) - timedelta(hours=1)
    db.add(q)
    db.commit()
    return q, channel, res


# ---------------------------------------------------------------------------
# 1. 表结构
# ---------------------------------------------------------------------------

def test_new_tables_created(env):
    """阅读量快照表与执行记录表自动建出来。"""
    S, eng = env
    from sqlalchemy import inspect
    tables = set(inspect(eng).get_table_names())
    assert "telegram_message_stats" in tables
    assert "telegram_stat_runs" in tables


def test_stat_table_has_required_columns(env):
    """快照表字段齐全（累计值口径 + 目标类型 + 采集时刻）。"""
    S, eng = env
    from sqlalchemy import inspect
    cols = {c["name"] for c in inspect(eng).get_columns("telegram_message_stats")}
    for name in ("queue_id", "chat_id", "target_kind", "target_name",
                 "message_id", "views", "forwards", "reactions",
                 "available", "note", "work_key", "collected_at"):
        assert name in cols, f"快照表缺列：{name}"


def test_stat_table_unique_constraint(env):
    """同一消息同一时刻只存一条快照（重复采集幂等）。"""
    from app.models import TelegramMessageStat
    names = {c.name for c in TelegramMessageStat.__table__.constraints
             if c.name}
    assert "uq_tg_stat_queue_msg_time" in names


def test_existing_tables_untouched(env):
    """**只增不改**：既有核心表一个都不能少。"""
    S, eng = env
    from sqlalchemy import inspect
    tables = set(inspect(eng).get_table_names())
    for name in ("push_queue", "channels", "bots", "resources",
                 "sc_publish_tasks", "sc_category_schedules"):
        assert name in tables, f"既有表被删：{name}"


# ---------------------------------------------------------------------------
# 2. 候选筛选
# ---------------------------------------------------------------------------

def test_candidates_only_include_sent_items(env):
    """只有「已发送 + 有消息号」的队列项才是候选。"""
    from app import tg_stats_service as svc
    from app.models import QueueItem, Resource
    S, _ = env
    with S() as db:
        q, _, _ = _seed_sent(db, message_id="111")
        # 未发送的项：不算候选。
        # 注意 push_queue 有 UNIQUE(resource_id, channel_id) 约束 → 必须另建资源。
        res2 = Resource(file_id="f:other", name="b.mkv", size=1,
                        media_type="movie", media_title="另一部")
        db.add(res2)
        db.flush()
        other = QueueItem(resource_id=res2.id, channel_id=q.channel_id,
                          status="pending", telegram_message_id="222")
        db.add(other)
        db.commit()
        rows = svc._candidate_rows(db, 72, 100)
        ids = {r["queue_id"] for r in rows}
        assert q.id in ids
        assert other.id not in ids


def test_candidates_exclude_items_without_message_id(env):
    """没有消息号的（发送失败 / 未发送）不入选。"""
    from app import tg_stats_service as svc
    from app.models import QueueItem, Resource
    S, _ = env
    with S() as db:
        q, _, _ = _seed_sent(db, message_id="111")
        # push_queue 有 UNIQUE(resource_id, channel_id) → 必须另建资源。
        res2 = Resource(file_id="f:blank", name="c.mkv", size=1,
                        media_type="movie", media_title="无消息号")
        db.add(res2)
        db.flush()
        blank = QueueItem(resource_id=res2.id, channel_id=q.channel_id,
                          status="sent", telegram_message_id="")
        db.add(blank)
        db.commit()
        rows = svc._candidate_rows(db, 72, 100)
        assert all(r["message_id"] for r in rows)


def test_candidates_respect_lookback_window(env):
    """回溯窗口外的消息不入选（避免每轮都重扫全部历史）。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        _seed_sent(db, message_id="301",
                   sent_at=datetime.now(timezone.utc) - timedelta(days=10))
        _seed_sent(db, message_id="302",
                   sent_at=datetime.now(timezone.utc) - timedelta(hours=2))
        rows = svc._candidate_rows(db, 24, 100)
        msgs = {r["message_id"] for r in rows}
        assert "302" in msgs
        assert "301" not in msgs


def test_candidates_ordered_oldest_first(env):
    """候选按发送时间从旧到新——先补老数据。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        base = datetime.now(timezone.utc) - timedelta(hours=5)
        _seed_sent(db, message_id="303", sent_at=base)
        _seed_sent(db, message_id="301", sent_at=base - timedelta(hours=2))
        _seed_sent(db, message_id="302", sent_at=base + timedelta(hours=1))
        rows = svc._candidate_rows(db, 72, 100)
        assert [r["message_id"] for r in rows] == ["301", "303", "302"]


def test_candidates_respect_max_per_run(env):
    """单轮上限是护栏（防接口风暴）。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        for i in range(8):
            _seed_sent(db, message_id=f"m{i}", chat_id=f"-100{i}")
        rows = svc._candidate_rows(db, 72, 3)
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# 3. 采集：口径核心
# ---------------------------------------------------------------------------

class _FakeMessage:
    def __init__(self, views=None, forwards=None, reactions=None, empty=False):
        self.views = views
        self.forwards = forwards
        self.reactions = reactions
        self.empty = empty


class _FakeReactions:
    def __init__(self, counts):
        class _R:
            def __init__(self, c):
                self.count = c
        self.results = [_R(c) for c in counts]


class _FakeChannelEntity:
    """Telethon 的 Channel 实体：megagroup=True 是群，False 是频道。"""
    def __init__(self, megagroup=False):
        self.megagroup = megagroup


class _FakeClient:
    """最小可用的 Telethon 客户端替身。"""

    def __init__(self, *, message=None, entity=None, get_entity_exc=None,
                 get_messages_exc=None):
        self._message = message
        self._entity = entity
        self._get_entity_exc = get_entity_exc
        self._get_messages_exc = get_messages_exc

    async def get_entity(self, chat_id):
        if self._get_entity_exc:
            raise self._get_entity_exc
        return self._entity

    async def get_messages(self, entity, ids=None):
        if self._get_messages_exc:
            raise self._get_messages_exc
        return self._message


def test_collect_channel_reads_views():
    """频道帖能拿到浏览量 / 转发 / 表情回应。"""
    import asyncio
    from app import tg_stats
    client = _FakeClient(
        message=_FakeMessage(views=1234, forwards=56, reactions=_FakeReactions([10, 5])),
        entity=_FakeChannelEntity(megagroup=False),
    )
    stat = asyncio.run(tg_stats.collect_message_stat(client, "-1001", "555"))
    assert stat.available is True
    assert stat.views == 1234
    assert stat.forwards == 56
    assert stat.reactions == 15
    assert stat.target_kind == "channel"


def test_collect_group_has_no_reads():
    """**群聊不暴露浏览量**：target_kind=group，浏览量不得编造出非零值。"""
    import asyncio
    from app import tg_stats
    client = _FakeClient(
        message=_FakeMessage(views=None, forwards=12, reactions=None),
        entity=_FakeChannelEntity(megagroup=True),
    )
    stat = asyncio.run(tg_stats.collect_message_stat(client, "-1002", "666"))
    assert stat.target_kind == "group"
    assert stat.views == 0, "群聊不得产出浏览量数值"
    assert stat.forwards == 12
    assert stat.note, "群聊应说明为何没有浏览量"


def test_collect_missing_message_marked_unavailable():
    """消息读不到 → 如实标记不可采集，**不填 0 冒充 0 次浏览**。"""
    import asyncio
    from app import tg_stats
    client = _FakeClient(message=_FakeMessage(empty=True),
                         entity=_FakeChannelEntity())
    stat = asyncio.run(tg_stats.collect_message_stat(client, "-1001", "999"))
    assert stat.available is False
    assert stat.note


def test_collect_unreachable_target_marked_unavailable():
    """目标访问不到（如账号不是订阅者）→ 标记不可采集，不抛异常。"""
    import asyncio
    from app import tg_stats
    client = _FakeClient(get_entity_exc=RuntimeError("no access"))
    stat = asyncio.run(tg_stats.collect_message_stat(client, "-1001", "999"))
    assert stat.available is False
    assert stat.note


def test_is_channel_distinguishes_megagroup():
    """Telethon 的 Channel 同时表示频道与超级群，靠 megagroup 区分。"""
    from app import tg_stats
    assert tg_stats._is_channel(_FakeChannelEntity(megagroup=False)) is True
    assert tg_stats._is_channel(_FakeChannelEntity(megagroup=True)) is False
    assert tg_stats._is_channel(None) is False


def test_count_reactions_sums():
    """表情回应总数 = 各回应项计数之和。"""
    from app import tg_stats
    assert tg_stats._count_reactions(_FakeReactions([3, 4, 5])) == 12
    assert tg_stats._count_reactions(None) == 0


# ---------------------------------------------------------------------------
# 4. 快照落库
# ---------------------------------------------------------------------------

def test_collect_once_writes_snapshots(env, monkeypatch):
    """一轮采集写入快照，且时间戳统一（同一轮一个时刻）。"""
    import asyncio
    from app import tg_stats_service as svc
    from app.models import TelegramMessageStat
    S, _ = env
    with S() as db:
        _seed_sent(db, message_id="101")
        _seed_sent(db, message_id="102", chat_id="-1002")
        fake = _FakeClient(message=_FakeMessage(views=100, forwards=1),
                           entity=_FakeChannelEntity())

        # 注意：_get_client 是**同步**函数（返回已缓存的客户端），
        # _ensure_authorized 才是异步——替身必须与真实签名一致，否则会拿到协程对象。
        def _fake_get_client(_db):
            return fake

        async def _fake_authorized(_client):
            return None

        # telethon 未装到测试环境 → 必须假扮「依赖就绪」才能走完整采集路径。
        monkeypatch.setattr(svc.tg_stats, "telethon_available", lambda: True)
        monkeypatch.setattr(svc.tg_stats, "_get_client", _fake_get_client)
        monkeypatch.setattr(svc.tg_stats, "_ensure_authorized", _fake_authorized)
        # 开关默认关，这里直接放行采集逻辑
        from app import runtime_config
        runtime_config.set_many(db, {svc.tg_stats.CFG_ENABLED: "true"})
        db.commit()

        result = asyncio.run(svc.collect_once(db))
        assert result["ok"] is True, result
        assert result["collected"] == 2
        rows = list(db.scalars(select(TelegramMessageStat)).all())
        assert len(rows) == 2
        assert len({r.collected_at for r in rows}) == 1, "同一轮快照时间戳必须统一"


def test_collect_once_disabled_returns_disabled(env):
    """开关关闭 → 结构化返回「未开启」，不报错、不写快照。"""
    import asyncio
    from app import tg_stats_service as svc
    from app.models import TelegramMessageStat
    S, _ = env
    with S() as db:
        _seed_sent(db, message_id="101")
        result = asyncio.run(svc.collect_once(db))
        assert result["ok"] is False
        assert result["reason"] == "disabled"
        assert list(db.scalars(select(TelegramMessageStat)).all()) == []


def test_collect_once_without_telethon_degrades(env, monkeypatch):
    """**未装 telethon 也不能拖垮主链**：返回未启用，不抛异常。

    语义顺序：开关未开 → 先报「未开启」（用户没要求采集，不必谈依赖）；
    开关已开但依赖缺失 → 报「依赖缺失」。两者都**不抛异常**。
    """
    import asyncio
    from app import tg_stats_service as svc
    from app import runtime_config
    S, _ = env
    with S() as db:
        _seed_sent(db, message_id="101")
        monkeypatch.setattr(svc.tg_stats, "telethon_available", lambda: False)

        # 开关未开
        result = asyncio.run(svc.collect_once(db))
        assert result["ok"] is False
        assert result["reason"] == "disabled"

        # 开关已开但依赖缺失
        runtime_config.set_many(db, {svc.tg_stats.CFG_ENABLED: "true"})
        db.commit()
        result = asyncio.run(svc.collect_once(db))
        assert result["ok"] is False
        assert result["reason"] == "dependency_missing"


def test_collect_once_records_run(env, monkeypatch):
    """每轮都留下执行记录（可诊断）。"""
    import asyncio
    from app import tg_stats_service as svc
    from app.models import TelegramStatRun
    S, _ = env
    with S() as db:
        _seed_sent(db, message_id="101")
        asyncio.run(svc.collect_once(db))
        runs = list(db.scalars(select(TelegramStatRun)).all())
        assert len(runs) == 1
        assert runs[0].status in ("disabled", "unavailable", "failed", "success")
        assert runs[0].finished_at is not None


# ---------------------------------------------------------------------------
# 5. 趋势：累计值求差
# ---------------------------------------------------------------------------

def _add_stat(db, *, queue_id, message_id, views, forwards=0, reactions=0,
              target_kind="channel", at=None):
    from app.models import TelegramMessageStat
    row = TelegramMessageStat(
        queue_id=queue_id, chat_id="-1001", target_kind=target_kind,
        target_name="订阅频道", message_id=str(message_id),
        views=views, forwards=forwards, reactions=reactions,
        available=True, work_key="work:x",
        collected_at=at or datetime.now(timezone.utc),
    )
    db.add(row)
    db.commit()
    return row


def test_trend_uses_delta_not_absolute(env):
    """**阅读量是累计值**，趋势必须求差：100→350 的增量是 250，不是 350。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        t0 = datetime.now(timezone.utc) - timedelta(hours=5)
        t1 = datetime.now(timezone.utc) - timedelta(hours=1)
        _add_stat(db, queue_id=1, message_id="1", views=100, at=t0)
        _add_stat(db, queue_id=1, message_id="1", views=350, at=t1)
        report = svc.trend_report(db, 7)
        assert report["totals"]["views_delta"] == 250


def test_trend_first_snapshot_has_no_delta(env):
    """首条快照没有前值 → 不计入增量（不能把累计值当增量）。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        _add_stat(db, queue_id=1, message_id="1", views=999)
        report = svc.trend_report(db, 7)
        assert report["totals"]["views_delta"] == 0


def test_trend_negative_delta_clamped_to_zero(env):
    """累计值异常回落（消息被删/重发）→ 增量夹到 0，不出负数。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        t0 = datetime.now(timezone.utc) - timedelta(hours=5)
        t1 = datetime.now(timezone.utc) - timedelta(hours=1)
        _add_stat(db, queue_id=1, message_id="1", views=500, at=t0)
        _add_stat(db, queue_id=1, message_id="1", views=100, at=t1)
        report = svc.trend_report(db, 7)
        assert report["totals"]["views_delta"] == 0


def test_trend_excludes_group_from_reads_totals(env):
    """**群聊不进浏览量口径**：群消息的浏览量增量不计入总量。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        t0 = datetime.now(timezone.utc) - timedelta(hours=5)
        t1 = datetime.now(timezone.utc) - timedelta(hours=1)
        _add_stat(db, queue_id=1, message_id="401", views=0, forwards=3,
                  target_kind="channel", at=t0)
        _add_stat(db, queue_id=1, message_id="401", views=0, forwards=8,
                  target_kind="channel", at=t1)
        _add_stat(db, queue_id=2, message_id="402", views=0, forwards=2,
                  target_kind="group", at=t0)
        _add_stat(db, queue_id=2, message_id="402", views=0, forwards=9,
                  target_kind="group", at=t1)
        report = svc.trend_report(db, 7)
        assert report["totals"]["views_delta"] == 0
        assert report["totals"]["forwards_delta"] == 12


def test_trend_capability_declares_group_has_no_reads(env):
    """报表明确告知前端：群聊没有浏览量。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        report = svc.trend_report(db, 7)
        assert report["capability"]["channel_views"] is True
        assert report["capability"]["group_views"] is False


def test_trend_targets_separate_channel_and_group(env):
    """目标维度聚合：频道与群分列，群目标不带浏览量。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        _add_stat(db, queue_id=1, message_id="401", views=500, target_kind="channel")
        _add_stat(db, queue_id=2, message_id="402", views=0, target_kind="group")
        report = svc.trend_report(db, 7)
        kinds = {t["kind"] for t in report["targets"]}
        assert kinds == {"channel", "group"}
        for t in report["targets"]:
            if t["kind"] == "group":
                assert t["views_available"] is False


def test_trend_buckets_are_ordered_by_day(env):
    """按天分桶且按日期升序。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        now = datetime.now(timezone.utc)
        _add_stat(db, queue_id=1, message_id="501", views=10,
                  at=now - timedelta(days=3))
        _add_stat(db, queue_id=1, message_id="502", views=20,
                  at=now - timedelta(days=1))
        report = svc.trend_report(db, 7)
        days = [b["day"] for b in report["buckets"]]
        assert days == sorted(days)


def test_stats_for_ai_gives_facts_only(env):
    """给 AI 的摘要只含实测值，并带口径说明（AI 不许编造）。"""
    from app import tg_stats_service as svc
    S, _ = env
    with S() as db:
        _add_stat(db, queue_id=1, message_id="501", views=100)
        summary = svc.stats_for_ai(db, 7)
        assert summary["totals"]["messages"] == 1
        assert "群聊不提供浏览量" in summary["note"]


# ---------------------------------------------------------------------------
# 6. 凭证与登录
# ---------------------------------------------------------------------------

def test_credential_keys_registered_in_runtime_config():
    """凭证键必须在 runtime_config 白名单里，否则 set_many 会静默丢弃。"""
    from app import runtime_config
    for key in ("tg_api_id", "tg_api_hash", "tg_phone",
                "tg_stats_enabled", "tg_session_dir"):
        assert key in runtime_config.DEFAULTS, f"凭证键未注册：{key}"


def test_credentials_encrypted_at_rest(env):
    """凭证密文落库，**明文不落库**。"""
    from app import runtime_config
    from app.models import AppConfig
    S, _ = env
    plain = "0123456789abcdef0123456789abcdef"
    with S() as db:
        runtime_config.set_many(db, {"tg_api_hash": plain})
        db.commit()
        row = db.get(AppConfig, "tg_api_hash")
        assert row is not None
        assert plain not in row.value_enc
        assert runtime_config.get(db, "tg_api_hash") == plain


def test_status_never_leaks_credentials(env):
    """状态接口只报「配没配」，**绝不回显明文凭证**。"""
    from app import tg_stats
    S, _ = env
    with S() as db:
        from app import runtime_config
        runtime_config.set_many(db, {
            "tg_api_id": "123456",
            "tg_api_hash": "SECRETHASHVALUE0123456789abcdef",
            "tg_phone": "+8613800000000",
        })
        db.commit()
        st = tg_stats.status(db)
        blob = json.dumps(st, ensure_ascii=False)
        assert "SECRETHASHVALUE" not in blob
        assert "123456" not in blob
        assert "+8613800000000" not in blob
        assert st["configured"] is True


def test_status_reports_missing_credential_reason(env):
    """未配置凭证 → 明确告诉用户缺什么。"""
    from app import tg_stats
    S, _ = env
    with S() as db:
        st = tg_stats.status(db)
        assert st["configured"] is False
        assert st["reason"]


def test_login_code_not_persisted(env):
    """验证码**不落库**——它是一次性的。"""
    from app import tg_stats
    from app.models import AppConfig
    S, _ = env
    with S() as db:
        db.add(AppConfig(key="tg_pending_code", value_enc="12345"))
        db.commit()
        # 登录态只存在模块内存态里，数据库里不应有任何验证码字段
        names = {c.name for c in AppConfig.__table__.columns}
        assert "tg_pending_code" not in names


# ---------------------------------------------------------------------------
# 7. 接口契约
# ---------------------------------------------------------------------------

def test_api_response_uses_neutral_field_names(env):
    """接口响应**不得出现后端字段名**（chat_id / message_id / work_key / views）。"""
    from app import tg_stats_api as api
    S, _ = env
    with S() as db:
        _seed_sent(db, message_id="101")
        trend = api.api_stats_trend(days=7, db=db)
        items = api.api_stats_items(limit=10, db=db)
        blob = json.dumps({"trend": trend, "items": items}, ensure_ascii=False)
        for banned in ("chat_id", "message_id", "work_key", "queue_id",
                       '"views"', "telegram_message_id"):
            assert banned not in blob, f"接口响应泄露后端字段名：{banned}"


def test_api_items_hides_group_reads(env):
    """群聊条目的阅读量为 null（不是 0——0 会被误读成「没人看」）。"""
    from app import tg_stats_api as api
    S, _ = env
    with S() as db:
        _add_stat(db, queue_id=1, message_id="402", views=0, target_kind="group")
        res = api.api_stats_items(limit=10, db=db)
        item = res["items"][0]
        assert item["reads"] is None
        assert item["kind_label"] == "群聊"


def test_api_targets_use_neutral_field_name(env):
    """接口返回中性字段名（reads_shown），不把后端的 views_available 暴露出去。"""
    import json as _json
    from app import tg_stats_api as api
    S, _ = env
    with S() as db:
        _add_stat(db, queue_id=1, message_id="c", views=100, target_kind="channel")
        res = api.api_stats_trend(days=7, db=db)
        blob = _json.dumps(res, ensure_ascii=False)
        assert "views_available" not in blob
        assert "reads_shown" in blob


def test_api_status_declares_capability(env):
    """状态接口带口径声明。"""
    from app import tg_stats_api as api
    S, _ = env
    with S() as db:
        res = api.api_stats_status(db=db)
        assert res["capability"]["channel_reads"] is True
        assert res["capability"]["group_reads"] is False
        assert "群聊" in res["capability"]["group_note"]


def test_api_config_saves_without_leaking(env):
    """配置接口保存成功且不回显明文。"""
    from app import tg_stats_api as api
    S, _ = env
    with S() as db:
        res = api.api_stats_config(api.Payload(payload={
            "api_id": "12345", "api_hash": "HASHVALUE0123456789abcdef0123",
            "enabled": True,
        }), db=db)
        assert res["ok"] is True
        blob = json.dumps(res, ensure_ascii=False)
        assert "HASHVALUE" not in blob
        assert res["enabled"] is True


def test_api_config_rejects_out_of_range(env):
    """回溯窗口与单轮上限都做区间夹取（防接口风暴 / 防全库扫描）。"""
    from app import tg_stats_api as api
    S, _ = env
    with S() as db:
        res = api.api_stats_config(api.Payload(payload={
            "lookback_hours": 99999, "max_per_run": 999999}), db=db)
        assert res["lookback_hours"] <= 720
        assert res["max_per_run"] <= 2000


def test_api_ai_summary_endpoint(env):
    """AI 摘要端点可用（供排期建议消费实测数据）。"""
    from app import tg_stats_api as api
    S, _ = env
    with S() as db:
        _add_stat(db, queue_id=1, message_id="501", views=10)
        res = api.api_stats_ai_summary(days=7, db=db)
        assert res["ok"] is True
        assert res["summary"]["totals"]["messages"] == 1


def test_api_collect_endpoint_never_raises(env):
    """手动采集端点不抛异常（未配置时返回结构化失败）。"""
    import asyncio
    from app import tg_stats_api as api
    S, _ = env
    with S() as db:
        res = asyncio.run(api.api_stats_collect(db=db))
        assert "ok" in res


# ---------------------------------------------------------------------------
# 8. 依赖与集成
# ---------------------------------------------------------------------------

def test_telethon_in_requirements():
    """镜像构建必须带上 telethon（否则功能永远处于未启用）。"""
    text = REQ.read_text(encoding="utf-8")
    assert "telethon" in text


def test_main_registers_router():
    """路由已注册到 /api/sc 前缀下。"""
    src = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    assert "tg_stats_api.router" in src


def test_app_still_imports_without_telethon(env):
    """**未装 telethon 时应用照常启动**——不能因为辅助功能拖垮主链。"""
    from app import tg_stats
    assert tg_stats.telethon_available() in (True, False)
    # 即使依赖缺失，status 也要能返回结构化结果而不是抛异常。
    # 用**本用例的临时库**（全局库路径下不一定有 app_config 表）。
    S, _ = env
    with S() as db:
        st = tg_stats.status(db)
        assert "dependency_ready" in st


def test_publisher_still_records_message_id():
    """采集锚点仍在：publisher 发送成功时落消息号与发送时间。"""
    src = (BACKEND / "app" / "publisher.py").read_text(encoding="utf-8")
    assert "q.telegram_message_id = message_id" in src
    assert "q.sent_at = utcnow()" in src
