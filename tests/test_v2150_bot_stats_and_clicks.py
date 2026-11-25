"""v2.1.50：阅读量采集改 bot 直连 + 新增「点击人数」独立指标。

两件事，两条红线：

一、**登录不再要手机号和验证码**
   真机痛点：登录 tgapi 太费劲（人工收码、超时、还要在服务器上养一个真人账号的会话）。
   关键事实：Bot 走 MTProto 的 `get_messages(ids=...)` 一样能读到 `Message.views`
   （`getHistory` 才被服务端拒）。所以整条链路可以只用推送 Bot 自己的令牌：
   会话 `tg_bot_<id>.session`，首次采集自动签入，人工零操作。
   顺带删掉 `api_base=` —— telethon 1.44 的 TelegramClient 没这个参数，传了必 TypeError。

二、**点击人数是独立指标，不与阅读量混算**
   阅读量 = Telegram 给的累计**次数**；点击 = 我们自己数的**人**（按人去重）。
   两者不同源，报表必须分列，不得相加、不得互相替代。

修复前本文件应红：session 名带手机号 / 登录残留两步 / 推送卡片没有 pc. 按钮 /
点击不去重 / 报表把点击并进阅读量。
"""
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app import runtime_config, tg_stats, tg_stats_service
from app.db import SessionLocal, migrate
from app.models import (
    AppConfig, AuditLog, Bot, Channel, DiscoveredChat, MediaRequest, MirrorDir,
    MirrorFile, MirrorMedia, MirrorRule, MirrorTask, ProcessedUpdate, QueueItem,
    Resource, ScanTask, TelegramMessageStat, TelegramPushClick, UserContribution,
    UserFeedback,
)

ROOT = Path(__file__).resolve().parents[1]
TG_SRC = (ROOT / "backend/app/tg_stats.py").read_text(encoding="utf-8")
API_SRC = (ROOT / "backend/app/tg_stats_api.py").read_text(encoding="utf-8")
PUB_SRC = (ROOT / "backend/app/publisher.py").read_text(encoding="utf-8")
LISTENER_SRC = (ROOT / "backend/app/bot_listener.py").read_text(encoding="utf-8")
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")

TABLES = [TelegramPushClick, TelegramMessageStat, MirrorFile, MirrorTask, MirrorDir,
          MirrorMedia, MirrorRule, QueueItem, Resource, ScanTask, Channel, Bot,
          DiscoveredChat, UserContribution, UserFeedback, MediaRequest,
          ProcessedUpdate, AppConfig, AuditLog]


def _clean():
    with SessionLocal() as db:
        for t in TABLES:
            try:
                db.query(t).delete()
            except Exception:
                db.rollback()
        db.commit()


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    _clean()
    yield
    _clean()


@pytest.fixture
def db(_fresh):
    with SessionLocal() as session:
        try:
            yield session
        finally:
            session.rollback()


# ---------------------------------------------------------------------------
# ① 采集身份 = 推送 Bot（不再有手机号）
# ---------------------------------------------------------------------------

def _bot(db, name="推送Bot", token="123456:ABCDEF", enabled=True):
    b = Bot(name=name, token=token, enabled=enabled)
    db.add(b)
    db.commit()
    return b


def _channel(db, bot, chat_id="-100123", kind="channel", enabled=True):
    ch = Channel(bot_id=bot.id, name="测试频道", chat_id=chat_id, kind=kind,
                 enabled=enabled)
    db.add(ch)
    db.commit()
    return ch


def test_session_file_named_after_bot(db):
    bot = _bot(db, name="主推送Bot")
    _channel(db, bot)
    path = tg_stats._session_path(db)
    assert path.name == f"tg_bot_{bot.id}.session", (
        f"会话文件名仍是 {path.name!r} —— 手机号没了以后按手机号派生会退化成 "
        "tg_default.session，多个 Bot 会串同一个会话")


def test_stats_bot_follows_push_bot(db):
    """不指定时跟着推送用同一个 Bot —— 它一定在频道/群里，才读得到消息。"""
    unused = _bot(db, name="闲置Bot", token="1:a")
    used = _bot(db, name="推送中Bot", token="2:b")
    _channel(db, used)
    assert tg_stats._stats_bot(db).id == used.id
    assert tg_stats._stats_bot(db).id != unused.id


def test_stats_bot_respects_explicit_choice(db):
    first = _bot(db, name="BotA", token="1:a")
    second = _bot(db, name="BotB", token="2:b")
    _channel(db, first)
    runtime_config.set_many(db, {tg_stats.CFG_BOT_ID: str(second.id)})
    db.commit()
    assert tg_stats._stats_bot(db).id == second.id


def test_status_says_bot_missing(db):
    """凭据齐了但一个 Bot 都没有 → 必须点名说「缺 Bot」，不能只说「未配置」。"""
    runtime_config.set_many(db, {tg_stats.CFG_API_ID: "12345",
                                 tg_stats.CFG_API_HASH: "0123456789abcdef0123456789abcdef"})
    db.commit()
    st = tg_stats.status(db)
    assert st["configured"] is True
    assert st["bot_ready"] is False
    assert "Bot" in st["reason"], st["reason"]


def test_status_reports_bot_name_not_token(db):
    bot = _bot(db, name="主推送Bot", token="123456:SECRET-TOKEN")
    _channel(db, bot)
    st = tg_stats.status(db)
    assert st["bot_ready"] is True
    assert st["bot_name"] == "主推送Bot"
    flat = repr(st)
    assert "SECRET-TOKEN" not in flat, "状态里绝不能出现 Bot 令牌明文"


def test_phone_login_code_is_gone():
    """手机号 + 验证码那条路彻底删掉，不留半套（留一半只会让人以为还能用）。"""
    for token in ("CFG_PHONE", "tg_phone", "request_login_code", "complete_login",
                  "_PENDING_LOGIN", "send_code_request"):
        assert token not in TG_SRC, f"手机号登录残留：{token}"
    for token in ("/stats/login/code", "/stats/login/verify"):
        assert token not in API_SRC, f"手机号登录接口残留：{token}"
    assert "/stats/login/bot" in API_SRC


def test_api_base_param_is_gone():
    """telethon 1.44 的 TelegramClient 没有 api_base 参数 —— 传了就是 TypeError。"""
    assert "api_base=" not in TG_SRC, (
        "还有 api_base= —— telethon 1.44 不支持这个参数，采集链路一进来就炸")


# ---------------------------------------------------------------------------
# ② 自动签入（人工零操作）
# ---------------------------------------------------------------------------

class _FakeClient:
    """最小 Telethon 替身：只实现采集链路用到的几个方法。"""

    instances = []
    authorized = False
    signed = []

    def __init__(self, session, api_id=None, api_hash=None, **kw):
        self.session = session
        self.kw = kw
        _FakeClient.instances.append(self)

    async def connect(self):
        return None

    async def disconnect(self):
        return None

    async def is_user_authorized(self):
        return _FakeClient.authorized

    async def sign_in(self, **kw):
        _FakeClient.signed.append(kw)


def _fake_require_telethon():
    def factory(*a, **kw):
        return _FakeClient(*a, **kw)
    return factory


def test_ensure_bot_login_signs_in_with_bot_token(db):
    _FakeClient.instances = []
    _FakeClient.signed = []
    _FakeClient.authorized = False
    bot = _bot(db, name="主推送Bot", token="123456:ABCDEF")
    _channel(db, bot)
    runtime_config.set_many(db, {tg_stats.CFG_API_ID: "12345",
                                 tg_stats.CFG_API_HASH: "0123456789abcdef0123456789abcdef"})
    db.commit()
    with patch.object(tg_stats, "_require_telethon", _fake_require_telethon):
        out = asyncio.run(tg_stats.ensure_bot_login(db))
    assert out["ok"] is True and out["already"] is False
    assert _FakeClient.signed and _FakeClient.signed[0].get("bot_token") == "123456:ABCDEF", \
        "没有用 Bot 令牌签入 —— 那还是得靠手机号"
    # 不允许把 api_base 之类不存在的参数塞给 TelegramClient
    assert "api_base" not in _FakeClient.instances[0].kw


def test_ensure_bot_login_is_idempotent(db):
    _FakeClient.instances = []
    _FakeClient.signed = []
    _FakeClient.authorized = True
    bot = _bot(db, name="主推送Bot", token="123456:ABCDEF")
    _channel(db, bot)
    runtime_config.set_many(db, {tg_stats.CFG_API_ID: "12345",
                                 tg_stats.CFG_API_HASH: "0123456789abcdef0123456789abcdef"})
    db.commit()
    with patch.object(tg_stats, "_require_telethon", _fake_require_telethon):
        out = asyncio.run(tg_stats.ensure_bot_login(db))
    assert out["already"] is True
    assert _FakeClient.signed == [], "已有有效会话时不该重复签入"


def test_ensure_authorized_auto_signs_on_first_collect(db):
    """首次采集自己签 —— 用户不用先去页面点任何按钮。"""
    _FakeClient.authorized = False
    _FakeClient.signed = []
    bot = _bot(db, name="主推送Bot", token="123456:ABCDEF")
    _channel(db, bot)
    client = _FakeClient("x")
    asyncio.run(tg_stats._ensure_authorized(client, db))
    assert _FakeClient.signed and _FakeClient.signed[0]["bot_token"] == "123456:ABCDEF"


def test_ensure_authorized_without_bot_explains_it(db):
    client = _FakeClient("x")
    _FakeClient.authorized = False
    with pytest.raises(tg_stats.MTProtoUnavailable) as exc:
        asyncio.run(tg_stats._ensure_authorized(client, db))
    assert "Bot" in str(exc.value)


def test_frontend_has_single_connect_button():
    """页面只剩一个「测试连接」，手机号/验证码两个输入全没了。"""
    assert "scStatsConnectBot" in APP_VUE
    assert "/sc/stats/login/bot" in APP_VUE
    for gone in ("登录手机号", "scStatsConfig.phone", "获取登录验证码", "完成登录"):
        assert gone not in APP_VUE, f"手机号登录残留：{gone}"


# ---------------------------------------------------------------------------
# ③ 推送卡片带「想看」按钮（只给真正推出去的卡片）
# ---------------------------------------------------------------------------

def _res(url="https://www.guangyapan.com/s/abc"):
    return SimpleNamespace(share_url=url, media_title="完美世界")


def test_push_card_has_click_button(db):
    from app import publisher
    _caption, markup = publisher.share_link_presentation(
        db, _res(), "正文\nhttps://www.guangyapan.com/s/abc", queue_id=7)
    rows = markup["inline_keyboard"]
    assert len(rows) == 2, markup
    assert rows[0][0]["url"].startswith("https://")
    assert rows[1][0]["callback_data"] == "pc.7", (
        "按钮回调必须是 pc.<队列号>，bot_listener 只认这个前缀")
    assert "获取资源" not in rows[1][0]["text"]


def test_search_card_has_no_click_button(db):
    """Bot 搜索结果不是推送，加点击按钮会污染口径 —— 不传 queue_id 就不加。"""
    from app import publisher
    _caption, markup = publisher.share_link_presentation(
        db, _res(), "正文\nhttps://www.guangyapan.com/s/abc")
    assert len(markup["inline_keyboard"]) == 1


def test_send_telegram_passes_queue_id():
    """推送入口必须把队列号传进去，否则按钮永远不出现。"""
    assert "share_link_presentation(db, res, caption, queue_id=q.id)" in PUB_SRC


# ---------------------------------------------------------------------------
# ④ 点击落库：按人去重、不改消息
# ---------------------------------------------------------------------------

def _cb(queue_id, user_id=111, chat_id="-100123", cb_id="CB1"):
    return {"id": cb_id, "data": f"pc.{queue_id}",
            "from": {"id": user_id},
            "message": {"message_id": 99, "chat": {"id": chat_id}}}


class _ApiSpy:
    calls = []

    @classmethod
    async def fake(cls, db, token, method, payload):
        cls.calls.append((method, payload))
        return {"ok": True}


def _click(db, cb):
    from app import bot_listener
    with patch.object(bot_listener, "_api", _ApiSpy.fake):
        return asyncio.run(bot_listener._handle_push_click(db, "tok", cb, cb["data"]))


def test_click_recorded_once_per_user(db):
    _ApiSpy.calls = []
    assert _click(db, _cb(7, user_id=111)) == "click"
    _click(db, _cb(7, user_id=111, cb_id="CB2"))
    rows = list(db.scalars(select(TelegramPushClick)).all())
    assert len(rows) == 1, "同一个人点两次被记成两行 —— 人数会虚高"
    assert rows[0].queue_id == 7 and rows[0].user_id == "111"


def test_click_counts_different_users(db):
    _ApiSpy.calls = []
    _click(db, _cb(7, user_id=111))
    _click(db, _cb(7, user_id=222, cb_id="CB3"))
    assert len(list(db.scalars(select(TelegramPushClick)).all())) == 2


def test_click_answers_but_does_not_edit_message(db):
    _ApiSpy.calls = []
    _click(db, _cb(7))
    methods = [m for m, _ in _ApiSpy.calls]
    assert "answerCallbackQuery" in methods, "不 answer，用户点完没有任何反馈"
    assert "editMessageText" not in methods, (
        "点击把推送卡片编辑掉了 —— 卡片必须保持原样，只回一句短提示")
    answered = dict(_ApiSpy.calls)["answerCallbackQuery"]
    assert answered.get("text"), "answer 不带文字，用户看不到任何反馈"


def test_click_routed_by_prefix_in_callback():
    """`pc.` 必须**先于**统一 answer 处理，否则 answer 已经用掉、提示发不出去。"""
    body = LISTENER_SRC[LISTENER_SRC.index("async def _handle_callback("):]
    i = body.index('if data.startswith("pc."):')
    j = body.index('await _api(db, token, "answerCallbackQuery"')
    assert i < j, "pc. 分支在统一 answer 之后，点击反馈会被吞掉"


# ---------------------------------------------------------------------------
# ⑤ 报表：点击人数单列，绝不与阅读量混算
# ---------------------------------------------------------------------------

def _add_stat(db, queue_id, message_id, views, work_key="work:x", kind="channel"):
    row = TelegramMessageStat(
        queue_id=queue_id, chat_id="-100123", target_kind=kind,
        target_name="测试频道", message_id=str(message_id), views=views,
        forwards=0, reactions=0, available=True, work_key=work_key,
        collected_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    db.add(row)
    db.commit()
    return row


def _add_click(db, queue_id, user_id, work_key="work:x", chat_id="-100123"):
    row = TelegramPushClick(queue_id=queue_id, chat_id=chat_id,
                            user_id=str(user_id), work_key=work_key,
                            clicked_at=datetime.now(timezone.utc) - timedelta(hours=1))
    db.add(row)
    db.commit()
    return row


def test_report_keeps_clickers_out_of_reads(db):
    _add_stat(db, 1, 101, 100)
    _add_stat(db, 1, 101, 400)          # 同一条消息两次快照 → 增量 300
    _add_click(db, 1, 111)
    _add_click(db, 1, 222)
    report = tg_stats_service.trend_report(db, 7)
    assert report["totals"]["views_delta"] == 300
    assert report["totals"]["clickers"] == 2, "点击人数没有单列"
    # 红线：点击不得并进阅读量
    assert report["totals"]["views_delta"] != 300 + 2


def test_report_dedupes_clickers_per_work(db):
    """同一个人点了同一部作品的两条推送 → 作品维度仍是 1 人。"""
    _add_stat(db, 1, 101, 100, work_key="work:x")
    _add_stat(db, 1, 101, 260, work_key="work:x")
    _add_stat(db, 2, 102, 100, work_key="work:x")
    _add_stat(db, 2, 102, 180, work_key="work:x")
    _add_click(db, 1, 111, work_key="work:x")
    _add_click(db, 2, 111, work_key="work:x")
    report = tg_stats_service.trend_report(db, 7)
    assert report["totals"]["clickers"] == 1
    assert report["works"][0]["clickers"] == 1


def test_api_trend_exposes_clickers_separately(db):
    from app import tg_stats_api
    _add_stat(db, 1, 101, 100)
    _add_stat(db, 1, 101, 250)
    _add_click(db, 1, 111)
    out = tg_stats_api.api_stats_trend(days=7, db=db)
    assert out["totals"]["reads_gain"] == 150
    assert out["totals"]["clickers"] == 1
    assert out["works"][0]["clickers"] == 1
    assert out["targets"][0]["clickers"] == 1


def test_api_never_leaks_backend_keys(db):
    """点击统计带上了 chat_id，但接口层不许把它输出。"""
    from app import tg_stats_api
    _add_stat(db, 1, 101, 100)
    _add_click(db, 1, 111)
    out = tg_stats_api.api_stats_trend(days=7, db=db)
    flat = repr(out)
    for bad in ("chat_id", "-100123", "work_key", "queue_id"):
        assert bad not in flat, f"接口暴露了后端字段：{bad}"


def test_frontend_shows_clickers_as_its_own_column():
    assert "想看人数" in APP_VUE
    assert "期间点「想看」人数" in APP_VUE
    # 不得把点击并进阅读量的展示里
    assert "点击" not in APP_VUE.split("期间新增阅读")[0][-200:]
