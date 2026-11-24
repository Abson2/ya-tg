"""v2.1.32：Bot「建议/反馈」指令 + 算术验证码 + 后台「用户留言板」。

需求（用户原话三句）：
  ① 后台「分发与互动中心」b 板块在「用户求片」「用户投稿」**旁并列**加「用户留言板」，
     用户留言后管理员在后台回复，并通过 tgbot 反馈给对应用户；
  ② 用户点「提交建议/反馈」指令时，要在对话框里输入**验证码**防刷；
  ③ 内置指令变成 **5 个按钮**（3+2 两行），注意排版。

本文件锁死四组口径：

  A. 留言链路：验证码 → 正文 → 落库。裸文字（没进会话态）一个字都不许落库。
  B. 验证码：答对才放行；答错换新题、非数字不扣次数、连错 3 次退出；
     **答案只在内存**，库里没有任何验证码痕迹（这是「不落库」的反证）。
  C. 后台回复：归档 + 回执两步，**回复先落库再发 TG**——发不出去也绝不回滚回复
     （反证：notify 返回 False 时 reply_text 仍在）。
  D. 展示口径：页面只说「待回复 / 已回复」，绝不露 pending/replied 原文；
     b 板块三列并排且 ≤1100px 统一降单列。

只用纯标准库 + 项目现有依赖，不引入 Pillow（图片验证码方案已被否决）。
"""
import asyncio
import itertools

import pytest
from sqlalchemy import delete, inspect, select

from app import bot_listener as bl
from app.db import SessionLocal, engine, migrate
from app.main import (
    FeedbackReplyInput,
    delete_feedback,
    feedback_list,
    interaction_status,
    reply_feedback,
)
from app.models import AuditLog, UserFeedback

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"

_SEQ = itertools.count(1)


@pytest.fixture(autouse=True)
def _fresh():
    migrate()
    # 内存态（验证码 / 会话流）是模块级的，必须逐用例清干净，否则上一个用例的
    # 已验证会话会让下一个用例「跳过验证码直接落库」而假绿。
    bl._captcha_sessions.clear()
    bl._flow_sessions.clear()
    bl._pick_sessions.clear()
    for _ in range(2):
        with SessionLocal() as db:
            db.execute(delete(UserFeedback))
            db.commit()
    yield
    bl._captcha_sessions.clear()
    bl._flow_sessions.clear()
    bl._pick_sessions.clear()


# --------------------------------------------------------------------------
# 工具
# --------------------------------------------------------------------------
def _uid() -> str:
    """每个用例一个独立 tg_user_id，避免会话态串味。"""
    return str(900000 + next(_SEQ))


def _say(text, uid, *, username="tester", chat_id=None, message_id=7):
    """模拟用户发一条消息，返回 (action, reply)。"""
    async def go():
        with SessionLocal() as db:
            a, r = await bl.handle_text(
                db, text, tg_user_id=str(uid), tg_username=username,
                tg_chat_id=str(chat_id if chat_id is not None else uid),
                tg_message_id=message_id)
            db.commit()
            return a, r
    return asyncio.run(go())


def _rows():
    with SessionLocal() as db:
        return list(db.scalars(select(UserFeedback).order_by(UserFeedback.id)).all())


def _last_id():
    with SessionLocal() as db:
        return db.scalar(select(UserFeedback.id).order_by(UserFeedback.id.desc()).limit(1))


def _ask(uid):
    """发一次「建议/反馈」拿到验证码题面，返回 (action, 题面)。"""
    return _say("建议/反馈", uid)


def _answer_of(uid):
    return int(bl._captcha_get(uid)["answer"])


def _pass_captcha(uid):
    """走完验证码，停在「请发送正文」那一步。"""
    _ask(uid)
    return _say(str(_answer_of(uid)), uid)


# --------------------------------------------------------------------------
# A. 留言链路
# --------------------------------------------------------------------------
def test_table_created_by_migrate_with_expected_columns():
    """新表由 create_all 建出来，老库升级不需要手工 ALTER。"""
    cols = {c["name"] for c in inspect(engine).get_columns("user_feedback")}
    assert {"id", "tg_user_id", "tg_username", "content", "status",
            "reply_text", "replied_at", "tg_chat_id", "tg_message_id",
            "created_at"} <= cols, cols


def test_full_flow_captcha_then_content_saves_row():
    """验证码 → 正文 → 落库，并带上来源 chat/message 便于排查。"""
    uid = _uid()
    action, prompt = _ask(uid)
    assert action == "feedback_captcha"
    assert "算术题" in prompt and "次机会" in prompt

    action, hint = _say(str(_answer_of(uid)), uid)
    assert action == "feedback_captcha_ok"
    assert "验证通过" in hint

    action, thanks = _say("希望搜索能按年份过滤", uid)
    assert action == "feedback"
    assert "已提交" in thanks

    rows = _rows()
    assert len(rows) == 1
    row = rows[0]
    assert row.content == "希望搜索能按年份过滤"
    assert row.status == "pending"          # 落库就是待回复，不需要审核
    assert row.tg_user_id == uid
    assert row.tg_username == "tester"
    assert row.tg_chat_id == uid
    assert row.tg_message_id == 7
    assert row.reply_text == ""
    # 收完一条就把会话态与验证码一起清掉，不能留着给下一条文本复用
    assert bl._flow_get(uid) == ""
    assert bl._captcha_get(uid) is None


def test_bare_text_without_flow_writes_nothing():
    """反证：没点指令的裸文字，一个字都不许进留言板（v0.9.16 #277 的边界不能破）。"""
    uid = _uid()
    action, hint = _say("我觉得这个功能不错", uid)
    assert action == "unrecognized"
    assert "/建议" in hint                  # 纠正提示里要给出正确入口
    assert _rows() == []


def test_ending_flow_clears_captcha_and_writes_nothing():
    """反证：验证码没答就发「结束」，会话与验证码一起作废，不落库。"""
    uid = _uid()
    _ask(uid)
    assert bl._captcha_get(uid) is not None
    action, _ = _say("结束", uid)
    assert action == "flow_end"
    assert bl._captcha_get(uid) is None
    assert bl._flow_get(uid) == ""
    assert _rows() == []


def test_draft_in_command_is_reused_after_captcha():
    """指令里已经带了正文时，验证通过直接用草稿落库，不让用户再打一遍。"""
    uid = _uid()
    action, _ = _say("/建议 播放器希望支持倍速", uid)
    assert action == "feedback_captcha"

    action, thanks = _say(str(_answer_of(uid)), uid)
    assert action == "feedback"            # 不是 feedback_captcha_ok
    assert "验证通过" in thanks
    rows = _rows()
    assert len(rows) == 1
    assert rows[0].content == "播放器希望支持倍速"
    assert bl._captcha_get(uid) is None


def test_same_user_two_messages_are_two_rows():
    """留言不做去重合并：同一个人连发两条就是两条（管理员要看原话）。"""
    uid = _uid()
    for text in ("第一条：海报有点糊", "第二条：排期太晚"):
        _pass_captcha(uid)
        _say(text, uid)
    rows = _rows()
    assert [r.content for r in rows] == ["第一条：海报有点糊", "第二条：排期太晚"]


# --------------------------------------------------------------------------
# B. 验证码
# --------------------------------------------------------------------------
def test_captcha_is_not_persisted_anywhere():
    """反证：验证码纯内存，库里连一张验证码表都没有、也没有任何留言行。"""
    uid = _uid()
    _ask(uid)
    assert bl._captcha_get(uid) is not None          # 内存里有
    assert _rows() == []                             # 库里没有
    tables = set(inspect(engine).get_table_names())
    assert not [t for t in tables if "captcha" in t], "验证码不该落库"
    cols = {c["name"] for c in inspect(engine).get_columns("user_feedback")}
    assert not [c for c in cols if "captcha" in c or "answer" in c], cols


def test_wrong_answer_reissues_a_new_question_and_saves_nothing():
    """答错换一道新题（不是同一题反复问），且不落库。

    「换了新题」用**会话对象换代**来判定（`_captcha_issue` 每次都建新 dict）：
    直接比题面字符串会有极小概率新题与旧题撞成同一对操作数，那样断言本身会假红。
    """
    uid = _uid()
    _ask(uid)
    old_sess = bl._captcha_get(uid)
    wrong = _answer_of(uid) + 1
    action, prompt = _say(str(wrong), uid)
    assert action == "feedback_captcha_wrong"
    assert "答案不对" in prompt
    assert "还有 2 次机会" in prompt
    sess = bl._captcha_get(uid)
    assert sess["tries"] == 1
    assert sess["verified"] is False
    assert sess is not old_sess, "答错必须重新出题，不能在旧会话上原地重问"
    assert _rows() == []


def test_non_number_input_does_not_consume_a_try():
    """发来一句话不算答错：不扣次数、原题重问（打错一个符号就被扣次数太粗暴）。"""
    uid = _uid()
    _ask(uid)
    question = bl._captcha_get(uid)["question"]
    action, prompt = _say("我已经写了啊", uid)
    assert action == "feedback_captcha"
    assert "请直接发送数字答案" in prompt
    sess = bl._captcha_get(uid)
    assert sess["tries"] == 0
    assert sess["question"] == question
    assert _rows() == []


def test_three_wrong_attempts_exit_without_saving():
    """连错 3 次直接退出，会话与验证码清空，不落库。"""
    uid = _uid()
    _ask(uid)
    for i in range(3):
        action, _ = _say(str(_answer_of(uid) + 1), uid)
    assert action == "feedback_denied"
    assert bl._captcha_get(uid) is None
    assert bl._flow_get(uid) == ""
    assert _rows() == []
    # 退出后再发正文也不会被当成留言（会话已清）
    action, _ = _say("还是想留言", uid)
    assert action == "unrecognized"
    assert _rows() == []


def test_fullwidth_digits_are_accepted():
    """全角数字（中文输入法常见）要能答对，否则用户会以为验证码坏了。"""
    uid = _uid()
    _ask(uid)
    ans = str(_answer_of(uid))
    full = ans.translate(str.maketrans("0123456789", "０１２３４５６７８９"))
    action, _ = _say(full, uid)
    assert action == "feedback_captcha_ok"


def test_captcha_question_is_solvable_and_single_operand_pair():
    """题面必须是「a ± b = ?」这种一眼能算的，且答案不为负（减法不出现负数）。"""
    for _ in range(40):
        sess = bl._captcha_issue("probe")
        q = sess["question"]
        assert " + " in q or " - " in q, q
        a, op, b = q.split(" ")
        a, b = int(a), int(b)
        assert 2 <= a <= 19 and 2 <= b <= 19, q
        assert sess["answer"] == (a + b if op == "+" else a - b)
        assert sess["answer"] >= 0, q


# --------------------------------------------------------------------------
# C. 后台回复
# --------------------------------------------------------------------------
def _seed(content="希望支持倍速播放", uid=None):
    uid = uid or _uid()
    _pass_captcha(uid)
    _say(content, uid)
    return _last_id(), uid


def _reply(fid, text):
    """用一条独立会话调回复接口（与线上一样：路由自己拿 Depends(get_db)）。"""
    with SessionLocal() as db:
        return asyncio.run(reply_feedback(fid, FeedbackReplyInput(reply_text=text), db=db))


def test_reply_saves_then_notifies_user(monkeypatch):
    """回复落库 + 回执私聊；回执里必须能看出「回的是哪条留言」。"""
    fid, uid = _seed("希望支持倍速播放")
    sent = {}

    async def fake_notify(db, tg_user_id, text):
        sent["uid"], sent["text"] = str(tg_user_id), text
        return True

    monkeypatch.setattr(bl, "notify_user", fake_notify)
    out = _reply(fid, "已排入下个版本")

    assert out["ok"] is True and out["notified"] is True
    assert sent["uid"] == uid
    assert "希望支持倍速播放" in sent["text"]      # 引用用户原话，用户知道回的是哪条
    assert "已排入下个版本" in sent["text"]

    with SessionLocal() as db:
        row = db.get(UserFeedback, fid)
        assert row.status == "replied"
        assert row.reply_text == "已排入下个版本"
        assert row.replied_at is not None


def test_reply_survives_notify_failure(monkeypatch):
    """反证：TG 发不出去（用户屏蔽了 Bot / Token 没配）也绝不回滚已写好的回复。

    这是求片审核回信的同一条纪律：先 commit 再 await 外部网络调用。
    """
    fid, _ = _seed()

    async def broken_notify(db, tg_user_id, text):
        return False

    monkeypatch.setattr(bl, "notify_user", broken_notify)
    out = _reply(fid, "收到了")
    assert out["ok"] is True and out["notified"] is False
    with SessionLocal() as db:
        row = db.get(UserFeedback, fid)
        assert row.status == "replied" and row.reply_text == "收到了"


def test_reply_can_be_rewritten_without_adding_history_rows():
    """再回复一次就覆盖上一条回复（同一条留言只保留最新回复，不追加历史行）。"""
    fid, _ = _seed()
    _reply(fid, "第一次回复")
    _reply(fid, "第二次回复")
    with SessionLocal() as db:
        row = db.get(UserFeedback, fid)
        assert row.reply_text == "第二次回复"
    assert len(_rows()) == 1


def test_reply_rejects_blank_text_and_missing_row():
    fid, _ = _seed()
    with pytest.raises(Exception) as e1:
        _reply(fid, "   ")
    assert getattr(e1.value, "status_code", None) == 400

    with pytest.raises(Exception) as e2:
        _reply(999999, "x")
    assert getattr(e2.value, "status_code", None) == 404


def test_delete_removes_row_and_writes_audit():
    """删除是管理侧清垃圾：行删掉，但审计必须留痕。"""
    fid, _ = _seed()
    with SessionLocal() as db:
        delete_feedback(fid, db=db)
    assert [r.id for r in _rows()] == []
    with SessionLocal() as db:
        acts = list(db.scalars(select(AuditLog.action).where(
            AuditLog.target_type == "feedback", AuditLog.target_id == str(fid))).all())
    assert "feedback_deleted" in acts


def test_list_filters_by_status_and_reports_counts():
    a, _ = _seed("第一条")
    b, _ = _seed("第二条")
    _reply(a, "收到")

    with SessionLocal() as db:
        assert [x["id"] for x in feedback_list(status="pending", db=db)] == [b]
        assert [x["id"] for x in feedback_list(status="replied", db=db)] == [a]
        assert len(feedback_list(db=db)) == 2
        # 列表接口必须带上展示所需字段（原始状态 + 回复 + 时间）
        item = [x for x in feedback_list(db=db) if x["id"] == a][0]
        assert item["status"] == "replied" and item["reply_text"] == "收到"
        assert item["created_at"] is not None and item["replied_at"] is not None

        st = interaction_status(db=db)
        assert st["feedback_total"] == 2
        assert st["feedback_pending"] == 1


# --------------------------------------------------------------------------
# D. 指令入口 / 键盘排版 / 页面口径
# --------------------------------------------------------------------------
def test_five_buttons_in_three_plus_two_layout():
    """Reply 键盘 5 个按钮，**3 + 2 两行**（4+1 会留一个孤零零占满整行的按钮）。"""
    kb = bl.main_reply_keyboard()
    rows = kb["keyboard"]
    assert [len(r) for r in rows] == [3, 2], rows
    flat = [b for r in rows for b in r]
    assert flat == ["求片", "投稿", "搜索", "打赏", "建议/反馈"]
    assert kb["resize_keyboard"] is True and kb["one_time_keyboard"] is False


def test_inline_menu_also_has_feedback_button():
    kb = bl.main_keyboard()
    datas = [b.get("callback_data") for r in kb["inline_keyboard"] for b in r]
    assert bl.CB_FEEDBACK in datas
    assert len(datas) == 6                    # 原 5 个 + 建议反馈，别把旧的挤掉
    assert bl.CB_DONATE in datas and bl.CB_SEARCH in datas


def test_feedback_command_and_all_three_texts_are_in_sync():
    """指令表 / 帮助 / 未识别提示三处必须同时提到「建议」，漏一处用户就找不到入口。"""
    cmds = {c["command"] for c in bl.BUILTIN_COMMANDS}
    assert "建议" in cmds and len(cmds) == 5
    entry = [c for c in bl.BUILTIN_COMMANDS if c["command"] == "建议"][0]
    for alias in ("反馈", "建议/反馈", "feedback"):
        assert alias in entry["aliases"]
    assert "/建议" in bl.HELP
    assert "/建议" in bl._UNRECOGNIZED_HINT
    # 别名三种写法都要能被点/打中
    for text in ("建议", "/建议", "反馈", "/反馈", "建议/反馈", "feedback", "/feedback"):
        assert bl._match_builtin(text.lower()) == "建议", text


def test_page_never_prints_raw_status_values():
    """页面只说「待回复 / 已回复」，不许把 pending/replied 原文摊给用户看。"""
    src = APP_VUE.read_text(encoding="utf-8")
    assert "用户留言板" in src
    # 状态映射函数必须两态齐全，且默认兜底不是英文原文
    assert "'待回复'" in src and "'已回复'" in src
    assert "function feedbackStatusLabel" in src and "function feedbackStatusTag" in src
    # 留言板三列不能把裸状态直接渲染
    assert "{{ s.row.status }}" not in src


def test_b_section_is_three_columns_and_collapses_on_narrow():
    """b 板块三列并排；≤1100px 统一降单列（手机不挤），且三张卡都在。"""
    src = APP_VUE.read_text(encoding="utf-8")
    assert 'class="three-col" style="margin-bottom:18px"' in src
    assert "用户留言板" in src and "用户投稿" in src and "用户求片" in src
    # 三列必须在那条统一的窄屏媒体查询里被降成单列
    # （文件里不止一条 ≤1100px 查询，要逐条找「同时含 .three-col 与单列」的那条）
    hits = [ln for ln in src.splitlines()
            if "@media(max-width:1100px)" in ln and ".three-col" in ln
            and "grid-template-columns:1fr" in ln]
    assert hits, "≤1100px 的媒体查询里没有把 .three-col 降成单列"
    # 文案从「固定四项」改成「固定五项」，别只改一半
    assert "固定五项" in src and "固定四项" not in src
