"""v2.1.42 验收：文件自动重试 3 次即停 + 周/月限额下线（只留日度）。

用户口径（2026-11-25 真机反馈后拍板）：
  ① 「失败文件改成重复 3 次则不再自动处理，由人工介入处理」；
  ② 「周、月限量功能砍掉，只保留日度限额」。

覆盖：
  - 自动尝试次数累加、达上限后原因里写清「已停止自动重试，需人工处理」
  - 达上限的行不再进自动重试池（`retry_rows` 排掉）
  - 达上限的行不再让所属作品每轮无条件排进队列（`_works_with_pending_files`）
  - `pending` / `copying` / `unknown` 不受影响（限流打回的不吃额度）
  - 人工「批量重试选中」清零重算（重新给满额度）
  - 诊断接口带 attempts / auto_stopped，且计数与明细同源
  - 周 / 月限额：配了也不再拦截，默认值与前端输入框都已撤掉
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

MIRROR_SRC = (BACKEND / "app" / "mirror.py").read_text(encoding="utf-8")
MAIN_SRC = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
SC2_SRC = (BACKEND / "app" / "share_center_v2.py").read_text(encoding="utf-8")
APP_VUE = (Path(__file__).resolve().parents[1]
           / "frontend/src/App.vue").read_text(encoding="utf-8")


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v2142_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'b.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng, d
    shutil.rmtree(d, ignore_errors=True)


def _rule(db, name="m42"):
    from app.models import MirrorRule
    r = MirrorRule(name=name, source_root_id="A-ROOT", target_root_id="B-ROOT",
                   interval_minutes=60, enabled=True, file_idle_seconds=0.5)
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


def _media(db, rule, wk):
    from app.models import MirrorMedia, MirrorFile
    m = MirrorMedia(rule_id=rule.id, work_key="work:" + wk, title_standard=wk,
                    source_dir_id="A-" + wk, target_dir_id="B-" + wk, status="synced")
    db.add(m)
    db.flush()
    return m


def _row(db, rule, media, fid, *, status="failed", attempts=0):
    from app.models import MirrorFile
    f = MirrorFile(media_id=media.id, rule_id=rule.id, identity=f"name:{fid}",
                   source_file_id=fid, target_file_id="", file_name=f"{fid}.mkv",
                   fingerprint="100:1", size=100, status=status, attempts=attempts)
    db.add(f)
    db.commit()
    db.refresh(f)
    return f


# ===========================================================================
# 一、自动尝试次数与「三次即停」
# ===========================================================================

def test_max_file_attempts_comes_from_runtime_config(env):
    """次数上限走运行时配置（默认 3），不写死在代码里。"""
    from app import mirror
    from app.runtime_config import DEFAULTS
    S, _eng, _d = env
    db = S()
    assert DEFAULTS["mirror_max_file_attempts"][1] == "3"
    assert mirror.max_file_attempts(db) == 3
    # 非法值 / 0 / 负数一律回落成「至少 1 次」，绝不让文件彻底没机会
    from app import runtime_config
    from app.models import AppConfig, utcnow
    db.add(AppConfig(key="mirror_max_file_attempts", value_enc="0", updated_at=utcnow()))
    db.commit()
    assert mirror.max_file_attempts(db) >= 1
    _ = runtime_config


def test_mark_file_failed_counts_up_and_notes_manual_at_limit(env):
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    media = _media(db, rule, "剧甲")
    row = _row(db, rule, media, "F1", status="pending", attempts=0)

    mirror.mark_file_failed(row, "B 库拒收", 3)
    assert row.attempts == 1 and row.status == "failed"
    assert "已停止自动重试" not in (row.error or ""), "没到上限不该说停止"

    mirror.mark_file_failed(row, "B 库拒收", 3)
    assert row.attempts == 2
    assert "已停止自动重试" not in (row.error or "")

    mirror.mark_file_failed(row, "B 库拒收", 3)
    assert row.attempts == 3
    assert "已停止自动重试" in (row.error or ""), "第 3 次失败必须明确告知停止自动处理"
    assert "批量重试选中" in (row.error or ""), "要告诉用户下一步点哪里"


def test_exhausted_failed_row_is_out_of_auto_retry(env):
    """试满上限的失败行不再进自动重试池；没试满的照旧。"""
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    media = _media(db, rule, "剧甲")
    done = _row(db, rule, media, "F1", attempts=3)
    fresh = _row(db, rule, media, "F2", attempts=1)

    assert mirror.file_attempts_exhausted(done, 3) is True
    assert mirror.file_attempts_exhausted(fresh, 3) is False


def test_works_with_pending_files_ignores_exhausted(env):
    """核心回归：只有「试满上限的失败行」的作品，不再被每轮无条件排进队列。"""
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    stuck = _media(db, rule, "永久失败剧")
    _row(db, rule, stuck, "F1", attempts=3)
    live = _media(db, rule, "还能救的剧")
    _row(db, rule, live, "F2", attempts=1)

    got = mirror._works_with_pending_files(db, rule.id)
    assert "work:还能救的剧" in got
    assert "work:永久失败剧" not in got, "试满 3 次的失败行不该再拖作品进自动队列"


def test_pending_never_eats_attempt_budget(env):
    """限流打回的 pending / copying / unknown 不算失败，不吃这 3 次额度。"""
    from app import mirror
    S, _eng, _d = env
    db = S()
    rule = _rule(db)
    m = _media(db, rule, "限流剧")
    _row(db, rule, m, "F1", status="pending", attempts=99)
    _row(db, rule, m, "F2", status="copying", attempts=99)

    assert mirror.file_attempts_exhausted(
        type("R", (), {"status": "pending", "attempts": 99})(), 3) is False
    assert "work:限流剧" in mirror._works_with_pending_files(db, rule.id)


def test_auto_retry_pool_skips_exhausted_rows():
    """源码契约：`retry_rows` 构建时必须排掉已试满的行（否则每轮重发一次）。"""
    assert "and not file_attempts_exhausted(m, _max_att)" in MIRROR_SRC
    # 自动失败出口统一走 mark_file_failed（累加次数 + 达上限改人工）
    assert MIRROR_SRC.count("mark_file_failed(") >= 4


def test_manual_retry_resets_attempts():
    """人工介入：批量重试选中要清零已自动尝试次数，重新给满额度。"""
    start = MIRROR_SRC.index("async def retry_failed_files(")
    try:
        end = MIRROR_SRC.index("async def ", start + 10)
    except ValueError:      # 它是文件里最后一个 async 函数
        end = len(MIRROR_SRC)
    body = MIRROR_SRC[start:end]
    assert "r.attempts = 0" in body, "定向重试没有清零已尝试次数，人工点完等于没点"


def test_last_message_reports_auto_stopped():
    """同步结果里必须让人看到「有几项已停手、归人工」，不能只从数字里消失。"""
    assert "已停止自动重试 {_stopped} 项" in MIRROR_SRC
    assert 'stats["files_auto_stopped"] = _stopped' in MIRROR_SRC


# ===========================================================================
# 二、诊断接口与前端：计数与明细同源
# ===========================================================================

def test_diag_counts_and_detail_share_predicate():
    """计数 files_auto_stopped 与明细 auto_stopped 必须是同一个谓词。"""
    assert '"files_auto_stopped"' in MAIN_SRC
    assert "mirror.file_attempts_exhausted(f, _max_att)" in MAIN_SRC
    assert '"attempts": int(f.attempts or 0)' in MAIN_SRC


def test_frontend_shows_auto_stopped_column_and_hint():
    assert "已停止自动处理" in APP_VUE, "待重试面板没标出哪些已停止自动处理"
    assert "s.row.attempts" in APP_VUE, "面板没显示已自动试了几次"
    assert "files_auto_stopped" in APP_VUE, "面板没显示已停止自动重试的条数"
    assert "重新给满 3 次机会" in APP_VUE, "没有告诉用户人工重试会重新给额度"


# ===========================================================================
# 三、周 / 月限额下线，只留日度
# ===========================================================================

def test_cycle_plan_defaults_no_week_month_quota():
    from app.share_center_v2 import CYCLE_PLAN_DEFAULTS
    assert "week_max" not in CYCLE_PLAN_DEFAULTS, "周限额默认值应已下线"
    assert "month_max" not in CYCLE_PLAN_DEFAULTS, "月限额默认值应已下线"
    assert "day_max" in CYCLE_PLAN_DEFAULTS, "日度限额必须保留"
    # v2.1.45（用户口径）：周/月休止窗口也整体下线 —— 契约从「保留」翻转为「必须没有」，
    # 因为页面上那两个子页已经撤掉，留着后端开关＝留一条看不见的拦截规则。
    assert "week_rest_enabled" not in CYCLE_PLAN_DEFAULTS
    assert "month_rest_enabled" not in CYCLE_PLAN_DEFAULTS
    assert "week_rest_hour" not in CYCLE_PLAN_DEFAULTS


def test_quota_gate_only_day_layer():
    start = SC2_SRC.index("def _cycle_quota_gate(")
    end = SC2_SRC.index("def _prune_empty_push_lists(", start)
    body = SC2_SRC[start:end]
    assert 'checks = (("day_max", "day"),)' in body
    # 注释里可以提到「已下线」，但**代码不能再读这两个键**
    assert '("week_max"' not in body and '("month_max"' not in body
    assert 'cycle.get("week_max")' not in body
    assert 'cycle.get("month_max")' not in body
    # v2.1.45：周 / 月的**休止窗口**判定也删干净了（不只是限额）。
    assert 'return True, "weekly_rest"' not in body
    assert 'return True, "monthly_rest"' not in body
    assert 'cycle.get("week_rest_enabled")' not in body
    assert 'cycle.get("month_rest_enabled")' not in body
    # 统计窗口只剩「一天」——周窗口的 timedelta(days=7) 必须彻底消失，
    # 否则哪怕 checks 只剩日度，也留着一个能按周统计的死代码。
    assert "end = start + timedelta(days=1)" in body
    assert "timedelta(days=7)" not in body


def test_save_cycle_plan_does_not_clamp_week_month():
    """保存时不再夹逼周/月键——配了也不生效，别让它看起来还管用。"""
    start = SC2_SRC.index("def api_save_cycle_plan(")
    end = SC2_SRC.index("def ", start + 10)
    body = SC2_SRC[start:end]
    assert '"day_max")' in body
    assert "week_max" not in body and "month_max" not in body
    # v2.1.45：连周/月休止窗口的数值夹逼也一起撤掉
    assert "week_rest_hour" not in body


def test_forecast_reason_map_has_no_week_month():
    assert '"week_quota": "本周推送部数已达发布规则上限"' not in SC2_SRC
    assert '"month_quota": "本月推送部数已达发布规则上限"' not in SC2_SRC
    assert '"day_quota": "今日推送部数已达发布规则上限"' in SC2_SRC
    # v2.1.45：休止窗口的原因码也一起删了，别留一条用户看不懂的拦截理由
    assert '"weekly_rest":' not in SC2_SRC
    assert '"monthly_rest":' not in SC2_SRC


def test_frontend_has_no_week_month_quota_inputs():
    assert "scCyclePlan.week_max" not in APP_VUE, "周限额输入框应已撤掉"
    assert "scCyclePlan.month_max" not in APP_VUE, "月限额输入框应已撤掉"
    assert "scCyclePlan.day_max" in APP_VUE, "日度限额输入框必须保留"
    # v2.1.45：周度 / 月度两个子页整体撤掉，页面里不该再出现这两个档位名
    assert "'小时','日度','周度','月度'" not in APP_VUE, "周期计划子页应只剩小时/日度"
    assert "scCyclePlan.week_rest_enabled" not in APP_VUE
    assert "scCyclePlan.month_rest_enabled" not in APP_VUE
