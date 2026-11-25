import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

# 测试环境隔离：在任何 app 模块导入前设置
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")
os.environ.setdefault("GUANGYA_SHARE_AUTHORIZATION", "fake-web-session")
os.environ.setdefault("GUANGYA_CLIENT_ID", "test-client")
# SQLite 的 WAL / SHM 旁文件必须与主库**一起**删除。
# 只删主库而留下孤立的 -wal / -shm，SQLite 会把旧 WAL 当成当前库的一部分，
# 报 `database disk image is malformed` 或 `disk I/O error`——
# 表现为大批量 ERROR + failed，且**单跑某个文件时完全正常**（因为那时库是干净的），
# 极易被误判成代码回归。v2.1.6 发版前实测踩过：全量 473 errors / 81 failed，
# 而在 v2.1.5 基线 worktree 上跑同样命令，失败模式与数量几乎完全一致，
# 证明是**测试环境缺陷**而非产品代码回归。
#
# 另外：**库文件名带进程号**，避免两个 pytest 进程并发时互相删库 ——
# 本仓库的测试共享同一个 DATABASE_URL，历史上并发跑（后台全量 + 前台单文件）
# 会让后启动的进程在导入期删掉前一个正在用的库，前一个随即报 disk I/O error。
# 进程级唯一名彻底消除这类竞态，也让并发跑全量成为安全操作。
_db_path = os.path.join(tempfile.gettempdir(), f"guangya_pytest_{os.getpid()}.db")
for _suffix in ("", "-wal", "-shm", "-journal"):
    _side = _db_path + _suffix
    if os.path.exists(_side):
        try:
            os.remove(_side)
        except OSError:
            # 旁文件被其他进程占用时不强删：删不掉主库也照样能建新库。
            pass
os.environ.setdefault("DATABASE_URL", f"sqlite:///{_db_path}")


import pytest


@pytest.fixture
def forecast_at_noon(monkeypatch):
    """v2.1.51：把「排期预测看到的现在」冻结到今天上海正午。

    为什么需要：`forecast_schedule` 的 `hours` 是「今天**剩余**时段」。
    深夜（≥23:00）跑全量时剩下只有当前那一个小时，于是
      · 按「未来整点」取行的断言（test_forecast_quota_is_in_works）取不到 → 假红；
      · 断言「两类分类都出现」的用例（test_forecast_reflects_even_plan）只剩 1 类 → 假红。
    本机 2026-11-25 23:02 发版全量撞上这两条，22:47 同一套代码全绿 —— 纯时间性。

    做法：数据仍用真实 `utcnow()` 造（都落在「今天」），只把预测侧读的时钟钉到正午。
    生产代码不依赖此 fixture。
    """
    from app import share_center_v2 as sc2
    real = sc2.utcnow()
    noon_sh = real.astimezone(sc2.shanghai_tz()).replace(
        hour=12, minute=0, second=0, microsecond=0)
    fixed = noon_sh.astimezone(timezone.utc)
    monkeypatch.setattr(sc2, "utcnow", lambda: fixed)
    return fixed


def ensure_full_day_hour_window(db, version_id: int) -> None:
    """把当前小时纳入可调度窗口，使 plan_engine 相关测试不依赖真实墙钟小时。

    背景：默认小时窗口模板是 8-24，落在窗口外时 plan_engine 整轮空转
    （这是**生产行为**：夜间不排期）。若测试直接断言「生成了任务/清单」，
    就会在 8-24 之外（例如 CI 的 UTC 夜间）随机失败——v2.1.1 全量跑时
    在上海时间 00:02 复现了 3 个 plan_engine 假红。

    做法：额外插入一条 0-24 的整日窗口（category_key 留空 = 不绑定分类，
    不影响分类轮转），让「当前小时是否允许排期」恒为真。
    生产代码不依赖此 helper。
    """
    from sqlalchemy import select
    from app.models import SCHourWindow
    exists = db.scalar(select(SCHourWindow.id).where(
        SCHourWindow.plan_version_id == version_id,
        SCHourWindow.start_hour == 0, SCHourWindow.end_hour == 24))
    if exists is None:
        db.add(SCHourWindow(plan_version_id=version_id, start_hour=0, end_hour=24,
                            category_key="", quota=0, enabled=True))
        db.commit()


def freeze_at_period_end(sc2):
    """返回「当前排期时段的最后一分钟」的 UTC 时刻，供 monkeypatch 冻结时钟。

    v2.1.23 起推送节奏从「每小时均分」升级为「每个 `HOUR_SEGMENT_HOURS` 小时
    时段均分」：间隔 = 时段分钟 ÷ 部数，到此刻应放行 = floor(已过分钟/间隔)+1。
    整点刚过时 due 只有 1，多任务用例会被节流成 1 条而假红。

    冻结到**时段末尾**，due 就等于本时段全部部数 —— 这些用例验证的是链路本身
    （扫描→建分享→推送、去重、强制推送），不该被分钟级节奏干扰。
    节流本身由 `test_v2119_paced_forecast` / `test_v2123_pacing_and_layout` 专门守。
    """
    from datetime import timedelta, timezone
    sh = sc2.utcnow().astimezone(timezone(timedelta(hours=8)))
    seg = int(getattr(sc2, "HOUR_SEGMENT_HOURS", 2) or 2)
    last_hour = sh.hour - sh.hour % seg + seg - 1
    sh = sh.replace(hour=last_hour, minute=59, second=0, microsecond=0)
    return sh.astimezone(timezone.utc)


@pytest.fixture(autouse=True)
def _clear_module_caches():
    """阶段二：识别缓存/内存态是模块级的，必须每用例清理，避免跨用例串扰假失败。

    与 bot_listener._poll_state / share_center._EVENT_FAIL_COUNT 的清理同理：
    任何新增的模块级内存态都要在这里登记清理，否则 patch 掉 tmdb.identify 的测试
    会命中上一个用例缓存的成功结果（v085 两个用例曾因此假失败）。
    """
    try:
        from app import main as _main
        _main._IDENTIFY_CACHE.clear()
    except Exception:
        pass
    # v2.1.32：建议/反馈的算术验证码会话也是模块级内存态。它比缓存更"粘"——
    # 上一个用例里已验证通过的 uid 会让下一个用例跳过验证码直接落库而假绿。
    try:
        from app import bot_listener as _bl
        _bl._captcha_sessions.clear()
    except Exception:
        pass
    yield
    try:
        from app import main as _main
        _main._IDENTIFY_CACHE.clear()
    except Exception:
        pass
    try:
        from app import bot_listener as _bl
        _bl._captcha_sessions.clear()
    except Exception:
        pass
