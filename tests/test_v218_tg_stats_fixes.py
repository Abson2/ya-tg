"""v2.1.8 #241 补强：阅读量报表口径修正 + 定时采集。

v2.1.7 把阅读量功能整体交付了，但**趋势这条主线当时是断的**——
本轮发现并修掉 4 个真实缺陷，并补上定时采集。

必须固化的点：

1. **按天分桶必须按各行自己的快照时间**（真实缺陷）：
   原实现用「今天」给所有快照归桶 → 一次查询覆盖多天窗口时，
   消息数维度全部堆在最后一根柱子上，而增量维度却分散在各天，
   两条口径对不上，折线图读不出真实趋势。
2. **推送目标的消息数必须按去重后的消息计**（真实缺陷）：
   原实现遍历快照行累加 → 同一条消息每轮采集再落一份快照，
   消息数会随采集轮数**线性虚高**。
3. **最近采集明细每条消息只出现一次**（真实缺陷）：
   docstring 声称「每条消息取最新一条快照」，实现却是直接 limit，
   同一条消息多轮采集会被列出多次。
4. **定时采集是趋势的前提**（真实缺口）：
   阅读量是累计值，趋势靠相邻快照求差。只靠手动点按钮，
   趋势图永远只有一个数据点（首次采集无前值 → 不计增量），
   「每日新增阅读」这类口径根本无法成立。

另有两条老规矩在本次一并守住：
- **读不到就如实标记，绝不填 0**；
- **群聊不暴露浏览量**，聚合键用 (chat_id, kind) 复合键分列。
"""
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine

BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.environ.setdefault("APP_SECRET", "t" * 40)
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = ROOT / "frontend" / "src" / "App.vue"

SH = timezone(timedelta(hours=8))
UTC = timezone.utc


@pytest.fixture
def env():
    from app.db import Base
    from app import models  # noqa: F401
    d = tempfile.mkdtemp(prefix="v218stats_", dir="/tmp")
    eng = create_engine(f"sqlite:///{os.path.join(d, 'stats.db')}",
                        connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=eng)
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=eng, autoflush=False)
    yield S, eng
    shutil.rmtree(d, ignore_errors=True)


def _snapshot(db, *, queue_id=1, chat_id="-1001", kind="channel",
              message_id="555", views=100, forwards=2, reactions=3,
              available=True, note="", work_key="work:测试作品",
              collected_at=None):
    """直接造一条快照行（绕过采集流程，专注报表口径）。"""
    from app.models import TelegramMessageStat
    row = TelegramMessageStat(
        queue_id=queue_id,
        chat_id=chat_id,
        target_kind=kind,
        target_name="订阅频道",
        message_id=message_id,
        views=views,
        forwards=forwards,
        reactions=reactions,
        available=available,
        note=note,
        work_key=work_key,
    )
    row.collected_at = collected_at or datetime.now(UTC)
    db.add(row)
    db.commit()
    return row


# ---------------------------------------------------------------------------
# 1. 按天分桶：必须按各行自己的快照时间（v2.1.7 的真实缺陷）
# ---------------------------------------------------------------------------

def test_daily_bucket_uses_each_snapshot_date_not_today(env):
    """跨天的快照必须分到各自的日期，不能全堆在今天。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    now = datetime.now(UTC)
    _snapshot(db, queue_id=1, message_id="m1", collected_at=now - timedelta(days=3))
    _snapshot(db, queue_id=2, message_id="m2", collected_at=now - timedelta(days=2))
    _snapshot(db, queue_id=3, message_id="m3", collected_at=now - timedelta(days=1))

    report = tg_stats_service.trend_report(db, days=7)
    days = [b["day"] for b in report["buckets"]]
    assert len(days) == 3, f"跨 3 天的快照应分到 3 个桶，实际 {days}"
    assert len(set(days)) == 3, f"三个桶的日期不应重复，实际 {days}"
    # 每桶各 1 条消息
    assert all(b["messages"] == 1 for b in report["buckets"]), report["buckets"]


def test_daily_bucket_messages_match_snapshot_count_per_day(env):
    """每天的消息数应等于当天实际快照条数。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    now = datetime.now(UTC)
    day1 = now - timedelta(days=2)
    _snapshot(db, queue_id=1, message_id="m1", collected_at=day1)
    _snapshot(db, queue_id=2, message_id="m2", collected_at=day1)
    _snapshot(db, queue_id=3, message_id="m3", collected_at=now - timedelta(days=1))

    report = tg_stats_service.trend_report(db, days=7)
    total = sum(b["messages"] for b in report["buckets"])
    assert total == 3, f"消息数合计应等于快照总数，实际 {total}"


# ---------------------------------------------------------------------------
# 2. 推送目标消息数：按去重后的消息计，不按快照条数（v2.1.7 的真实缺陷）
# ---------------------------------------------------------------------------

def test_target_messages_count_deduped_not_snapshot_rows(env):
    """同一条消息采集 3 轮，目标的消息数仍应是 1 而不是 3。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    now = datetime.now(UTC)
    _snapshot(db, queue_id=1, message_id="m1", views=100, collected_at=now - timedelta(hours=3))
    _snapshot(db, queue_id=1, message_id="m1", views=150, collected_at=now - timedelta(hours=2))
    _snapshot(db, queue_id=1, message_id="m1", views=180, collected_at=now - timedelta(hours=1))

    report = tg_stats_service.trend_report(db, days=7)
    assert len(report["targets"]) == 1
    target = report["targets"][0]
    assert target["messages"] == 1, f"消息数应按去重后的消息计，实际 {target['messages']}"
    # 累计值取最新快照
    assert target["views_total"] == 180


def test_target_messages_count_distinct_messages(env):
    """两条不同消息 → 消息数 2（去重逻辑不能把不同消息也合并掉）。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    now = datetime.now(UTC)
    _snapshot(db, queue_id=1, message_id="m1", collected_at=now - timedelta(hours=2))
    _snapshot(db, queue_id=2, message_id="m2", collected_at=now - timedelta(hours=1))

    report = tg_stats_service.trend_report(db, days=7)
    assert report["targets"][0]["messages"] == 2


def test_same_chat_id_channel_and_group_stay_separate(env):
    """同一个 id 既登记为频道又登记为群时，必须分列（口径不混算）。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    now = datetime.now(UTC)
    _snapshot(db, queue_id=1, chat_id="-1001", kind="channel",
              message_id="m1", views=500, collected_at=now - timedelta(hours=2))
    _snapshot(db, queue_id=2, chat_id="-1001", kind="group",
              message_id="m2", views=0, collected_at=now - timedelta(hours=1))

    report = tg_stats_service.trend_report(db, days=7)
    kinds = {t["kind"] for t in report["targets"]}
    assert kinds == {"channel", "group"}, f"频道与群必须分列，实际 {kinds}"
    ch = next(t for t in report["targets"] if t["kind"] == "channel")
    gr = next(t for t in report["targets"] if t["kind"] == "group")
    assert ch["views_available"] is True
    assert gr["views_available"] is False
    assert ch["views_total"] == 500
    assert gr["views_total"] == 0


# ---------------------------------------------------------------------------
# 3. 最近采集明细：每条消息只出现一次（v2.1.7 的真实缺陷）
# ---------------------------------------------------------------------------

def test_recent_items_dedupe_same_message(env):
    """同一条消息采集多轮，明细里只出现一次（取最新快照）。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    now = datetime.now(UTC)
    _snapshot(db, queue_id=1, message_id="m1", views=100, collected_at=now - timedelta(hours=3))
    _snapshot(db, queue_id=1, message_id="m1", views=150, collected_at=now - timedelta(hours=2))
    _snapshot(db, queue_id=1, message_id="m1", views=180, collected_at=now - timedelta(hours=1))

    items = tg_stats_service.recent_items(db, limit=50)
    assert len(items) == 1, f"同一条消息只应出现一次，实际 {len(items)} 次"
    assert items[0]["views"] == 180, "应保留最新快照的数值"


def test_recent_items_keeps_distinct_messages(env):
    """不同消息照常全部列出。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    now = datetime.now(UTC)
    _snapshot(db, queue_id=1, message_id="m1", collected_at=now - timedelta(hours=2))
    _snapshot(db, queue_id=2, message_id="m2", collected_at=now - timedelta(hours=1))

    items = tg_stats_service.recent_items(db, limit=50)
    assert len(items) == 2


def test_recent_items_respects_limit_after_dedup(env):
    """去重之后仍要受 limit 约束（不能因为去重前条数多而超量）。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    now = datetime.now(UTC)
    for i in range(5):
        for k in range(3):
            _snapshot(db, queue_id=i + 1, message_id=f"m{i}",
                      collected_at=now - timedelta(hours=2) + timedelta(minutes=k))

    items = tg_stats_service.recent_items(db, limit=2)
    assert len(items) == 2


# ---------------------------------------------------------------------------
# 4. 定时采集：趋势的前提（v2.1.7 的真实缺口）
# ---------------------------------------------------------------------------

def test_interval_config_key_registered(env):
    """间隔配置键必须注册进 runtime_config（未注册会被静默丢弃）。"""
    S, eng = env
    from app import runtime_config
    assert "tg_stats_collect_interval_hours" in runtime_config.DEFAULTS


def test_due_for_collect_true_when_never_collected(env):
    """从未采集过 → 立即到点。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    assert tg_stats_service.due_for_collect(db, 1) is True


def test_due_for_collect_false_when_interval_not_elapsed(env):
    """刚采集过且间隔未到 → 不重复采集。"""
    S, eng = env
    from app import tg_stats_service
    from app.models import TelegramStatRun
    db = S()
    run = TelegramStatRun(status="success", lookback_hours=72)
    db.add(run)
    db.commit()

    assert tg_stats_service.due_for_collect(db, 6) is False


def test_due_for_collect_true_when_interval_elapsed(env):
    """间隔已到 → 到点。"""
    S, eng = env
    from app import tg_stats_service
    from app.models import TelegramStatRun
    db = S()
    run = TelegramStatRun(status="success", lookback_hours=72)
    db.add(run)
    db.commit()
    # 把开始时间往前拨 3 小时
    run.started_at = datetime.now(UTC) - timedelta(hours=3)
    db.commit()

    assert tg_stats_service.due_for_collect(db, 1) is True


def test_due_for_collect_false_when_interval_zero(env):
    """间隔为 0 → 关闭定时采集（仅手动）。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    assert tg_stats_service.due_for_collect(db, 0) is False


def test_failed_run_counts_as_attempted(env):
    """失败的采集也算「试过了」——否则失败间隔会一直重试把接口打满。"""
    S, eng = env
    from app import tg_stats_service
    from app.models import TelegramStatRun
    db = S()
    run = TelegramStatRun(status="failed", lookback_hours=72, error="网络异常")
    db.add(run)
    db.commit()

    assert tg_stats_service.due_for_collect(db, 6) is False


def test_status_reports_interval_hours(env):
    """状态接口要报出间隔（前端据此显示采集方式）。"""
    S, eng = env
    from app import tg_stats, tg_stats_service
    db = S()
    tg_stats_service  # 确保模块可加载
    st = tg_stats.status(db)
    assert "interval_hours" in st
    assert st["interval_hours"] == 0


def test_readonly_status_has_no_disk_side_effect(env):
    """**只读状态查询不得有写盘副作用**。

    原实现里 `_session_dir()` 会顺手 `mkdir`，于是「看一眼是否已登录」
    就在服务器上凭空造出 `./data/tg_session` 目录，测试也会在仓库里留垃圾。
    现在目录只在真正登录 / 建客户端时才创建。

    用例不依赖 pytest 的 `tmp_path` fixture：沙箱下 pytest 会在系统临时根
    建 `pytest-of-<user>` 目录，与既有目录撞 EEXIST → PermissionError，
    用例直接 ERROR（与被测行为无关）。项目内已有同款约定，
    统一改用 `tempfile.mkdtemp(dir="/tmp")` 自建临时目录。
    """
    S, eng = env
    from app import tg_stats
    from app import runtime_config

    tmpdir = tempfile.mkdtemp(prefix="v218_readonly_", dir="/tmp")
    try:
        db = S()
        target = Path(tmpdir) / "tg_session_sideeffect"
        runtime_config.set_many(db, {tg_stats.CFG_SESSION_DIR: str(target)})
        db.commit()

        st = tg_stats.status(db)
        assert "logged_in" in st
        assert not target.exists(), (
            "只读状态查询创建了会话目录——目录创建必须延后到登录/建客户端时")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_session_dir_has_writable_path_in_container(env):
    """**容器可写性**：会话目录在容器里必须真的能被创建。

    真实缺陷：容器主进程经 `gosu yatg` 以 UID 10001 运行，而 `/app` 属主是
    root（Dockerfile 只 chown 了 /app/db 与 /app/logs）。运行期
    `mkdir /app/data/tg_session` 会 PermissionError，而异常被
    `except Exception: pass` 吞掉 → 目录建不出来 → **登录功能静默失败**
    （页面一直显示「未就绪」，没有任何错误提示）。

    本条守住三个部署层面的事实：
      ① Dockerfile 建了 /app/data 并 chown 给 yatg；
      ② entrypoint 启动时确保目录存在且属主正确；
      ③ compose 把它挂出来（否则容器重建就要重新登录）。
    """
    root = ROOT

    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    assert "/app/data" in dockerfile, "Dockerfile 未创建 /app/data（会话目录无处安放）"
    # chown 必须把 data 一起带上
    chown_line = ""
    for line in dockerfile.splitlines():
        if "chown" in line and "yatg" in line:
            chown_line = line
            break
    assert chown_line, "Dockerfile 未对运行用户做 chown"
    assert "/app/data" in chown_line, (
        f"chown 未覆盖 /app/data，运行期建目录会 PermissionError：{chown_line}")

    entry = (root / "scripts" / "docker-entrypoint.sh").read_text(encoding="utf-8")
    assert "/app/data" in entry, "entrypoint 未处理会话目录"
    assert "mkdir -p /app/data" in entry, "entrypoint 未确保会话目录存在"
    # chown 必须覆盖 /app/data。按**命令行**判定而不是 split("chown")[1]：
    # 注释里也出现 "chown" 字样（"先把这两个目录统一 chown 给 yatg"），
    # 按首次出现切分会取到注释片段 → 断言假红。
    chown_cmds = [ln.strip() for ln in entry.splitlines()
                  if "chown" in ln and ln.strip().startswith("chown")]
    assert chown_cmds, "entrypoint 未对运行用户做 chown"
    assert any("/app/data" in ln for ln in chown_cmds), (
        f"chown 未覆盖 /app/data，运行期建目录会 PermissionError：{chown_cmds}")

    for name in ("docker-compose.yml", "docker-compose.build.yml"):
        text = (root / name).read_text(encoding="utf-8")
        assert "./data:/app/data" in text, (
            f"{name} 未挂载会话目录——容器重建会丢失登录态")


def test_session_credential_files_never_committed(env):
    """**安全红线**：Telegram 会话凭据文件绝不能进仓库。

    `.session` 文件承载 MTProto 登录态，等同账号凭据。一旦被 `git add -A`
    （v1.5.1 踩过）或手工补加提交进公开仓库，账号就等于泄露了。
    本条守卫同时检查 `.gitignore` 规则与**当前索引**里有没有这类文件。
    """
    root = ROOT
    gi = (root / ".gitignore").read_text(encoding="utf-8")
    assert "*.session" in gi, ".gitignore 必须忽略 *.session（MTProto 登录凭据）"
    assert "data/" in gi, ".gitignore 必须忽略本地数据目录"

    # 当前 git 索引里不得有 session / 数据目录文件
    import subprocess
    try:
        out = subprocess.run(
            ["git", "-c", "core.quotepath=false", "ls-files"],
            cwd=str(root), capture_output=True, text=True, check=True).stdout
    except Exception:
        return  # 非 git 环境（如源码包解压）跳过
    bad = [f for f in out.splitlines()
           if f.endswith(".session") or f.startswith("data/")]
    assert not bad, f"仓库里混入了凭据/本地数据文件：{bad}"


def test_session_dir_has_no_disk_side_effect(env):
    """`_session_dir()` 只算路径不建目录；`_ensure_session_dir()` 才建。"""
    S, eng = env
    from app import tg_stats
    from app import runtime_config

    tmpdir = tempfile.mkdtemp(prefix="v218_helper_", dir="/tmp")
    try:
        db = S()
        target = Path(tmpdir) / "tg_session_helper"
        runtime_config.set_many(db, {tg_stats.CFG_SESSION_DIR: str(target)})
        db.commit()

        path = tg_stats._session_dir(db)
        assert path == target
        assert not target.exists(), "_session_dir 不该建目录"

        tg_stats._ensure_session_dir(db)
        assert target.exists(), "_ensure_session_dir 应该建目录"
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_job_registered_in_scheduler(env):
    """定时采集 job 必须注册进 APScheduler（否则配置了也不跑）。"""
    src = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    assert "_job_tg_stats_collect" in src, "缺少定时采集 job"
    assert 'id="tg_stats_collect"' in src, "定时采集 job 未注册"
    assert "due_for_collect" in src, "job 未按间隔判断是否到点"


def test_job_uses_independent_session_and_lock(env):
    """定时采集必须用独立 Session + 进程内锁（不能占用请求线程、不能并发重入）。

    v2.1.8 复查补强：会话必须在**线程内**创建（与 `_bot_poll_in_thread` /
    `_reap_stale_mirror_tasks_in_thread` 的既有做法一致）——Session 不是线程
    安全对象，跨线程复用同一实例是隐患。所以采集体抽成
    `_tg_stats_collect_in_thread()`，job 只负责加锁 + to_thread。
    """
    src = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    i = src.find("async def _job_tg_stats_collect")
    assert i > 0
    seg = src[i:i + 2200]
    assert "_tg_stats_lock" in seg, "缺少防重入锁"
    assert "asyncio.to_thread" in seg, "job 未把采集放进线程"

    j = src.find("def _tg_stats_collect_in_thread")
    assert j > 0, "缺少线程内的采集实现"
    hseg = src[j:j + 2200]
    assert "SessionLocal()" in hseg, "线程内必须自建独立会话"
    assert "collect_once_threaded" in hseg, "线程内未走线程隔离入口"
    assert "due_for_collect" in hseg, "线程内未按间隔判断是否到点"


def test_config_api_accepts_interval_hours(env):
    """配置接口接受间隔设置（走真实写入路径）。

    **为什么不套 TestClient**：`with TestClient(app)` 会触发 app 的 lifespan
    （migrate + 注册全部 APScheduler job + scheduler.start()），全量跑时它与
    其它用例共享的全局 `SessionLocal` / `scheduler` / `runtime_config` 互相干扰，
    导致本用例单独跑通过、全量跑失败。这里直接调函数级实现（v217 测试同款做法），
    断言的是**真正的业务行为**：间隔被写库、被读回、被夹逼后回显。
    """
    S, eng = env
    from app import tg_stats, tg_stats_api
    from app import runtime_config

    db = S()
    result = tg_stats_api.api_stats_config(
        body=tg_stats_api.Payload(payload={"interval_hours": 6, "enabled": True}),
        db=db,
    )
    assert result["ok"] is True
    # 写库成功且能读回
    assert runtime_config.get(db, tg_stats.CFG_COLLECT_INTERVAL_HOURS) == "6"
    # 状态接口读出同一个值
    assert tg_stats.status(db)["interval_hours"] == 6
    # 接口回显的间隔与库里的值一致
    assert result["interval_hours"] == 6
    # 开关同样生效
    assert tg_stats.status(db)["enabled"] is True


def test_config_api_clamps_interval_range(env):
    """间隔超出范围要被夹到合法区间（0-168 小时）。"""
    S, eng = env
    from app import tg_stats_api
    from app import tg_stats
    from app import runtime_config
    db = S()

    src = (BACKEND / "app" / "tg_stats_api.py").read_text(encoding="utf-8")
    assert "max(0, min(168," in src, "间隔未做范围夹逼"


# ---------------------------------------------------------------------------
# 4b. 采集不得在事件循环里同步写库（六轮死锁教训的红线）
#
# 采集过程有同步 db.commit()，而 SQLite 只有单写者 + busy_timeout=30000——
# 一旦碰上写锁竞争，这句同步 commit 会在事件循环线程里干等最长 30 秒，
# 整个进程的所有 HTTP 请求都被冻住。所以采集必须在工作线程里跑。
# ---------------------------------------------------------------------------

def test_threaded_wrapper_exists():
    """必须提供线程隔离的采集入口。"""
    from app import tg_stats_service
    assert hasattr(tg_stats_service, "collect_once_threaded"), "缺少线程隔离入口"


def test_threaded_wrapper_runs_in_own_event_loop(env):
    """线程隔离入口能在独立事件循环里跑完一轮采集。"""
    S, eng = env
    from app import tg_stats, tg_stats_service
    from app import runtime_config
    db = S()
    runtime_config.set_many(db, {tg_stats.CFG_ENABLED: "false"})
    db.commit()
    result = tg_stats_service.collect_once_threaded(db)
    assert result["reason"] == "disabled"


def test_threaded_wrapper_never_raises(env):
    """线程隔离入口永不抛异常（异常转成结构化结果）。"""
    S, eng = env
    from app import tg_stats_service
    db = S()
    result = tg_stats_service.collect_once_threaded(db)
    assert isinstance(result, dict)
    assert "ok" in result


def test_job_does_not_call_collect_once_directly(env):
    """后台 job 不得直接在事件循环里调 collect_once（必须走线程隔离入口）。"""
    src = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    i = src.find("async def _job_tg_stats_collect")
    assert i > 0
    seg = src[i:i + 2600]
    assert "collect_once_threaded" in seg or "_tg_stats_collect_in_thread" in seg, (
        "job 未走线程隔离路径")
    assert "await tg_stats_service.collect_once(" not in seg, "job 直接调了 collect_once"


def test_job_uses_to_thread(env):
    """后台 job 必须用 to_thread 隔离同步写库。"""
    src = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    i = src.find("async def _job_tg_stats_collect")
    seg = src[i:i + 2600]
    assert "asyncio.to_thread" in seg, "job 未用 to_thread"


def test_job_does_not_create_session_on_event_loop_thread(env):
    """**红线**：job 自身**不得**在事件循环线程里建会话。

    SQLAlchemy Session 不是线程安全对象。历史写法是在 async job 里
    `with SessionLocal() as db:` 建会话、再把这个会话丢给工作线程用——
    等于跨线程复用同一 Session，是隐患（也与项目里
    `_bot_poll_in_thread` / `_reap_stale_mirror_tasks_in_thread`
    「会话在线程内创建」的既有做法不一致）。

    现在的结构是：
        job（事件循环线程）→ to_thread(_tg_stats_collect_in_thread)
                             → 线程内 with SessionLocal() as db
    所以 job 函数体内**不允许**出现 SessionLocal()。
    """
    src = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
    i = src.find("async def _job_tg_stats_collect")
    assert i > 0
    # job 函数体到下一个顶层 def / async def 之前
    j = src.find("\nasync def ", i + 10)
    seg = src[i:j if j > 0 else i + 2600]
    assert "SessionLocal()" not in seg, (
        "job 在事件循环线程里建了会话——会话必须在线程内创建"
        "（见 _tg_stats_collect_in_thread）")


def test_api_collect_uses_to_thread(env):
    """手动采集端点同样要线程隔离。"""
    src = (BACKEND / "app" / "tg_stats_api.py").read_text(encoding="utf-8")
    i = src.find("async def api_stats_collect")
    assert i > 0
    seg = src[i:i + 900]
    assert "to_thread" in seg, "手动采集端点未做线程隔离"
    assert "collect_once_threaded" in seg, "手动采集端点未走线程隔离入口"


def test_threaded_collect_does_not_block_event_loop(env):
    """**实测**：采集期间的同步写库不会冻结事件循环。

    做法：把 Session.commit 换成慢版（模拟 SQLite 写锁等待），
    然后统计采集窗口内事件循环的「心跳」个数——
    - 未隔离：窗口内 0 个心跳（循环被整个冻住）
    - 已隔离：窗口内约 20 个心跳（循环正常）
    这是本条红线唯一可靠的验证方式，光看代码结构说服力不够。
    """
    import asyncio
    import threading
    import time

    S, eng = env
    from app import tg_stats, tg_stats_service
    from app import runtime_config
    db = S()
    runtime_config.set_many(db, {tg_stats.CFG_ENABLED: "true"})
    db.commit()

    orig_commit = db.commit

    def slow_commit():
        time.sleep(0.5)          # 模拟 SQLite 写锁等待（busy_timeout 30s 的缩比版）
        orig_commit()

    db.commit = slow_commit

    # 记录采集实际在哪个线程执行——隔离的**充分条件**是它不在调用方线程上。
    seen_threads = []
    orig_collect = tg_stats_service.collect_once

    async def spy(*args, **kwargs):
        seen_threads.append(threading.current_thread().name)
        return await orig_collect(*args, **kwargs)

    async def _run():
        ticks = []
        stop = False

        async def ticker():
            while not stop:
                await asyncio.sleep(0.05)
                ticks.append(time.time())

        task = asyncio.create_task(ticker())
        await asyncio.sleep(0.2)      # 让 ticker 稳定
        base = len(ticks)
        t0 = time.time()
        await asyncio.to_thread(tg_stats_service.collect_once_threaded, db)
        elapsed = time.time() - t0
        n = len(ticks) - base
        stop = True
        await task
        return elapsed, n

    try:
        tg_stats_service.collect_once = spy
        elapsed, n = asyncio.run(_run())
    finally:
        tg_stats_service.collect_once = orig_collect

    # ① 采集必须跑在**另一个线程**上（否则同步写库就在事件循环线程里）。
    assert seen_threads, "未记录到采集执行线程"
    assert seen_threads[0] != threading.current_thread().name, (
        f"采集在调用方线程上执行（{seen_threads[0]}）——同步写库会冻结事件循环")

    # ② 无阻塞时心跳数应接近 elapsed/0.05；被冻住时接近 0。
    assert n >= 5, (
        f"采集期间事件循环只跳了 {n} 次心跳（耗时 {elapsed:.2f}s）——"
        f"同步写库仍在事件循环线程里，采集必须走线程隔离")


def test_real_job_does_not_block_event_loop(env, monkeypatch):
    """**端到端**：跑生产用的那个 job，事件循环不得被冻结。

    上一条测的是 service 层入口；而真正上线跑的是 `main._job_tg_stats_collect`。
    这里直接调它（把 SessionLocal 换成本测试库），统计采集窗口内的心跳数——
    只要 job 里少了一层 to_thread，心跳会直接掉到 0。

    v2.1.8 复查：job 现在是**两层隔离**——
      `to_thread(_tg_stats_collect_in_thread)` → 线程内自建 Session →
      `collect_once_threaded` 再起独立事件循环。
    因此 monkeypatch 的 SessionLocal 必须是**可多次调用**的工厂
    （每次返回同一个测试会话即可），否则线程内 `with SessionLocal()` 会拿不到库。
    """
    import asyncio
    import time

    S, eng = env
    from app import main as app_main
    from app import tg_stats
    from app import runtime_config

    db = S()
    runtime_config.set_many(db, {
        tg_stats.CFG_ENABLED: "true",
        tg_stats.CFG_COLLECT_INTERVAL_HOURS: "1",
    })
    db.commit()

    orig_commit = db.commit

    def slow_commit():
        time.sleep(0.4)          # 模拟写锁等待
        orig_commit()

    db.commit = slow_commit

    # 把 job 用的会话换成本测试库。
    # 注意：线程内用 `with SessionLocal() as db:`，退出时会 close 这个会话——
    # 所以断言必须在会话关闭前取到的数据（心跳数）上做，不依赖关闭后的 db。
    monkeypatch.setattr(app_main, "SessionLocal", lambda: db)
    # 让 job 认为「到点了」
    monkeypatch.setattr(app_main.tg_stats_service, "due_for_collect", lambda d, i: True)

    async def _run():
        ticks = []
        stop = False

        async def ticker():
            while not stop:
                await asyncio.sleep(0.05)
                ticks.append(time.time())

        task = asyncio.create_task(ticker())
        await asyncio.sleep(0.2)
        base = len(ticks)
        await app_main._job_tg_stats_collect()
        n = len(ticks) - base
        stop = True
        await task
        return n

    n = asyncio.run(_run())
    assert n >= 3, (
        f"定时采集 job 期间事件循环只跳了 {n} 次心跳——"
        f"job 里的同步写库还在事件循环线程里，会冻住整个进程")


# ---------------------------------------------------------------------------
# 5. 前端：间隔配置项
# ---------------------------------------------------------------------------

def test_frontend_has_interval_field():
    """页面要有「自动采集间隔」配置项。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "interval_hours" in app, "缺少间隔字段"
    assert "自动采集间隔" in app, "缺少间隔配置项标签"


def test_frontend_interval_bound_zero_and_max():
    """间隔取值范围 0-168（0 表示关闭）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert ':min="0" :max="168"' in app, "间隔取值范围不对"


def test_frontend_sends_interval_on_save():
    """保存配置时要带上间隔。"""
    app = APP_VUE.read_text(encoding="utf-8")
    i = app.find("async function saveScStatsConfig")
    assert i > 0
    seg = app[i:i + 900]
    assert "interval_hours" in seg, "保存时未提交间隔"


def test_frontend_shows_collect_mode():
    """页面要如实告诉用户当前的采集方式（自动 / 仅手动）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    assert "采集方式" in app, "缺少采集方式说明"
    assert "仅手动采集" in app, "缺少仅手动采集的说明"


def test_frontend_no_backend_field_names():
    """页面不得出现后端字段名（红线）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    i = app.find('class="sc-stats-layout"')
    assert i > 0
    seg = app[i:]
    for bad in ("tg_stats_enabled", "views_available", "queue_id",
                "target_kind", "work_key", "chat_id", "message_id"):
        assert bad not in seg, f"页面出现后端字段名：{bad}"


def test_frontend_stats_loader_stays_independent():
    """阅读量加载仍走独立 loader（不得混进发布中心主加载）。"""
    app = APP_VUE.read_text(encoding="utf-8")
    i = app.find("async function loadSc(")
    assert i > 0
    # loadSc 函数体结束前不应出现 stats 端点
    j = app.find("\n}", i)
    seg = app[i:j]
    assert "/sc/stats/" not in seg, "阅读量端点混进了发布中心主加载"
