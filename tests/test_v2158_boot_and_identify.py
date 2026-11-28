"""v2.1.58 回归测试：刷新卡「正在恢复会话」+ 识别完成却归「待识别」。

两个问题都是真机复现的，各自都有一份「反证」用例——把口径改回去必须变红。

一、刷新卡死（前端）
- 根因链：`api()` 默认 `timeoutMs = 0`（不设超时）→ `load()` 的
  `Promise.allSettled(23 个请求)` 只要有一个 pending 就永不落定 →
  `onMounted` 的 `await load()` 永不返回 → `authed` 停在 `null` → 永久骨架屏。
- 收口：① `api()` 默认 30 秒超时，超时报「请求超时」；② `load()` 末尾三个
  裸 await 加兜底；③ `onMounted` 加 `withTimeout` 总闸，且**只有会话失效
  （token 被 401 清掉）才回登录页**，超时/断网不再把有效会话踢成登录页。

二、识别状态（后端）
- 根因：作品级 `identify_status` 直接取首行，而首行是 `id desc` 的**最新入库那一集**，
  那一集恰好 failed → 整部剧进「待识别」（前端判据 `!== 'identified'`）。
- 收口：组内任一成员 identified 即作品级 identified，且不会被后续失败成员覆盖。
"""
from pathlib import Path

from app.db import SessionLocal, migrate
from app.main import library_series
from app.models import Resource

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


def _clean():
    db = SessionLocal()
    db.query(Resource).filter(Resource.file_id.like("v2158-%")).delete(synchronize_session=False)
    db.commit()
    db.close()


def _res(seq, name, **kw):
    return Resource(file_id=f"v2158-{seq}", name=name, ext=".mkv", size=1, utime=1, **kw)


def _seed_work(tag, statuses, **base):
    """按顺序入库，最后一条 id 最大 = 真机里的「首行」。"""
    db = SessionLocal()
    for i, st in enumerate(statuses, start=1):
        db.add(_res(f"{tag}-e{i}", f"Show.S01E{i:02d}.mkv", identify_status=st, **base))
    db.commit()
    db.close()


# ---------- 二、作品级识别状态 ----------
def test_work_identified_when_any_episode_identified():
    """反证：旧口径（取首行）下这里会是 failed。"""
    migrate(); _clean()
    _seed_work("a", ["identified", "identified", "failed"],
               media_title="谷雨", media_type="tv", tmdb_id=279155)
    db = SessionLocal()
    groups = library_series(q="", media_type="", db=db)
    db.close()
    g = next(x for x in groups if x.get("tmdb_id") == 279155)
    assert g["count"] == 3
    assert g["identified_count"] == 2 and g["unidentified_count"] == 1
    assert g["identify_status"] == "identified", g["identify_status"]
    _clean()


def test_all_failed_work_keeps_failure_label():
    """一集都没识别成功时，仍要显示失败原因，不能退化成空串。"""
    migrate(); _clean()
    _seed_work("b", ["failed", "failed"], media_title="黑冰", media_type="tv", tmdb_id=555001)
    db = SessionLocal()
    groups = library_series(q="", media_type="", db=db)
    db.close()
    g = next(x for x in groups if x.get("tmdb_id") == 555001)
    assert g["identify_status"] == "failed", g["identify_status"]
    assert g["identified_count"] == 0 and g["unidentified_count"] == 2
    _clean()


def test_unidentified_tab_predicate_now_excludes_partial_work():
    """前端「待识别」判据 `!== 'identified'`：部分识别的作品必须被排除。"""
    migrate(); _clean()
    _seed_work("c", ["identified", "failed"], media_title="大宅门", media_type="tv", tmdb_id=555002)
    db = SessionLocal()
    groups = library_series(q="", media_type="", db=db)
    db.close()
    g = next(x for x in groups if x.get("tmdb_id") == 555002)
    assert (g["identify_status"] != "identified") is False
    # 后端按 identify_status 筛选时用同一份判据（口径不许两套）
    db = SessionLocal()
    only_failed = library_series(q="", media_type="", identify_status="failed", db=db)
    db.close()
    assert all(x.get("tmdb_id") != 555002 for x in only_failed)
    _clean()


def test_identified_is_sticky_against_later_failure():
    """顺序反过来（先失败后成功）也必须落 identified——粘性判据，不靠行序。"""
    migrate(); _clean()
    _seed_work("d", ["failed", "identified", "failed"], media_title="浴血红颜",
               media_type="tv", tmdb_id=555003)
    db = SessionLocal()
    groups = library_series(q="", media_type="", db=db)
    db.close()
    g = next(x for x in groups if x.get("tmdb_id") == 555003)
    assert g["identify_status"] == "identified", g["identify_status"]
    _clean()


# ---------- 一、启动不再卡骨架屏 ----------
def test_api_has_default_timeout():
    assert "const API_TIMEOUT_MS = 30000" in APP
    assert "async function api(path, opt = {}, timeoutMs = API_TIMEOUT_MS)" in APP
    # 反证：默认值退回 0（不设超时）就是原缺陷
    assert "timeoutMs = 0)" not in APP


def test_api_reports_timeout_not_scan_message():
    assert "请求超时（${Math.round(_ms / 1000)} 秒未返回）" in APP
    # 旧的扫描专用文案不许再兜所有接口
    assert "throw Error('扫描仍在进行中，请稍后刷新资源库查看结果')" not in APP


def test_load_tail_awaits_are_guarded():
    assert "try { await loadAuth() } catch (e) {}" in APP
    assert "try { await loadTmdb() } catch (e) {}" in APP
    assert "try { await loadAi() } catch (e) {}" in APP
    # 反证：裸 await 回来就是「load() 会抛」
    assert "\n  await loadAuth()\n" not in APP


def test_boot_has_timeout_gate_and_settles():
    assert "const BOOT_TIMEOUT_MS = 12000" in APP
    assert "function withTimeout(p, ms, label = '加载')" in APP
    # v2.1.59：不再 await 总闸（那正是「刷新要等几秒」的根因），改为立即放行 +
    # 总闸只在后台收敛。总闸仍必须存在（防止 load 永不结束把后台任务吊死）。
    assert "withTimeout(loadP, BOOT_TIMEOUT_MS, '首次加载')" in APP
    assert "await withTimeout(loadP, BOOT_TIMEOUT_MS, '首次加载')" not in APP
    # 落定：有 token 必须先无条件放行到主界面（v2.1.59 先放行，数据后台补）
    seg = APP[APP.index("if (sessionToken.value) {"):]
    seg = seg[:seg.index("sessionPollTimer = setInterval")]
    assert "authed.value = true" in seg
    # token 已被 401 清掉时，api() 的 forceSessionExpired 负责把 authed 落 false
    assert "authed.value = false" in APP


def test_boot_timeout_does_not_log_user_out():
    """超时 ≠ 会话失效：只有 401 那条路能清 token 回登录页。"""
    seg = APP[APP.index("if (sessionToken.value) {"):]
    seg = seg[:seg.index("sessionPollTimer = setInterval")]
    # 超时分支不许清 token
    assert "localStorage.removeItem(SESSION_KEY)" not in seg
