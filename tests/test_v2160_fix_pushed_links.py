"""v2.1.60：已推送卡片链接修复脚本（scripts/fix_pushed_links.py）的纯逻辑。

改的是线上群消息，所以只测不碰网络的部分：
  · 分享 id 的解析（正文 / 按钮 / 干扰链接）
  · 判定分层：正文旧链接优先于按钮旧链接
  · 按钮重定向：只换分享链接，文案与「想看」回调一个不动
  · Telegram 错误归桶：改不了 ≠ 真出错
"""
import asyncio
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OLD = "https://www.guangyapan.com/s/111_aey8CVbCzY9joZbY"
NEW = "https://www.guangyapan.com/s/999_aey8CVbCzY9joZbY"


def _load():
    spec = importlib.util.spec_from_file_location(
        "fix_pushed_links", ROOT / "scripts" / "fix_pushed_links.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


FPL = _load()


class _Btn:
    def __init__(self, text, url=None, data=None):
        self.text, self.url, self.data = text, url, data


class _Row:
    def __init__(self, *buttons):
        self.buttons = list(buttons)


class _Markup:
    def __init__(self, *rows):
        self.rows = list(rows)


def test_share_id_parsing():
    assert FPL.share_id_in(OLD) == "111"
    assert FPL.share_id_in("https://example.com/a") == ""
    assert FPL.share_id_in("") == ""
    # 正文里同一链接出现两次只算一个 id；顺序按出现先后
    assert FPL.share_ids_in_text(f"看图 {OLD} 再看 {NEW} 又 {OLD}") == ["111", "999"]


def test_markup_roundtrip_keeps_everything_else():
    markup = _Markup(
        _Row(_Btn("📥 获取资源", url=OLD)),
        _Row(_Btn("👍 想看", data=b"pc.646")),
    )
    rows, changed = FPL.retarget_markup(FPL.markup_to_dict(markup), NEW)
    assert changed == 1
    assert rows[0][0] == {"text": "📥 获取资源", "url": NEW}
    assert rows[1][0] == {"text": "👍 想看", "callback_data": "pc.646"}   # 回调原样


def test_retarget_leaves_already_correct_button_alone():
    rows, changed = FPL.retarget_markup([[{"text": "📥 获取资源", "url": NEW}]], NEW)
    assert changed == 0 and rows[0][0]["url"] == NEW


def test_retarget_ignores_non_share_buttons():
    """外部链接（比如 TMDB 页）绝不能被顺手改掉。"""
    kb = [[{"text": "TMDB", "url": "https://www.themoviedb.org/movie/1"}],
          [{"text": "📥 获取资源", "url": OLD}]]
    rows, changed = FPL.retarget_markup(kb, NEW)
    assert changed == 1
    assert rows[0][0]["url"] == "https://www.themoviedb.org/movie/1"


def test_classify_prefers_body_stale():
    assert FPL.classify([], [], "999") == "no_link"
    assert FPL.classify(["999"], ["999"], "999") == "ok"
    assert FPL.classify([], ["111"], "999") == "stale_button"
    assert FPL.classify(["111"], ["999"], "999") == "stale_body"
    # 正文与按钮都旧，仍按正文报（正文更危险，要人看一眼）
    assert FPL.classify(["111"], ["111"], "999") == "stale_body"


def test_bucket_separates_impossible_from_broken():
    assert FPL.bucket("Bad Request: message is not modified") == "unchanged"
    assert FPL.bucket("Bad Request: message can't be edited") == "too_old"
    assert FPL.bucket("Bad Request: message to edit not found") == "gone"
    assert FPL.bucket("Bad Request: there is no text in the message to edit") == "wrong_method"
    assert FPL.bucket("Internal Server Error") == "error"


# ---------- 二次核实：不一致 ≠ 坏链接 ----------
class _Msg:
    def __init__(self, url):
        self.message = ""
        self.reply_markup = _Markup(_Row(_Btn("📥 获取资源", url=url)))


def _refine_with(monkeypatch, url, root_files):
    class FakeWf:
        def __init__(self, _db):
            pass

        async def share_access_token(self, _tail):
            return "tok"

        async def share_root_files(self, _tok, max_items=5):
            return root_files

    monkeypatch.setattr(FPL, "GuangYaWebFiles", FakeWf)
    rows = [{"verdict": "stale_button", "chat_id": "1", "message_id": "2",
             "current_id": "999", "current_url": NEW, "dir_id": "777"}]
    return asyncio.run(FPL.refine(None, rows, {("1", "2"): _Msg(url)}))[0]["verdict"]


def test_refine_accepts_another_valid_share_of_same_work(monkeypatch):
    """真机三条（雪花女神龙等）：按钮是**另一条合法分享**，根目录就是本作品目录 → 不动。"""
    verdict = _refine_with(monkeypatch, OLD,
                           [{"file_id": "777", "name": "某剧", "size": 0, "is_dir": False}])
    assert verdict == "ok_other"


def test_refine_still_fixes_when_old_share_is_dead(monkeypatch):
    """链接打不开（光鸭抛错）→ 保持要改。"""
    class DeadWf:
        def __init__(self, _db):
            pass

        async def share_access_token(self, _tail):
            raise RuntimeError("分享已失效（code=201）")

    monkeypatch.setattr(FPL, "GuangYaWebFiles", DeadWf)
    rows = [{"verdict": "stale_button", "chat_id": "1", "message_id": "2",
             "current_id": "999", "current_url": NEW, "dir_id": "777"}]
    verdict = asyncio.run(FPL.refine(None, rows, {("1", "2"): _Msg(OLD)}))[0]["verdict"]
    assert verdict == "stale_button"
