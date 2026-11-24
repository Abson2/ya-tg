"""v2.1.35：推送类型文案统一为「首次」。

用户原话（2026-11-25）：
  「推送历史里面推送类型叫首次，推送队列里面叫首次推送，把字段保持一致都叫首次
    （包括推送失败的列表）」

实际情况比用户记的更绕一点：三张表的**真实任务行**本来都已经是「首次」（走
`scKindLabel(kind, true)`），不一致的是另外两处——

  · 「推送队列」里的**待排期行**（`_src === 'plan'`）单独写死「首次发布」；
  · 「推送任务」页与详情弹窗用的是 `scKindLabel(kind)`（不传 short）→ 「首次发布」。

同一个含义在同一个页面上有两种写法，用户看到的就是「这块叫首次、那块叫什么别的」。
现在 `first` 只剩一种取值「首次」，`short` 形参保留但不再影响文案。

本文件锁死：
  ① 运行时代码里不许再出现「首次发布」（注释里解释历史可以，代码不行）；
  ② 待排期行与真实任务行取同一个词；
  ③ 三张表的「推送类型」列必须都走同一个函数 —— 不许某一列自己拼文案。
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_VUE = (ROOT / "frontend/src/App.vue").read_text(encoding="utf-8")


def _strip_comments(text: str) -> str:
    """剥整行注释。⚠️ 不能用正则剥块注释：App.vue 里有 `/api/*` 这种写法，
    会把后面十几万字符一路吞掉（v2.1.34 踩过）。"""
    out = []
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("//") or s.startswith("/*") or s.startswith("*"):
            continue
        out.append(line)
    return "\n".join(out)


def test_no_long_form_left_in_runtime_code():
    """运行时代码里「首次发布」必须绝迹 —— 它就是分叉的源头。"""
    src = _strip_comments(APP_VUE)
    assert "首次发布" not in src, "还有地方在用长文案「首次发布」"


def test_kind_label_first_is_single_word():
    """`first` 只映射到一个词，不再按 short 分长短两套。"""
    src = _strip_comments(APP_VUE)
    assert "first: '首次'," in src, "first 没有映射成「首次」"
    assert "short ? '首次'" not in src, "又按 short 分成长短两套了"
    assert re.search(r"first:\s*short", src) is None, "first 仍在看 short 参数"


def test_plan_row_uses_the_same_word():
    """「待排期」行不能单独写一套文案 —— 排队即首次。"""
    src = _strip_comments(APP_VUE)
    assert "if (row._src === 'plan') return '首次'" in src, \
        "待排期行的推送类型又和真实任务行不一致了"


def test_three_tables_share_one_function():
    """历史 / 队列 / 失败三张表的「推送类型」列都走 `scQueueKindLabel`。"""
    src = _strip_comments(APP_VUE)
    # 三张表各有一处推送类型列，加上推送任务页用 scKindLabel，总数至少 3
    assert src.count("scQueueKindLabel(") >= 4, \
        "有表格自己拼推送类型文案，没走统一函数"
    for label in ("推送历史", "推送队列", "推送失败"):
        assert label in src


def test_other_kinds_untouched():
    """「追更」「手动推送」两句不动 —— 这次只统一首次。"""
    src = _strip_comments(APP_VUE)
    assert "update: '追更'" in src
    assert "force: '手动推送'" in src
