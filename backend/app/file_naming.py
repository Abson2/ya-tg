"""v0.8.6：B 库单集 / 单电影文件的英文剧名改名（纯命名规则，无网络、无 DB）。

边界（用户 2026-11-18 明确，改动前务必确认）：
  - A 源头库**只读**，绝不改名；
  - B 库分类目录 / 影视主目录 / 季目录命名保持 v0.8.3 的 `·` 约定不变；
  - 本次**只改 B 库里的单集、单电影文件名**；
  - 格式选「只换中文片名段」：把中文剧名替换为英文剧名，其余后缀
    （季集号、年份、画质、编码、压制组、扩展名）**原样保留**，包括里面残留的中文；
  - `MirrorFile.file_name` 永远保存 A 库原名，改名结果另存 `target_name`。
    A→B 映射与增量比对仍用 `source_file_id + fingerprint(size:utime)`，与文件名无关，
    因此改名不会破坏「后期对比变量更新」。

英文名来源由 `mirror._resolve_english_title()` 负责（TMDB en-US → AI 直译 → 本地拼音），
本模块只负责「拿到英文名之后怎么拼出 B 库文件名」。
v0.8.9：新增 `pinyin_title()` 零网络兜底，TMDB 查不到且 AI 不可用也必须有可用英文名。
"""
import re

_CN = re.compile(r"[\u4e00-\u9fff]")
_CN_LEAD = re.compile(r"[\u4e00-\u9fff]+")
# 网盘文件名禁止的字符。create_dir 实测连 `*` 都拒绝（code=141），改名同样先清掉。
_ILLEGAL = re.compile(r'[\\/:*?"<>|\r\n\t]')
_BRACKETS = re.compile(r"[\[(（【{][^\])）】}]*[\])）】}]")
_TAG = re.compile(r"\b(?:tmdb|imdb)[-_:：]?\d+\b", re.I)
_NAME_MARKS = re.compile(r"[*·]")
_YEAR = re.compile(r"(?:19|20)\d{2}")


def work_core_title(work_name: str) -> tuple[str, str]:
    """从 A 库影视主目录名提取「中文片名段」与年份，供文件名定位与 AI 翻译。

    括号段（年份、`{tmdb-}`、压制组噪声）与 `·` / `*` 标记都不属于片名本体。
    """
    text = str(work_name or "").strip()
    year_m = _YEAR.search(text)
    year = year_m.group(0) if year_m else ""
    core = _BRACKETS.sub(" ", text)
    core = _TAG.sub(" ", core)
    core = _NAME_MARKS.sub("", core)
    core = re.sub(r"\s+", " ", core).strip(" -_.")
    return core, year


def sanitize_english_title(title: str) -> str:
    """英文名清洗：去掉网盘非法字符，压缩空白；不含中文才可作为「英文剧名」使用。"""
    t = _ILLEGAL.sub(" ", str(title or ""))
    t = re.sub(r"\s+", " ", t).strip(" .-_")
    if not t or _CN.search(t) or not re.search(r"[A-Za-z]", t):
        return ""
    return t[:120]


def pinyin_title(chinese: str) -> str:
    """v0.8.9：零网络兜底「直译」——中文剧名转拼音，保证永远有 ASCII 剧名。

    TMDB en-US 查不到、AI 直译未配置或超时时，绝不把整轮同步卡在网络等待上：
    直接在本地生成纯 ASCII 剧名，让 B 库单集/电影文件名不再含中文。
    这是音译而非语义直译，仅作最后一层兜底；AI 可用时优先用 AI 直译。
    `pypinyin` 是纯 Python 依赖，缺包时返回空串由调用方列入「待翻译」清单。
    """
    text = str(chinese or "").strip()
    if not text or not _CN.search(text):
        return ""
    try:
        from pypinyin import lazy_pinyin
        parts = lazy_pinyin(text)
    except ImportError:
        return ""
    words: list[str] = []
    for part in parts:
        p = re.sub(r"[^A-Za-z0-9]", "", str(part))
        if p:
            words.append(p[:1].upper() + p[1:].lower())
    return sanitize_english_title(" ".join(words))


def _core_variants(core: str) -> list[str]:
    """A 目录名是无标记中文，B 库文件名可能被压制成 `大·醉·侠` / `大 醉 侠` 等。

    按长度降序匹配，优先吃掉最长的片名段，避免只替换掉第一个汉字。
    """
    out = {core}
    if core:
        out.add("·".join(core))
        out.add("*".join(core))
        out.add(" ".join(core))
        out.add(".".join(core))
    return sorted((v for v in out if v), key=len, reverse=True)


def english_file_name(source_name: str, core_title: str, english_title: str) -> str:
    """把 A 库文件名里的中文片名段换成英文名，返回 B 库应使用的新文件名。

    返回空串表示「不需要改名」：原文件名没有中文（已经是英文/ASCII），
    或英文名为空 / 不合法（调用方应记为「待翻译」，绝不瞎猜）。
    扩展名、季集号、年份、画质、压制组等后缀一律原样保留。
    """
    raw = str(source_name or "").strip()
    ext_m = re.search(r"(\.[A-Za-z0-9]{1,5})$", raw)
    ext = ext_m.group(1) if ext_m else ""
    stem = raw[: -len(ext)] if ext else raw
    if not stem or not _CN.search(stem):
        return ""
    eng = sanitize_english_title(english_title)
    if not eng:
        return ""

    new_stem = ""
    for variant in _core_variants(str(core_title or "").strip()):
        if variant and variant in stem:
            new_stem = stem.replace(variant, eng, 1)
            break
    if not new_stem:
        # 目录名与文件名写法差异过大时，退化为「第一个中文连续段」= 片名段。
        # 只替换这一段，后面的「第04集」等中文后缀按用户要求保持原样。
        m = _CN_LEAD.search(stem)
        if not m:
            return ""
        new_stem = stem[: m.start()] + eng + stem[m.end():]
    new_stem = _ILLEGAL.sub(" ", new_stem)
    new_stem = re.sub(r"\s{2,}", " ", new_stem).strip()
    if not new_stem or new_stem == stem:
        return ""
    return new_stem + ext


def with_conflict_suffix(name: str, n: int) -> str:
    """B 库已有同名文件（例如 A 库内容更新后新复制的版本）时加 ASCII 序号。

    只在冲突时使用，保证「只换中文片名段」的常规情况下文件名不被额外改写。
    """
    if n <= 1:
        return name
    ext_m = re.search(r"(\.[A-Za-z0-9]{1,5})$", name)
    ext = ext_m.group(1) if ext_m else ""
    stem = name[: -len(ext)] if ext else name
    return f"{stem}-v{n}{ext}"
