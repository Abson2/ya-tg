"""阶段三 B-05：限制校验缓存 TTL + 硬上限 GC。

公开群累计用户 uid 会让 _restrict_cache 线性膨胀（原实现只在命中时覆盖、从不删除）。
验证 _restrict_gc：过期项清理 + 超上限时按写入时间淘汰最旧项。
"""
import time

from app import bot_listener


def test_expired_entries_removed():
    d = bot_listener._restrict_cache
    d.clear()
    now = time.time()
    d["fresh"] = (now, True, "")
    d["stale"] = (now - 9999, False, "old")
    bot_listener._restrict_gc(ttl=120)
    assert "fresh" in d and "stale" not in d
    d.clear()


def test_hard_cap_trims_oldest():
    d = bot_listener._restrict_cache
    d.clear()
    cap = bot_listener._RESTRICT_CACHE_MAX
    now = time.time()
    for i in range(cap + 50):
        d[f"u{i}"] = (now + i, True, "")  # u0 最旧
    bot_listener._restrict_gc(ttl=0)  # ttl=0 禁止按时间清，只能靠硬上限
    assert len(d) == cap
    assert "u0" not in d  # 最旧的被淘汰
    assert f"u{cap + 49}" in d  # 最新的保留
    d.clear()


def test_ttl_zero_keeps_unexpired_semantics():
    """ttl=0 表示不按过期清（与现有 'ttl>0 才判过期' 语义一致）。"""
    d = bot_listener._restrict_cache
    d.clear()
    d["a"] = (time.time() - 9999, True, "")
    bot_listener._restrict_gc(ttl=0)
    assert "a" in d  # ttl=0 不按时间淘汰，仅靠硬上限
    d.clear()
