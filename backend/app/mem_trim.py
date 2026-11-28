"""进程内堆回收（v2.1.67）。

**为什么需要它**：CPython 释放对象时只把内存还给 pymalloc / glibc，**不还给操作系统**。
glibc 的 malloc arena 只要里面还剩一个活对象，整块就留着；而 pymalloc 的 arena
要「完全为空」才归还。真机表现就是 RSS 冲高后回不去：容器常驻 1.33 GB，
`gc.collect()` 只能回落十来 MiB，VmHWM 与 VmRSS 几乎相等（说明一直没真正释放过）。

`malloc_trim(0)` 让 glibc 对各个 arena 顶部的空闲页做 `madvise(MADV_DONTNEED)`，
把它们还给内核。实测（2026-11-29，容器内，2.59 万行库）：

    连跑 25 轮整表实体化后 RSS = 236.7 MiB
    gc.collect() 之后          = 225.1 MiB   （-11.6）
    malloc_trim(0) 之后        = 179.4 MiB   （-45.7，-20%）
    连续再 trim 3 次           = 179.4 MiB   （一次到位，无需连打）

**平台边界**：只在 glibc（Linux）上有效。macOS 的 libc 没有 `malloc_trim`，
musl（Alpine）也没有——两处都静默跳过，返回 False，绝不影响主流程、绝不抛异常。

**为什么节流**：trim 要遍历所有 arena，不是零成本。5 秒一轮的调度里每轮都打
没有意义（刚 trim 完下一轮又分配回来了），按「每 N 轮一次」摊薄。
"""
from __future__ import annotations

import ctypes
import logging

logger = logging.getLogger(__name__)

_libc = None
_probed = False
_calls = 0
_failures = 0
_counter = 0


def _load():
    """惰性探测 libc 是否提供 malloc_trim。只在第一次调用时试一次。"""
    global _libc, _probed
    if _probed:
        return _libc
    _probed = True
    try:
        lib = ctypes.CDLL("libc.so.6")
        # 符号不存在会抛 AttributeError —— 用它判断「这个平台有没有」
        getattr(lib, "malloc_trim")
        _libc = lib
    except Exception:
        _libc = None          # macOS / musl / 精简镜像：静默降级
    return _libc


def malloc_trim() -> bool:
    """调一次 malloc_trim(0)。返回是否真的执行了（平台不支持时为 False）。"""
    global _calls, _failures
    lib = _load()
    if lib is None:
        return False
    try:
        lib.malloc_trim(0)
        _calls += 1
        return True
    except Exception as exc:          # 极端情况（库被替换等）也不能影响上层
        _failures += 1
        logger.debug("[mem] malloc_trim 失败（忽略）：%s", exc)
        return False


def trim_every(n: int = 60) -> bool:
    """每 n 次调用真正 trim 一次。返回本次是否执行了 trim。

    n<=1 表示每次都 trim。计数器是模块级的，与调用方线程无关——
    调度入口只有 `_run_share_center_v2_cycle_in_thread` 一处，不存在竞争问题。
    """
    global _counter
    if n and n > 1:
        _counter += 1
        if _counter % int(n):
            return False
    return malloc_trim()


def stats() -> dict:
    """诊断用：这个进程到底调过几次、成功几次。"""
    return {"available": _load() is not None, "calls": _calls,
            "failures": _failures, "counter": _counter}
