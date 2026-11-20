# Ya-TG 性能优化报告（阶段二路线 B + 阶段三）

> 生成基线：全部整改 `[skip ci]` 提交后、统一发版前。数据来自隔离测试环境，**纯本地 mock 数据，不碰生产网盘、不发真实网络请求**。
> 取证脚本：`tests/perf_strict_baseline.py`（可重复运行）；机器为本机 Python 3.13 隔离 venv，单次结果有波动，取多轮区间。

## 1. 严格顺序引擎：排序/遍历开销（隔离基线）

严格顺序镜像的确定性开销集中在三处：文件名自然排序 `natural_key`、目录树 DFS 遍历 `_media_files`、作品轮转切批 `_plan_work_batch`。用 100 / 1000 / 10000 三档模拟文件树（每目录 50 文件，work → season → file 三层），实测：

| 规模 | natural_key 排序 | _media_files DFS | _plan_work_batch 切批 |
| --- | --- | --- | --- |
| 100 文件 | 0.07–0.09 ms | 0.07–0.09 ms | 0.08 ms |
| 1000 文件 | 0.79 ms | 0.72–0.92 ms | <0.01 ms |
| 10000 文件 | 8.5–9.6 ms | 7.4–7.5 ms | 0.01 ms |

**结论**：10000 文件的排序与遍历均 <10 ms，相对网络复制（每文件 idle ≥0.5s + 轮询）可忽略；严格顺序的"确定性顺序"不引入性能瓶颈。此前 C5 记录的"10000 文件排序 <8ms"为单次样本，多轮复跑实测上界约 9.6 ms，本报告以真实区间为准（不沿用偏乐观的单次数值）。

## 2. 阶段三查询与渲染优化（N+1 / SQL 聚合 / 前端并行）

| 项 | 改造前 | 改造后 | 证据 |
| --- | --- | --- | --- |
| 投稿/队列/审核列表 | 逐行 `db.get` 1+N | 一次批量 `IN` 查询 + map | `tests/test_v101_n1_optimization.py` |
| 日志统计 | 28 次 SQL 聚合 | 7 次 | 同上 |
| `runtime_config.get_many/source_map` | 每 key 一次查询 | 批量读取 | `tests/test_v101_config_batch.py` |
| 前端 `load()` 22 个 GET | 逐条 await 串行 | `Promise.allSettled` 并行，单失败不阻断 | `tests/test_v104_dark_theme.py` 间接受益 + build 通过 |
| `_restrict_cache` | 无限增长 | TTL + 硬上限 GC | `tests/test_v101_cache_gc.py` |
| 求片 TMDB 元数据 | 每次重复请求 TMDB | 24h TTL + 500 硬上限缓存（失败不缓存） | `tests/test_v103_tmdb_meta_cache.py` |

**未量化项（如实标注）**：以上为结构性优化（查询次数/缓存命中），未在真机 NAS 上对"页面加载耗时"做秒表对比——真机环境涉及网络 RTT 与光鸭限流，串行→并行的实际收益取决于后端响应时间。优化前后功能一致性由全量回归保证，性能收益以"SQL 次数/请求数下降"为客观口径，不虚构端到端毫秒数。

## 3. 未做的性能项（如实记录）

- **A-04 逐作品串行 TMDB**：保持串行。并发会触发 TMDB 限流（`TmdbRateLimited`），反而降低吞吐并可能封禁；属刻意取舍，非遗漏。
- **完整 async session 迁移**：属架构级重构，触及全部 50+ 路由，违反"功能一致"原则本批不做；已用 `asyncio.to_thread` 把两个纯 DB job 挪出事件循环缓解（见 SECURITY_REPORT A-07）。
- **strict 模式真机端到端耗时**：未验证。严格顺序每文件需"核验→复制→轮询终态→下一个"，真机单轮耗时取决于网盘复制速度与任务轮询周期，**不得用本地 mock 数据推断真机时长**。需在隔离测试网盘跑一轮完整作品后再出真机数字。

## 4. 复现方法

```bash
# 隔离性能基线（本地 mock，无网络/DB/生产网盘）
/Users/axs/.workbuddy/binaries/python/envs/default/bin/python tests/perf_strict_baseline.py

# 相关性能护栏单测
pytest tests/test_v100_natural_sort.py tests/test_v101_n1_optimization.py \
       tests/test_v101_config_batch.py tests/test_v101_cache_gc.py \
       tests/test_v103_tmdb_meta_cache.py -q
```
