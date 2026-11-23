# B 库清空重整流程（v2.1.11）

> 适用场景：**用户手动清理了 B 库存量文件**（整个 B 库被搬空，或个别子目录被清空），准备重新整理。
> 入口：后台管理 → 镜像同步 → 「B 库清空重整」按钮。
> 端点：`POST /api/mirror/restructure`

---

## 一、为什么需要这一步

B 库的文件在**网盘上**被用户手工删掉/搬走后，系统里的四层状态还停留在旧快照：

| 层 | 旧状态 | 用户看到的现象 |
| --- | --- | --- |
| 镜像映射层 | 文件「已同步」、目录「已创建」 | 映射清单里全是绿灯，实际 B 库早已空了 |
| 镜像任务层 | 复制任务「已提交 / 进行中」 | 任务列表一直挂着待确认，轮询反复失败 |
| 资源库层 | 资源「已识别 / 已分享」 | 资源库面板列着一堆**实际不存在**的影视 |
| 分享中心层 | 分享任务「待发布」、分享链接「有效」 | 推送时建链接失败、或复用一个已失效的链接 |

2026-11-23 真机已出过一次严重事故：镜像重建 B 库的空窗期撞上「B 库暂空」，被当可信空批次放行，资源库 4387 行被清到 0，九州·海上牧云记的快照 0/75 资源全失效，推送报「文件不存在」。

v2.1.11 起，清理一律**归档**而非物理删除，这类事故不再造成不可逆的数据丢失。

---

## 二、核心原则（红线）

### 1. 清理动作**不联动镜像同步**

> 用户原话：「重新镜像同步任务执行不联动，仍然等镜像同步任务开始」

重整端点**绝不**调用 `sync_rule`、**绝不**启动任何镜像任务、**绝不**碰 A 库。
它只把状态归位，**重建完全交给下一次定时镜像同步任务自然开始**。

这条边界有专门的回归测试守着（`test_restructure_never_triggers_sync`）。

### 2. 只归档，不物理删除

历史行与执行记录必须保留，事后要能追溯「当时推过什么、分享过什么」。

- `resources` / `push_queue` 两张表新增三列：`clean_state` / `cleaned_at` / `clean_reason`。
- 归档行**不出现在任何业务查询里**（`do_orm_execute` 事件钩子全局过滤，一处生效、全局覆盖）。
- 需要看归档行时用 `include_archived()` 上下文旁路（审计追溯、清点数量时用）。

### 3. 同一唯一键永远只有一行

归档行仍留在表里，唯一键（`resources.file_id`、`push_queue` 的 `resource_id+channel_id`）**依然被占用**。

重建时若直接按唯一键 INSERT 新行，会撞 `UNIQUE constraint failed` —— 这会让**整批扫描直接崩掉**。

真实触发路径（不只是理论）：B 库文件被删 → 归档 → 用户从网盘回收站恢复 → **fileId 不变** → 下一轮扫描撞约束。

所以重建命中同唯一键时，一律**复用归档行并清掉归档标记**（`app/archive.py` 的 `find_resource_by_file_id` / `find_queue_item`）。这与分享中心「cancelled 的 first 任务要复活而不是新建」是同一条原则。

### 4. A 库锚点必须保留

文件映射重置时**只清 B 库侧字段**：

| 清掉 | 保留 |
| --- | --- |
| `target_file_id`（B 库文件 ID） | `source_file_id`（A 库文件 ID） |
| `target_name`（B 库改名结果） | `file_name`（A 库原名） |
| `rename_status` / `rename_error` | `fingerprint`（size:utime 指纹） |
| `task_id` / `error` | |

`file_name` / `source_file_id` / `fingerprint` 是**增量比对锚点**——下轮同步靠它判断「这个文件还没复制过」。清了就变成永远复制不回来。

---

## 三、四层处理明细

### ① 镜像映射层

| 对象 | 处理 |
| --- | --- |
| `MirrorFile` | `status` → `pending`；清 `target_file_id` / `target_name` / `rename_status` / `rename_error` / `task_id`；**保留 A 库锚点** |
| `MirrorMedia` | `status` → `pending`；`last_synced_at` 置空 |
| `MirrorDir` | `status` → `missing`；`target_dir_id` 清空；`error` 打上 `[pruned]` 标记（= 待重建，不占用「待创建目录」清单） |

### ② 镜像任务层

本规则下**未终结**的复制任务终态化：`task_status` → `cancelled`，`status` → `cancelled`，写明终止原因。
已完成的（`completed` / `failed` / 历史行）不动。

### ③ 资源库层

| 对象 | 处理 |
| --- | --- |
| `Resource` | 归档（`clean_state='cleaned'` + `cleaned_at` + `clean_reason`）；分享态归零（`share_status='not_created'`、`share_id`/`share_url` 清空） |
| `QueueItem` | 归档；非终态行 `status` → `cancelled`（**终态化**，避免归档行仍被 worker 领走重发） |

### ④ 分享中心层

| 对象 | 处理 |
| --- | --- |
| `ShareTask` | 未终结的（`planned`/`building`/`ready`/`submitted`/`accepted`/`publishing`）→ `cancelled` |
| `ShareRecord` | `current_flag=1` 的作废（`current_flag=0` + `share_status='cancelled'`），**历史行保留** |

### ⑤ 日志层

镜像规则的 `last_status` → `never`、`last_message` → 空、`last_pipeline_json` → 空、
`batch_status` → `idle`、`batch_id` → 空、`control_action` → 空。

效果：页面状态标签回到「尚未同步」，进度条不再显示上一轮的陈旧百分比。

---

## 四、关联口径（**必须精确，不能靠作品名猜**）

这一节是本流程最容易写错的地方。

| 关联 | 用的键 | 为什么 |
| --- | --- | --- |
| 资源库行 ↔ 文件映射行 | **`Resource.file_id` = `MirrorFile.target_file_id`**（B 库文件 ID） | 唯一严格对得上的键 |
| 分享中心行 ↔ 资源库行 | `work_group_key(resource)` | 分享任务本来就是从资源行建出来的，这套键在分享中心内部自洽 |

**不能**用 `MirrorMedia.work_key` 去匹配资源行：`MirrorMedia.work_key` 是「**A 库目录名**」归一化，
`work_group_key(resource)` 是「**B 库资源名**」归一化——两套口径，名字对不上时整部作品会被漏掉。

（这个坑在开发中真实踩到过：测试数据里 `MirrorMedia.work_key='work:test'` 而资源行的
`share_dir_name='测试剧'`，按作品名匹配的结果是「一部都没命中」。现已改为文件 ID 精确关联。）

---

## 五、范围

| scope | 含义 | 参数 |
| --- | --- | --- |
| `library` | 整个 B 库被清空（默认） | — |
| `dir` | 只有个别子目录被清空 | `source_dir_ids`（A 库目录 ID 列表，对应 `MirrorDir.source_dir_id`） |

`scope=dir` 时，其它作品**完全不动**（有专门的回归测试守着）。

---

## 六、用法

### 预览（强烈建议先做）

```json
POST /api/mirror/restructure
{ "scope": "library", "dry_run": true }
```

返回各层将要变动的条数，**不写库**：

```json
{
  "ok": true, "dry_run": true, "scope": "library",
  "preview": {
    "media": 89, "files": 4387, "dirs": 12, "tasks": 3,
    "resources": 4387, "queue": 1656,
    "share_tasks": 4, "share_records": 89
  }
}
```

### 执行

```json
POST /api/mirror/restructure
{ "scope": "library", "dry_run": false }
```

### 前端

后台管理 → 镜像同步 → A/B 库设置面板底部 →「B 库清空重整」。
按钮会依次弹两次确认：第一次说明场景与影响范围，第二次列出具体条数。

---

## 七、边界与保护

| 情况 | 行为 |
| --- | --- |
| 镜像批次正在运行 / 已暂停 | **409 拒绝**——用户点错会毁掉正在进行的同步 |
| 没有镜像规则 | 400，提示先保存 A/B 库目录 ID |
| `scope=dir` 但没给 `source_dir_ids` | 400 |
| `scope` 取值非法 | 400（只接受 `library` / `dir`） |

---

## 八、重建路径（重整之后会发生什么）

1. **用户手动清空 B 库** → 网盘上文件真的没了。
2. **点「B 库清空重整」** → 四层状态归位（不触发同步）。
3. **下一次定时镜像同步任务开始**（或用户点「立即同步」）：
   - 目录映射 `missing` + `[pruned]` → 引擎按 A 库目录树在 B 库**重建目录**；
   - 文件映射 `pending` + 保留 A 库锚点 → 引擎发现 B 库没有对应文件 → **重新复制**；
   - 复制完成 → 改名 → 翻译 → 入库识别；
   - 资源库行：按文件 ID 匹配，命中归档行则**复活复用**（不新建、不撞唯一键）；
   - 分享中心：`ShareTask` 的 `dedup_key` 命中已取消的旧任务 → **复活旧任务**而不是新建（否则撞 `uq_share_task_dedup`）；
   - 重新建分享链接 → 重新入队推送。

---

## 九、相关代码

| 文件 | 作用 |
| --- | --- |
| `backend/app/archive.py` | 归档 / 复活语义的集中实现（`find_resource_by_file_id` / `find_queue_item`） |
| `backend/app/db.py` | 全局归档过滤（`do_orm_execute` 钩子）+ `ContextVar` 旁路开关 |
| `backend/app/models.py` | `clean_state` / `cleaned_at` / `clean_reason` 三列 + `include_archived()` / `revive_archived()` |
| `backend/app/main.py` | `POST /api/mirror/restructure` 端点 + `_restructure_scope_rows()` 关联计算 |
| `backend/app/scanner.py` | `_upsert_resource` 按唯一键查找时复用归档行 |
| `tests/test_v211_b_library_restructure.py` | 需求1 的端到端验收（10 项） |
