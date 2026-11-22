<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import QRCode from 'qrcode'

// v0.9.20：浏览器不再保存明文管理员密码，只存会话 token（服务端未活跃 2h 强制过期）。
const SESSION_KEY = 'ya_tg_session'
const password = ref('')                                    // 仅登录框临时持有，登录后清空
const sessionToken = ref(localStorage.getItem(SESSION_KEY) || '')
const sessionRemaining = ref(0)                             // 剩余未活跃秒数（轮询会话状态）
const authed = ref(false)
// v0.9.13 #252：刷新后保留当前页面。审核中心已删除，历史残留值回退到概览。
const TAB_STORAGE_KEY = 'ya_tg_current_tab'
const _savedTab = localStorage.getItem(TAB_STORAGE_KEY) || '概览'
const tab = ref(_savedTab === '审核中心' ? '概览' : _savedTab)
function setTab(name) {
  tab.value = name
  localStorage.setItem(TAB_STORAGE_KEY, name)
}
// 阶段六：深色主题。纯附加层——html.dark 类 + Element Plus 暗色变量，默认浅色不变。
const THEME_KEY = 'ya_tg_theme'
const isDark = ref(localStorage.getItem(THEME_KEY) === 'dark')
function applyTheme(dark) {
  isDark.value = !!dark
  document.documentElement.classList.toggle('dark', isDark.value)
  localStorage.setItem(THEME_KEY, isDark.value ? 'dark' : 'light')
}
function toggleTheme() { applyTheme(!isDark.value) }
applyTheme(isDark.value)  // 首屏即时应用，避免闪白
const clock = ref('')
const viewMode = ref('series')  // v0.6.4：作品视图为默认，单集按剧聚合，不再散落
const keyword = ref('')
const statusFilter = ref('')
const seriesGroups = ref([])   // v0.6.4：/api/library/series 聚合结果

const overview = ref({}), task = ref({ cron: '*/30 * * * *', enabled: true })
const resources = ref([])
const reviews = ref([]), queue = ref([]), templates = ref([]), audit = ref([])
// 四模块重构：分享中心任务列表与状态（独立命名，避免与光鸭 shareStatus 冲突）
// 分享中心：统一走全新数据体系（概览/任务/分类/排期/规则）
// 所有状态来自后端 /api/sc/*，前端不承载计划、不启定时器、不读旧链表。
const scOverview = ref({ counts: {}, status_counts: {}, link_state_counts: {}, kind_counts: {}, kpi: {}, schedule: {}, windows: [], recent_tasks: [], push_list_counts: {}, active_version_id: 0, pacing: [], paused: false, enabled: true, trend: { series: [] }, category_ratio: { items: [] }, hot_works: { items: [] }, views: { history: [], queue: [], failed: [] } })
const scTasks = ref([]), scLists = ref([]), scListItems = ref([])
const scCategories = ref([]), scPlans = ref({ items: [], versions: [] })
const scCursors = ref([]), scAI = ref([]), scWindows = ref([])
const scPacingForm = ref({ scope_key: 'global:group', target_type: 'group', default_interval_seconds: 4, min_interval_seconds: 3, max_per_minute: 0, jitter_seconds: 0 })
const scActiveList = ref(null) // 当前展开明细的清单（卡片内交互）
// 发布总览：左侧子导航 + KPI 点击筛选 + 任务详情（全部基于 /api/sc/* 真实数据）
const scSubNav = ref('发布总览')
// 顶部 tab：发布总览 → 发布规则 → 推送任务（合并原发布队列）→ 分享管理 → 阅读量
const scSubNavs = [
  { key: '发布总览', ico: '◈' }, { key: '发布规则', ico: '◷' },
  { key: '推送任务', ico: '▷' }, { key: '分享管理', ico: '▦' },
  { key: '阅读量', ico: '◉' }
]
// ---------------------------------------------------------------------------
// v2.1.7 #241：阅读量统计（MTProto user account）
// 口径边界（页面必须如实表达，不得越界承诺）：
//   - **只有频道帖有浏览量**，群聊 Telegram 根本不提供 → 群聊目标不显示阅读量列；
//   - **阅读量是累计值**，趋势按相邻两次采集求差 → 展示的是「期间新增」；
//   - 读不到的消息标记为不可采集，**不显示 0**（0 会被误读成「没人看」）。
// 页面只使用中性展示名，不出现任何后端字段名。
// ---------------------------------------------------------------------------
const scStatsStatus = ref({ ready: false, dependency_ready: false, configured: false, logged_in: false, enabled: false, reason: '', capability: {} })
const scStatsTrend = ref({ buckets: [], targets: [], works: [], totals: {}, capability: {} })
const scStatsItems = ref([])
const scStatsRuns = ref([])
const scStatsDays = ref(7)
const scStatsLoading = ref(false)
const scStatsCollecting = ref(false)
const scStatsConfig = ref({ api_id: '', api_hash: '', phone: '', enabled: false, lookback_hours: 72, max_per_run: 200 })
const scStatsLoginCode = ref('')
const scStatsLoginCodeSent = ref(false)
const scStatsSaving = ref(false)
// 小时分类排期：每行一条 sc_hour_windows 记录，分类下拉来自真实 SCCategory
const scHourRows = ref([])
const scHourSaving = ref(false)
// v2.0.5 设计稿：分类-时间排期 / 基础规则 / 周期计划 / 趋势粒度 / 搜索筛选 / 选中详情
const scTrendPeriod = ref('day')
const scGenreCatalog = ref([])   // TMDB 官方 genre 字典（来自 /sc/categories/catalog）
const scSchedRows = ref([]), scSchedSaving = ref(false)
// v2.1.6：分类排期表勾选集（批量操作按勾选生效；未勾选或全勾选 = 全部）
const scSchedSelection = ref([]), scSchedTableRef = ref(null)
// v2.1.2：分类偏好排名（真实派生：字典优先级 + 已成功推送数），决定高峰独占时段分配
const scPreference = ref({ items: [], segments: [], peak_segments: [], day_segments: [], night_segments: [], full_day_window: [0, 24], segment_hours: 2, source: '' })
const scBasicRules = ref({ quick_publish_enabled: 'true', update_detect_minutes: '10', quick_publish_hourly_limit: '3', conflict_policy: 'prefer_release' })
const scCyclePlan = ref({ hour_max: 60, category_hour_max: 8, category_day_max: 0, quiet_start: 0, quiet_end: 7, quiet_enabled: false, day_max: 180, window_start: 0, window_end: 24, week_max: 1200, week_rest_enabled: false, month_max: 5000 })
const scRuleTab = ref('基础规则') // 基础规则 / 分类时间排期
const scCycleTab = ref('小时')     // 小时 / 日度 / 周度 / 月度
const scShareSearch = ref(''), scShareStatus = ref('全部'), scSelectedShares = ref([])
const scTaskSearch = ref(''), scTaskTypeFilter = ref('全部'), scSelectedDetail = ref(null)
const scTaskStatusFilter = ref(''), scLinkFilter = ref(''), scSelectedTasks = ref([])
const scDetailVisible = ref(false), scDetailTask = ref(null), scDetailExecutions = ref([])
// v2.1.2：分享详情改隐藏式弹窗（点「详情」才弹出），清理按钮的 loading 态
const scShareDetailVisible = ref(false)
const scCleaningTasks = ref(false), scCleaningLinks = ref(false), scAdoptingAllAi = ref(false)
const scCatForm = ref({ category_key: '', name: '', media_type: '', priority: 0 })
const scPlanVersionForm = ref({ plan_id: 1, config: '{}' })
const scFilteredTasks = computed(() => {
  let rows = scTasks.value
  const f = scTaskStatusFilter.value
  if (f === 'pending') rows = rows.filter(r => ['planned', 'ready'].includes(r.status))
  else if (f === 'inflight') rows = rows.filter(r => ['submitted', 'accepted', 'publishing', 'building'].includes(r.status))
  else if (f) rows = rows.filter(r => r.status === f)
  if (scLinkFilter.value) rows = rows.filter(r => (r.link_state || 'unknown') === scLinkFilter.value)
  return rows
})
// v2.1.1：分享管理改用独立真实端点 /sc/share-records（此前复用了 overview.views，
// 导致「全部记录 36 / 待核验 146」这类口径对不上的数字）。
const scShareRecords = ref([])
// 分享管理统计卡：后端按「清单项 + 任务」真实聚合，口径与表格行数一致
const scShareStats = computed(() => scOverview.value.share_stats || {})
// v2.1.4：链接状态中文映射。
// 分享管理页已改为消费后端下发的 `link_status_label` + `link_tone`
// （后端翻译，唯一权威），此处保留脚本侧兜底映射作为回归锚点：
// 任何入口拿到裸状态值都不得直接渲染，必须过这个函数。
const scLinkStateLabel = (state) => ({
  active: '有效', unknown: '待校验', suspicious: '疑似失效',
  inactive: '已失效', cleaned: '已清理', manual_review: '待人工',
}[state || 'unknown'] || '未知')
// v2.1.3：资源库卡片的分享状态 / 链接状态中文化。
// 红线：页面不得出现后端字段名或状态码（此前这里直接渲染 `link_state` 原值，
// 会把「链接suspicious」「链接cleaned」这种内部码露给用户）。
const mediaShareLabel = (status) => ({
  active: '已分享', not_created: '未分享', failed: '分享失败',
  cancelled: '已取消', expired: '已过期',
}[status || 'not_created'] || '未分享')
const mediaLinkLabel = (state) => ({
  suspicious: '链接疑似失效', inactive: '链接已失效',
  cleaned: '链接已清理', retired: '链接已退役', manual_review: '链接待人工',
}[state || ''] || '')
// 推送任务状态中文化（界面上不暴露内部枚举名）
// v2.1.6：新增 skipped ——「该资源在此渠道已推送过，跳过重复投递」
// （泰迪熊2 双推修复：已 sent 的队列行不得被复活，故任务落到已跳过终态）。
const scTaskStatusLabel = (status) => ({
  planned: '待发布', ready: '待推送', building: '建链中',
  submitted: '已提交', accepted: '已领取', publishing: '推送中',
  published: '已成功', failed: '失败', cancelled: '已取消',
  skipped: '已跳过',
}[status || ''] || status || '—')
// v2.1.6：执行记录里的「阶段 / 状态」同样中文化，页面不得直出内部枚举名。
const scExecStageLabel = (stage) => ({
  build: '建分享', submit: '提交队列', send: '发送',
}[stage || ''] || stage || '—')
const scExecStatusLabel = (status) => ({
  running: '进行中', success: '成功', failed: '失败', skipped: '已跳过',
}[status || ''] || status || '—')
// v2.1.3：时间显示统一简化。
// 真机问题：后端原先直接吐 ISO 串 `2026-11-22T01:00:17.123456+00:00`——
//  ①中间的 `T` 是用户完全看不懂的「未知字符」；②带微秒与时区后缀，列宽被顶满；
//  ③SQLite 存的是 UTC，浏览器按本地时区解读会整体偏移。
// 现在后端统一给上海本地时间的 `YYYY-MM-DD HH:mm:ss`，这里再收敛到分钟精度
// （表格友好），并兼容其它端点仍在返回的 ISO 串。
const fmtTime = (value, withSeconds = false) => {
  const raw = String(value ?? '').trim()
  if (!raw) return '—'
  const pad = n => String(n).padStart(2, '0')
  // 已是「YYYY-MM-DD HH:mm[:ss]」（后端已转本地时区）→ 直接用
  const m = raw.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::(\d{2}))?/)
  if (m) return withSeconds ? `${m[1]} ${m[2]}:${m[3] || '00'}` : `${m[1]} ${m[2]}`
  // 仍是 ISO：带偏移按浏览器本地时区展示；无偏移的按 UTC 处理（后端 SQLite 存 UTC）
  let iso = raw
  if (!/[Zz]|[+-]\d{2}:?\d{2}$/.test(iso)) iso += 'Z'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return raw
  const base = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`
  return withSeconds ? `${base}:${pad(d.getSeconds())}` : base
}
// v2.1.3：耗时毫秒 → 人类可读（页面不出现「耗时(ms)」这种工程口径）。
const fmtDuration = (ms) => {
  const n = Number(ms)
  if (!Number.isFinite(n) || n <= 0) return '—'
  if (n < 1000) return `${Math.round(n)} 毫秒`
  const s = n / 1000
  if (s < 60) return `${s.toFixed(1)} 秒`
  const m = Math.floor(s / 60)
  return `${m} 分 ${Math.round(s - m * 60)} 秒`
}
const scShareViews = computed(() => {
  let rows = [...(scShareRecords.value || [])]
  const s = scShareSearch.value.trim().toLowerCase()
  if (s) rows = rows.filter(r => (r.share_title || '').toLowerCase().includes(s)
    || (r.title || '').toLowerCase().includes(s)
    || (r.share_url || '').toLowerCase().includes(s)
    || (r.share_code || '').toLowerCase().includes(s))
  if (scShareStatus.value === '有效') rows = rows.filter(r => r.link_state === 'active')
  else if (scShareStatus.value === '待校验') rows = rows.filter(r => (r.link_state || 'unknown') === 'unknown')
  else if (scShareStatus.value === '疑似失效') rows = rows.filter(r => r.link_state === 'suspicious')
  else if (scShareStatus.value === '已失效') rows = rows.filter(r => ['inactive', 'cleaned'].includes(r.link_state))
  else if (scShareStatus.value === '已失败') rows = rows.filter(r => r.status === 'failed')
  return rows
})
const scTaskViewRows = computed(() => {
  let rows = scTasks.value
  const s = scTaskSearch.value.trim().toLowerCase()
  if (s) rows = rows.filter(r => (r.title || '').toLowerCase().includes(s)
    || (r.ref || '').toLowerCase().includes(s))
  if (scTaskTypeFilter.value !== '全部') rows = rows.filter(r => r.kind === scTaskTypeFilter.value)
  // KPI / 统计条点击跳转筛选
  const f = scTaskStatusFilter.value
  if (f === 'pending') rows = rows.filter(r => ['planned', 'ready'].includes(r.status))
  else if (f === 'inflight') rows = rows.filter(r => ['submitted', 'accepted', 'publishing', 'building'].includes(r.status))
  else if (f) rows = rows.filter(r => r.status === f)
  if (scLinkFilter.value) rows = rows.filter(r => (r.link_state || 'unknown') === scLinkFilter.value)
  // v2.1.4：推送渠道筛选（全部 / 仅频道 / 仅群）
  const c = scTaskChannelFilter.value
  if (c === 'channel') rows = rows.filter(r => (r.channel_names || []).length > 0)
  else if (c === 'group') rows = rows.filter(r => (r.group_names || []).length > 0)
  return rows
})
// v2.1.4：推送任务页的渠道筛选（全部 / 仅频道 / 仅群）
const scTaskChannelFilter = ref('')
// v2.1.6：推送渠道展示口径——「群:名称（群id或用户名）」「频道:名称（频道id或用户名）」。
// 后端下发结构化条目 {label, ref}，ref 优先取 Bot 发现台账里的 @用户名，
// 没有用户名时退回 chat_id。旧字段 channel_names / group_names 仍在下拉筛选里用。
function scChannelTargetLines(row) {
  const lines = []
  for (const t of (row?.channel_targets || [])) {
    lines.push(`频道:${t.label}（${t.ref}）`)
  }
  for (const t of (row?.group_targets || [])) {
    lines.push(`群:${t.label}（${t.ref}）`)
  }
  return lines
}
// v2.1.4：任务状态筛选下拉的选项（中文标签，不暴露内部枚举名）
const scTaskStatusOptions = computed(() => {
  const counts = scOverview.value.status_counts || {}
  const order = ['planned', 'ready', 'building', 'submitted', 'accepted',
                 'publishing', 'published', 'failed', 'cancelled', 'skipped', 'cleaned']
  return order
    .filter(st => counts[st])
    .map(st => ({ value: st, label: `${scTaskStatusLabel(st)}（${counts[st]}）` }))
})
const auth = ref({}), shareStatus = ref({}), webqr = ref(null), webqrPng = ref('')
const smsStatus = ref({}), smsPhone = ref(''), smsCode = ref(''), smsStep = ref(1), smsBusy = ref(false)
const tmdbStatus = ref({}), tmdbForm = ref({ api_key: '', bearer_token: '', language: 'zh-CN', scan_min_interval: '', scan_max_calls: '', bot_min_interval: '', bot_max_calls: '', rate_window_seconds: '' }), tmdbTesting = ref(false), tmdbSaving = ref(false)
const aiStatus = ref({}), aiForm = ref({ enabled: false, base_url: '', api_key: '', model: '', prompt: '', timeout: 30 }), aiSaving = ref(false), aiTesting = ref(false)
const aiParseName = ref(''), aiParseResult = ref(null)
const typeFilter = ref(''), identifyFilter = ref('')
// v0.7.0：一级状态栏（识别状态 / 媒体类型 / 完结追更）、勾选批量管理
const finishFilter = ref(''), statusTab = ref('all'), typeTab = ref('all')
const selectedSeries = ref([]), batchBusy = ref(false)
const batchDuration = ref(0)
// v2.1.6：资源库按 TMDB 分类分组展示 + 分类折叠。
// 分组依据后端 /api/library/series 返回的 primary_genre（TMDB 官方中文名，
// 与分享中心分类字典同一权威口径）；无分类的作品归「未分类」桶。
// collapsedGroups 记录被折叠的分类名；groupByCategory 关闭时回到原来的平铺海报墙。
const groupByCategory = ref(true)
const collapsedGroups = ref([])
const UNGROUPED_NAME = '未分类'
// v0.7.0：TG 分发扩展配置 + 赞赏码
const tgExtra = ref({ push_mode: 'channel', custom_bot_token: '', custom_chat_id: '', fubao_bot: '', link_style: 'button', detail_level: 'compact' })
const tgExtraSaving = ref(false), rewardCfg = ref({ code: '' }), rewardSaving = ref(false)
// v0.6.1：链路自检、Telegram 反代地址、批量重识别/立即推送状态
const diagnostics = ref({ blockers: [] }), telegramBase = ref(''), telegramSaving = ref(false)
// v0.6.4：队列推送调度（间隔秒 / Cron 二选一）
const queueSchedule = ref({ interval_seconds: 30, cron: '', send_delay: 2 }), scheduleSaving = ref(false)
const reidentifying = ref(false), publishing = ref(0), runningQueue = ref(false)
const matchTried = ref([]), matchTranslated = ref([])
// v0.8.0：扫描间隔按钮、A/B 镜像、扫描批次、用户互动
const scanInterval = ref(30), scanCustom = ref(false), scanPresets = ref([10, 15, 30, 60, 120, 360, 720, 1440])
const mirrorCfg = ref({ name: '默认镜像', source_root_id: '', target_root_id: '', interval_minutes: 60, enabled: false, star_naming: true, auto_identify: true, file_idle_seconds: 1.0, min_idle_seconds: 0.5, auto_create_dirs: true, rename_files: true, max_works_per_run: 0, mirror_mode: 'full', interval_presets: [], unverified_endpoints: [], note: '' })
const mirrorStatus = ref({ counts: {}, media: [], dirs: [], missing_dirs: [], tasks: [], rule: {}, config: {}, unverified_endpoints: [], rename_errors: [], untranslated_works: [], lock: {}, pipeline: [], progress: {} })
// v0.8.9：诊断表格与长说明默认收起，镜像同步页不再把所有信息堆在一屏
const mirrorDiagOpen = ref([])
const diagCount = computed(() => {
  const s = mirrorStatus.value
  return (s.missing_dirs?.length || 0) + (s.file_errors?.length || 0) +
    (s.untranslated_works?.length || 0) + (s.rename_errors?.length || 0)
})
const scanBatches = ref([]), mirrorBusy = ref(false), mirrorSaving = ref(false)
// 阶段二路线B C4：严格顺序引擎开关 + 批次控制状态
const mirrorStrict = ref(false), batchInfo = ref({ status: 'idle', total: 0, done: 0, percent: 0, current_sequence: 0 })
// v0.9.17.2：失败任务定向批量重试的勾选行（目录 / 文件各自独立）
const missingDirSelected = ref([]), fileErrSelected = ref([])
// v0.8.1：扫描设置不再单独成页，折叠进「资源库」
// v0.8.3：定时配置里不再出现任何根目录 ID 输入框，统一引用镜像同步里的 B 库 ID
const showScanPanel = ref(false)
// v0.9.4：Bot 交互配置——交互 Bot 复用 bots 表 token、白名单复用推送目标；内置四指令固定
const interactionCfg = ref({ contribution_enabled: false, donation_enabled: false, tgto_base_url: '', tgto_scene: 'ENV_FILTER_GUANGYA', tgto_api_key: '', interaction_bot_id: '', interaction_bot_username: '', donation_image_url: '', donation_text: '', donation_button_text: '打赏支持',
  // v0.9.13 #254：TgtoDrive 频道监控白名单规则 a/b/c（求片联动，与投稿转存无关）
  tgto_rule_resolution: '4k', tgto_rule_effect: 'all', tgto_rule_required_keyword: '265',
  // v0.9.19：投稿转存——审核通过后直接调光鸭 API 转存到指定目录（不经 TgtoDrive）
  contribution_restore_enabled: false, contribution_restore_target_dir_id: '',
  bot_push_channels: [], bot_push_groups: [], bot_restrict_enabled: false, bot_restrict_cache_seconds: 120, bot_required_channels: [], bot_required_groups: [], bot_commands: [], bot_gate_mode: 'whitelist', bot_options: [],
  // v0.9.5：使用限制跳转链接 + 推送消息频道/群跳转（模板变量 channel_link/group_link）
  // v0.9.10：新增机器人跳转链接（推送 Bot 本身），渲染层统一追加三行文字链接
  bot_required_channel_link: '', bot_required_group_link: '', tg_channel_link: '', tg_group_link: '', tg_bot_link: '' })
// v0.9.5：推送白名单 = 目录路由表 + 全局单 Bot
const pushCfg = ref({ bot: null, max_routes: 10 })
// v0.9.8：一条路由的频道 ID 与群 ID 分别填写，可只填其一或两个都填
// v0.9.14 #257：targets 支持一个目录绑定多个频道/群（与双字段并存，targets 优先）
const pushRoutes = ref([{ dir_id: '', channel_id: '', group_id: '', targets: [], note: '' }])
const pushSaving = ref(false), pushBotToken = ref('')
const broadcastText = ref(''), broadcastSending = ref(false)
function addPushRoute() {
  if (pushRoutes.value.length >= (pushCfg.value.max_routes || 10)) { ElMessage.warning('推送白名单最多 10 条'); return }
  pushRoutes.value.push({ dir_id: '', channel_id: '', group_id: '', targets: [], note: '' })
}
function removePushRoute(i) { pushRoutes.value.splice(i, 1) }
// v0.9.14 #257：路由目标多选——从已发现/手动目标里挑，写入 targets
function addRouteTarget(r) { r.targets = r.targets || []; r.targets.push({ kind: 'channel', chat_id: '', name: '' }) }
function removeRouteTarget(r, i) { r.targets.splice(i, 1) }
function onRouteTargetPick(r, i) {
  const d = discoverChats.value.find(x => x.chat_id === r.targets[i].chat_id)
  if (d) { r.targets[i].kind = d.chat_type === 'channel' ? 'channel' : 'group'; r.targets[i].name = d.title || d.username || '' }
}
// v0.9.14 #258：自动发现群组 / 统一推送目标
const discoverChats = ref([]), discovering = ref(false)
const manualChatId = ref(''), manualChatName = ref(''), manualChatKind = ref('auto')
const discoverChecking = ref(-1)
const TYPE_TAG = { group: '普通群', supergroup: '超级群', channel: '频道', private: '私聊' }
const MEMBER_LABEL = { creator: '群主', administrator: '管理员', member: '已加入',
  restricted: '受限', left: '已退出', kicked: '已被移出', unknown: '未知' }
// v0.9.15 #278：筛选 + 批量 + 轮询错误状态
const discFilterType = ref(''), discFilterBot = ref(''), discKeyword = ref('')
const discShowInactive = ref(true), discSelected = ref([]), discPollError = ref('')
const discBots = ref([])
function permLabel(c) {
  if (!c.perm_checked_at) return '未检查'
  if (c.perm_status === 'ok' && c.perm_can_send) return '可发送'
  if (c.perm_status === 'not_member') return 'Bot 不在群'
  if (c.perm_status === 'no_permission') return '无发送权限'
  return '未知'
}
function permTagType(c) {
  if (!c.perm_checked_at) return 'info'
  return (c.perm_status === 'ok' && c.perm_can_send) ? 'success' : 'danger'
}
function memberTagType(c) {
  return (c.member_status === 'left' || c.member_status === 'kicked') ? 'danger' : 'success'
}
// v0.9.17.1 #283：只有「Bot 仍在且可发送」的发现目标才允许在目录路由里选为推送目标。
// 已退出/被移出（left/kicked）或权限判 not_member 的目标不再出现在下拉——否则测试里
// 勾了「显示已退出」后，一个 Bot 根本不在的群仍能被选中推送，必然发不出去。
function isPushableDisc(c) {
  if (!c) return false
  if (c.active === false) return false
  const m = String(c.member_status || '')
  if (m === 'left' || m === 'kicked') return false
  if (c.perm_status === 'not_member') return false
  return true
}
const pushableDiscoverChats = computed(() => (discoverChats.value || []).filter(isPushableDisc))
async function loadDiscover() {
  syncDiscBots()
  const params = []
  if (discFilterType.value) params.push('chat_type=' + encodeURIComponent(discFilterType.value))
  if (discFilterBot.value) params.push('bot_id=' + encodeURIComponent(discFilterBot.value))
  if (discKeyword.value.trim()) params.push('q=' + encodeURIComponent(discKeyword.value.trim()))
  params.push('include_inactive=' + (discShowInactive.value ? 'true' : 'false'))
  try {
    const dc = await api('/discover/chats' + (params.length ? '?' + params.join('&') : ''))
    discoverChats.value = dc.chats || []
    discPollError.value = dc.poll_error || ''
  } catch (e) {}
}
async function refreshDiscover() {
  discovering.value = true
  try {
    const body = {}
    if (manualChatId.value.trim()) {
      body.manual_chat_id = manualChatId.value.trim()
      body.manual_name = manualChatName.value.trim()
      body.manual_kind = manualChatKind.value
    }
    const d = await api('/discover/refresh', { method: 'POST', body: JSON.stringify(body) })
    discoverChats.value = d.chats || []
    discPollError.value = (d.polled && d.polled.error) || ''
    // v0.9.16 #279：收敛为单一提示链，避免"发现完成"与诊断两个 toast 叠加
    if (d.added) {
      manualChatId.value = ''; manualChatName.value = ''
      ElMessage.success(`已添加：${d.added.title || d.added.chat_id}`)
    } else if (d.add_error) {
      ElMessage.error(d.add_error)  // 私有邀请链接 / getChat 失败的分类原因
    } else if (d.polled && d.polled.error_kind === 'conflict') {
      ElMessage.warning('Bot 轮询冲突：可能有另一个实例在使用同一 Token：' + d.polled.error)
    } else if (d.diagnosis) {
      ElMessage.info({ message: d.diagnosis, duration: 8000, showClose: true })
    } else if (d.polled && d.polled.messages) {
      ElMessage.info(`本轮处理 ${d.polled.messages} 条更新；新增发现 ${d.polled.discovered || 0} 个目标`)
    } else {
      ElMessage.info(d.hint || '暂无新记录；请确保 Bot 已入群并等待成员状态变化或新消息事件')
    }
  } catch (e) { ElMessage.error(e.message) }
  finally { discovering.value = false }
}
async function checkDiscoverPerm(c) {
  discoverChecking.value = c.id
  try {
    const d = await api(`/discover/chats/${c.id}/permission`, { method: 'POST' })
    const idx = discoverChats.value.findIndex(x => x.id === c.id)
    if (idx >= 0 && d.row) discoverChats.value[idx] = d.row
    ElMessage[d.ok ? 'success' : 'warning'](d.detail || '权限检查完成')
  } catch (e) { ElMessage.error(e.message) }
  finally { discoverChecking.value = -1 }
}
async function toggleDiscoverWhitelist(c, newValue) {
  // v0.9.17.1 热修：@change 给的 newValue 才是用户想要的目标态。此前用 v-model
  // 绑定 + 函数内 !prev 取反，而 @change 触发时 v-model 已翻转，导致发给后端的
  // 永远是旧值 → 开关"点了没反应/弹回"。改为受控 :model-value + @change 显式传值，
  // 成功后以回执为准，失败回滚到操作前状态。
  const prev = !!c.in_whitelist
  const target = (typeof newValue === 'boolean') ? newValue : !prev
  if (target === prev) return
  try {
    const d = await api(`/discover/chats/${c.id}/whitelist`, {
      method: 'POST', body: JSON.stringify({ in_whitelist: target }) })
    c.in_whitelist = d.in_whitelist
    ElMessage.success(d.message)
    await load()  // 白名单变动同步到目录路由卡片
  } catch (e) { c.in_whitelist = prev; ElMessage.error(e.message) }
}
async function batchWhitelist(on) {
  if (!discSelected.value.length) { ElMessage.warning('请先勾选目标'); return }
  try {
    const d = await api('/discover/chats/batch-whitelist', {
      method: 'POST', body: JSON.stringify({ ids: discSelected.value.slice(), in_whitelist: on }) })
    ElMessage.success(`批量${on ? '加入' : '移出'}白名单：成功 ${d.succeeded}/${d.total}`)
    discSelected.value = []
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function testDiscoverPush(c) {
  try {
    const d = await api(`/discover/chats/${c.id}/test`, { method: 'POST' })
    ElMessage[d.ok ? 'success' : 'error'](d.message || (d.ok ? '测试消息已发送' : '测试失败'))
  } catch (e) { ElMessage.error(e.message) }
}
async function deleteDiscoverChat(c) {
  // v0.9.18.1：删除 = 仅从本列表移除，不动推送白名单、不动 Bot 已加入的群/频道
  const ok = await confirmBox(`确认从列表移除目标「${c.title || c.chat_id}」？\n仅从此列表移除，不影响推送白名单与 Bot 已加入的群/频道；之后可手动输入 ID 或刷新重新加入。`)
  if (!ok) return
  try {
    await api(`/discover/chats/${c.id}`, { method: 'DELETE' })
    ElMessage.success('已从列表移除')
    await loadDiscover()
  } catch (e) { ElMessage.error(e.message) }
}
async function batchDeleteDiscover() {
  if (!discSelected.value.length) { ElMessage.warning('请先勾选目标'); return }
  const ok = await confirmBox(`确认从列表移除选中的 ${discSelected.value.length} 个目标？\n仅从此列表移除，不影响推送白名单与 Bot 已加入的群/频道。`)
  if (!ok) return
  try {
    const d = await api('/discover/chats/batch-delete', {
      method: 'POST', body: JSON.stringify({ ids: discSelected.value.slice() }) })
    ElMessage.success(`已从列表移除 ${d.removed} 个目标`)
    discSelected.value = []
    await loadDiscover()
  } catch (e) { ElMessage.error(e.message) }
}
// v0.9.15 #276：Bot 筛选来源（复用交互配置里的 bot_options，不含 token 明文）
function syncDiscBots() {
  try { discBots.value = (interactionCfg.value && interactionCfg.value.bot_options) || [] } catch (e) { discBots.value = [] }
}
// v0.9.15 #276：startgroup 官方授权链接（群组）/ 频道添加引导
const inviteLink = ref(null), inviteLoading = ref(false), channelGuideVisible = ref(false)
const inviteBotId = ref(null)
async function genInviteLink(botId) {
  inviteLoading.value = true
  try {
    const d = await api('/discover/invite-link', {
      method: 'POST', body: JSON.stringify(botId ? { bot_id: botId } : {}) })
    if (!d.ok) { ElMessage.error(d.error || '生成授权链接失败'); return }
    inviteLink.value = d
    // v0.9.16 #278：getMe 读取到的用户名回填输入框，管理员可见「自动读取」结果。
    // 后端 invite-link 端点已把该值持久化到配置，这里只同步本地显示，无需再 POST。
    if (d.bot_username) interactionCfg.value.interaction_bot_username = d.bot_username
    ElMessage.success('已生成授权链接，复制后在 Telegram 打开选择目标群')
  } catch (e) { ElMessage.error(e.message) }
  finally { inviteLoading.value = false }
}
function copyText(text, label) {
  if (!text) { ElMessage.warning('无可复制内容'); return }
  // v0.9.16 #278：局域网 HTTP 访问时 navigator.clipboard 不存在/被拒，
  // fallback 到隐藏 textarea + execCommand('copy')，兼容非 HTTPS 环境。
  const ok = (function () {
    try {
      const ta = document.createElement('textarea')
      ta.value = String(text)
      ta.setAttribute('readonly', '')
      ta.style.position = 'fixed'; ta.style.top = '-1000px'; ta.style.opacity = '0'
      document.body.appendChild(ta)
      ta.focus(); ta.select(); ta.setSelectionRange(0, ta.value.length)
      const done = document.execCommand('copy')
      document.body.removeChild(ta)
      return !!done
    } catch (e) { return false }
  })()
  if (ok) { ElMessage.success((label || '内容') + '已复制'); return }
  // 最后再试异步 Clipboard API（HTTPS/localhost 环境）
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(String(text)).then(
      () => ElMessage.success((label || '内容') + '已复制'),
      () => ElMessage.warning('浏览器禁止复制，请手动选中：' + text))
  } else {
    ElMessage.warning('浏览器禁止复制，请手动选中：' + text)
  }
}
function openStartgroup() {
  if (!inviteLink.value || !inviteLink.value.startgroup_url) { genInviteLink(); return }
  window.open(inviteLink.value.startgroup_url, '_blank', 'noopener')
}
const interactionStatus = ref({}), contributions = ref([]), mediaRequests = ref([])
const interactionSaving = ref(false), tgtoTesting = ref(false), wantTitle = ref('')
// v0.9.8 #194：推送效果报表（只展示后端真实可得指标）
const pushStats = ref({ series: [], totals: {}, unavailable: [], available: [], period: 'day', days: 30 })
const statsPeriod = ref('day'), statsDays = ref(30), statsLoading = ref(false)
async function loadPushStats() {
  statsLoading.value = true
  try {
    pushStats.value = await api(`/push-stats?period=${statsPeriod.value}&days=${statsDays.value}`)
  } catch (e) { ElMessage.error(e.message) }
  finally { statsLoading.value = false }
}
function statBarHeight(v, max) { return max > 0 ? Math.max(2, Math.round(v / max * 100)) : 2 }
// v0.9.0：多行文本框 ↔ 数组 的双向映射，避免用户直接编辑数组
const newCmd = ref({ command: '', description: '', reply: '' })
const _linesToArr = (v) => String(v || '').split('\n').map(s => s.trim()).filter(Boolean)
const _arrToLines = (a) => (a || []).join('\n')
const requiredChannelsText = computed({ get: () => _arrToLines(interactionCfg.value.bot_required_channels), set: v => interactionCfg.value.bot_required_channels = _linesToArr(v) })
const requiredGroupsText = computed({ get: () => _arrToLines(interactionCfg.value.bot_required_groups), set: v => interactionCfg.value.bot_required_groups = _linesToArr(v) })
function addCustomCommand() {
  const c = newCmd.value
  const name = String(c.command || '').trim().replace(/^\//, '')
  if (!name) { ElMessage.warning('请填写指令名'); return }
  const list = interactionCfg.value.bot_commands || (interactionCfg.value.bot_commands = [])
  if (list.some(x => x.command === name || x.builtin && x.command === name)) { ElMessage.warning('指令已存在'); return }
  list.push({ command: name, description: c.description || name, reply: c.reply || '', builtin: false })
  newCmd.value = { command: '', description: '', reply: '' }
}
function removeCustomCommand(c) {
  const list = interactionCfg.value.bot_commands || []
  const i = list.indexOf(c)
  if (i >= 0) list.splice(i, 1)
}
const finishChecks = ref([]), finishChecking = ref(false)
// v0.8.3：日志保留策略（操作审计页配置，不新增 .env 项）
const logCfg = ref({ retention_days: 30, max_rows: 5000, auto_prune: true, schedule: '' })
const logStats = ref([]), logBusy = ref(false), logOverview = ref({ total_rows: 0, total_pruneable: 0, container_log: '' })

async function loadTmdb() {
  try {
    tmdbStatus.value = await api('/tmdb/status')
    const r = tmdbStatus.value.rate_limit || {}
    tmdbForm.value.scan_min_interval = r.scan_min_interval ?? ''
    tmdbForm.value.scan_max_calls = r.scan_max_calls ?? ''
    tmdbForm.value.bot_min_interval = r.bot_min_interval ?? ''
    tmdbForm.value.bot_max_calls = r.bot_max_calls ?? ''
    tmdbForm.value.rate_window_seconds = r.rate_window_seconds ?? ''
  } catch (e) {}
}
async function loadAi() {
  try {
    aiStatus.value = await api('/ai/status')
    aiForm.value.enabled = !!aiStatus.value.enabled
    aiForm.value.base_url = aiStatus.value.base_url || 'https://api.openai.com/v1'
    aiForm.value.model = aiStatus.value.model || 'gpt-4o-mini'
    aiForm.value.prompt = aiStatus.value.custom_prompt || ''
    aiForm.value.timeout = aiStatus.value.timeout || 30
  } catch (e) {}
}
async function saveAi() {
  aiSaving.value = true
  try {
    const d = await api('/ai/config', { method: 'POST', body: JSON.stringify(aiForm.value) })
    aiStatus.value = d.status || {}
    ElMessage.success('AI 配置已保存（密钥加密存储），无需重启')
  } catch (e) { ElMessage.error(e.message) }
  finally { aiSaving.value = false }
}
async function testAi() {
  aiTesting.value = true
  try {
    const d = await api('/ai/test', { method: 'POST' })
    if (d.ok) ElMessage.success('连接成功，模型可用')
    else ElMessage.warning(d.message || '连接失败')
    await loadAi()
  } catch (e) { ElMessage.error(e.message) }
  finally { aiTesting.value = false }
}
async function parseAi() {
  if (!aiParseName.value.trim()) { ElMessage.warning('请输入文件名'); return }
  try { aiParseResult.value = await api('/ai/parse', { method: 'POST', body: JSON.stringify({ name: aiParseName.value }) }) }
  catch (e) { ElMessage.error(e.message) }
}
async function saveTmdb() {
  tmdbSaving.value = true
  try {
    const d = await api('/tmdb/config', { method: 'POST', body: JSON.stringify(tmdbForm.value) })
    tmdbStatus.value = d.status || {}
    ElMessage.success('TMDB 密钥已保存（加密存储），无需重启')
  } catch (e) { ElMessage.error(e.message) }
  finally { tmdbSaving.value = false }
}
async function testTmdb() {
  tmdbTesting.value = true
  try {
    const d = await api('/tmdb/test', { method: 'POST' })
    if (d.ok) ElMessage.success('连接成功，密钥有效')
    else ElMessage.error(d.message || '密钥无效')
    await loadTmdb()
  } catch (e) { ElMessage.error(e.message) }
  finally { tmdbTesting.value = false }
}

const smsCountdown = ref('')  // 凭证到期倒计时（本地每秒刷新）
function fmtBj(iso) {
  if (!iso) return ''
  const d = new Date(iso)
  if (isNaN(d.getTime())) return ''
  return d.toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false,
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' })
}
function fmtCountdown(sec) {
  if (sec == null || sec < 0) return '—'
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = Math.floor(sec % 60)
  return `${h}小时${String(m).padStart(2, '0')}分${String(s).padStart(2, '0')}秒`
}
function updateCountdown() {
  const d = smsStatus.value
  if (!d || !d.logged_in || d.expires_at) {
    // 有 expires_at 时按绝对时间算，避免依赖后端秒数需要频繁轮询
  }
  if (d && d.logged_in && d.expires_at) {
    const left = Math.floor((new Date(d.expires_at).getTime() - Date.now()) / 1000)
    smsCountdown.value = left > 0 ? fmtCountdown(left) : '已到期，等待自动刷新'
  } else if (d && d.logged_in && d.expires_in_seconds != null) {
    smsCountdown.value = fmtCountdown(d.expires_in_seconds)
  } else {
    smsCountdown.value = ''
  }
}
async function loadSms() {
  try {
    smsStatus.value = await api('/sms/status')
    // 页面刷新后根据后端真实登录态恢复向导步骤，避免"登录后又变回未连接"
    if (smsStatus.value.logged_in) smsStep.value = 3
    updateCountdown()
  } catch (e) {}
}
async function sendSms() {
  if (!smsPhone.value.trim()) { ElMessage.warning('请输入手机号'); return }
  smsBusy.value = true
  try { await api('/sms/send', { method: 'POST', body: JSON.stringify({ phone: smsPhone.value }) }); smsStep.value = 2; ElMessage.success('验证码已发送') }
  catch (e) { ElMessage.error(e.message) }
  finally { smsBusy.value = false }
}
async function verifySms() {
  if (!smsCode.value.trim()) { ElMessage.warning('请输入验证码'); return }
  smsBusy.value = true
  try { await api('/sms/verify', { method: 'POST', body: JSON.stringify({ code: smsCode.value }) }); smsStep.value = 3; ElMessage.success('光鸭已连接，凭证已加密保存'); await load(); await loadSms() }
  catch (e) { ElMessage.error(e.message) }
  finally { smsBusy.value = false }
}
async function refreshSms() {
  try { await api('/sms/refresh', { method: 'POST' }); ElMessage.success('光鸭凭证已刷新'); await loadSms() }
  catch (e) { ElMessage.error(e.message) }
}
// v0.9.6：「立即刷新凭证」按实际登录方式分流。扫码(webqr)的 refresh_token 是用
// webqr 公开 client_id 签发的，走短信通道(硬编码 aMe 客户端)会 invalid_grant。
// sms/tgto 才走短信刷新；webqr 走 /auth/refresh 的网页端设备码刷新分支。
async function refreshCredential() {
  const flow = (smsStatus.value?.flow || auth.value?.flow || '').toString()
  if (flow === 'sms' || flow === 'tgto') return refreshSms()
  return refreshToken()
}
function resetSms() { smsStep.value = 1; smsCode.value = '' }
// v0.9.5：Bot / 频道 / 规则 独立管理已并入「交互 Bot 与推送配置」（pushCfg/pushRoutes）
const tpl = ref({ name: '', content: '', enabled: true, is_default: false })
// v0.9.6：内置模板预设（下拉一键载入）+ 选中项
const tplPresets = ref([]), tplPresetPick = ref('')
function applyTplPreset() {
  const p = tplPresets.value.find(x => x.name === tplPresetPick.value)
  if (!p) return
  tpl.value = { id: 0, name: p.name, content: p.content, enabled: true, is_default: false }
  ElMessage.success('已载入内置模板，可修改后点「保存模板」另存')
}
function editTpl(t) { tpl.value = { id: t.id, name: t.name, content: t.content, enabled: t.enabled, is_default: t.is_default }; tplPresetPick.value = '' }
async function deleteTpl(t) {
  const tip = t.is_default
    ? `确认删除默认模板「${t.name}」？删除后系统会自动把另一套模板设为默认；若没有其它模板会自动恢复内置默认模板。`
    : `确认删除模板「${t.name}」？此操作不可撤销。`
  const ok = await confirmBox(tip)
  if (!ok) return
  try { await api(`/templates/${t.id}`, { method: 'DELETE' }); ElMessage.success('模板已删除'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
async function setDefaultTpl(t) {
  if (t.is_default) { ElMessage.info('该模板已是默认'); return }
  try {
    await api(`/templates/${t.id}`, { method: 'PUT',
      body: JSON.stringify({ name: t.name, content: t.content, enabled: t.enabled, is_default: true }) })
    ElMessage.success(`「${t.name}」已设为默认，后续推送按此模板渲染`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
}

let clockTimer = null, webqrTimer = null, mirrorPollTimer = null, sessionPollTimer = null
// v0.9.10.2：进度看板实时轮询——任务运行中每 2 秒只拉 /mirror/status（不复跑整页 load），
// 展示复制/改名/入库/分享/入队各阶段真实数量变化；空闲时降为 5 秒轻量刷新。
function stageLabel(s) {
  return ({ copy: '复制到 B 库', rename: '英文改名', identify: '入库识别',
            share: '生成分享 / 推送', done: '收尾', failed: '失败' })[s] || '处理中'
}
async function pollMirrorStatus() {
  if (!authed.value) return
  try { mirrorStatus.value = await api('/mirror/status') } catch (e) {}
  try { batchInfo.value = await api('/mirror/batch/status') } catch (e) {}
}
// 阶段二路线B C4：批次控制（暂停/恢复/跳过/取消），危险操作二次确认
async function batchControl(action) {
  if (action === 'cancel') {
    try { await ElMessageBox.confirm('取消后未完成的复制任务将停止推进，确定继续？', '取消当前批次', { type: 'warning' }) }
    catch (e) { return }
  }
  try {
    const d = await api(`/mirror/batch/control?action=${action}`, { method: 'POST' })
    ElMessage.success(`批次指令已下发：${action}`)
    await pollMirrorStatus()
    return d
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}

const navGroups = [
  { label: '工作台', items: [
    { key: '概览', icon: 'grid', label: '概览' },
    { key: '任务中心', icon: 'tasks', label: '任务中心' }
  ] },
  { label: '光鸭资源', items: [
    { key: '光鸭授权', icon: 'key', label: '光鸭授权' },
    { key: '镜像同步', icon: 'copy', label: '镜像同步' },
    { key: '资源库', icon: 'folder', label: '资源库' },
    // 四模块重构：原「推送队列」改名「分享中心」并上移；key 保留兼容既有断言与 section
    { key: '推送队列', icon: 'send', label: '分享中心' }
  ] },
  { label: '分发与互动', items: [
    { key: '分发与互动中心', icon: 'link', label: '分发与互动中心' }
  ] },
  { label: '系统', items: [
    { key: 'TMDB 设置', icon: 'image', label: 'TMDB 设置' },
    { key: '消息模板', icon: 'file', label: '消息模板' },
    { key: '操作审计', icon: 'clock', label: '操作审计' }
  ] }
]

const paths = {
  grid: 'M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z',
  tasks: 'M9 5h11M9 12h11M9 19h11M4 5l1.5 1.5L8 4M4 12l1.5 1.5L8 11M4 19l1.5 1.5L8 18',
  key: 'M21 2l-2 2m-7.6 7.6a5.5 5.5 0 1 1-7.8 7.8 5.5 5.5 0 0 1 7.8-7.8zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4',
  scan: 'M3 7V5a2 2 0 0 1 2-2h2M17 3h2a2 2 0 0 1 2 2v2M21 17v2a2 2 0 0 1-2 2h-2M7 21H5a2 2 0 0 1-2-2v-2M3 12h18',
  folder: 'M3 6a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z',
  route: 'M6 3v12a3 3 0 0 0 3 3h9M6 3a2 2 0 1 0 0 4 2 2 0 0 0 0-4zm12 12a2 2 0 1 0 0 4 2 2 0 0 0 0-4z',
  check: 'M20 6L9 17l-5-5',
  send: 'M22 2L11 13M22 2l-7 20-4-9-9-4z',
  file: 'M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8zM14 2v6h6M16 13H8M16 17H8',
  clock: 'M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20zM12 6v6l4 2',
  refresh: 'M23 4v6h-6M1 20v-6h6M3.5 9a9 9 0 0 1 14.9-3.4L23 10M1 14l4.6 4.4A9 9 0 0 0 20.5 15',
  external: 'M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6M15 3h6v6M10 14L21 3',
  link: 'M10 13a5 5 0 0 0 7.5.5l3-3a5 5 0 0 0-7-7l-1.7 1.7M14 11a5 5 0 0 0-7.5-.5l-3 3a5 5 0 0 0 7 7l1.7-1.7',
  image: 'M3 3h18v18H3zM21 15l-5-5L5 21',
  search: 'M11 19a8 8 0 1 0 0-16 8 8 0 0 0 0 16zM21 21l-4.4-4.4',
  plus: 'M12 5v14M5 12h14',
  copy: 'M8 8h12v12H8zM16 8V4H4v12h4',
  shield: 'M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z',
  bolt: 'M13 2L3 14h9l-1 8 10-12h-9z',
  play: 'M5 3l14 9-14 9z',
  logout: 'M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9',
  moon: 'M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z',
  sun: 'M12 17a5 5 0 1 0 0-10 5 5 0 0 0 0 10zM12 1v2M12 21v2M4.2 4.2l1.4 1.4M18.4 18.4l1.4 1.4M1 12h2M21 12h2M4.2 19.8l1.4-1.4M18.4 5.6l1.4-1.4'
}
const icon = name => paths[name] || paths.grid

// 阶段六：任务中心——纯聚合视图。数据源只用 load()/pollMirrorStatus() 已加载的 ref，
// 不新增任何 API，不造任何假数；缺失数据一律显示 0 并标注来源，点击可跳到对应原始页。
const taskCenter = computed(() => {
  const s = mirrorStatus.value || {}
  const node = k => (s.pipeline || []).find(n => n && n.key === k) || {}
  const copyN = node('copy'), renameN = node('rename'), identN = node('identify'),
        shareN = node('share'), pushN = node('push')
  // 队列状态聚合（真实 queue ref，后端 /api/queue status ∈ pending/review/retry/failed/sent）
  const q = { pending: 0, review: 0, retry: 0, failed: 0, sent: 0 }
  for (const item of (queue.value || [])) { const st = item && item.status; if (st in q) q[st]++ }
  // 投稿：审核态 status + 转存态 restore_status（v0.9.19 两态分离）
  const c = contributions.value || []
  const cPending = Number((interactionStatus.value || {}).contributions_pending || 0) ||
                   c.filter(x => x.status === 'pending').length
  const cRestorePending = c.filter(x => ['restoring', 'retry_wait', 'failed'].includes(x.restore_status)).length
  const cDone = c.filter(x => x.status === 'approved' || x.status === 'transferred').length
  // 求片：审核态 review_status + 同步态 status（管理员内部态，仅作计数）
  const m = mediaRequests.value || []
  const mPending = m.filter(x => (x.review_status || 'pending') === 'pending').length
  const mDone = m.filter(x => x.review_status === 'approved').length
  const mSynced = m.filter(x => x.status === 'synced').length
  // 批次状态映射（真实 batchInfo，阶段二 C4）
  const batchStatus = String(batchInfo.value?.status || 'idle')
  const batchLabel = ({ idle: '空闲', running: '运行中', paused: '已暂停',
                        cancelling: '取消中' })[batchStatus] || batchStatus
  const mk = (label, value, tone) => ({ label, value, tone: tone || '' })
  return [
    { key: 'mirror', label: '镜像任务', icon: 'copy', target: '镜像同步',
      status: batchStatus === 'running' ? 'active' : (['paused', 'cancelling'].includes(batchStatus) ? 'warn' : 'idle'),
      metrics: [mk('批次状态', batchLabel, batchStatus === 'running' ? 'good' : ''),
                mk('当前序列', batchInfo.value?.current_sequence || 0),
                mk('严格顺序', mirrorStrict.value ? '开' : '关')],
      note: '来源：批次状态接口 + 镜像进度接口（实时轮询）' },
    { key: 'copy', label: '复制到 B 库', icon: 'copy', target: '镜像同步',
      status: copyN.status || 'idle',
      metrics: [mk('本轮完成', copyN.value || 0), mk('文件总数', copyN.total || 0)],
      note: '来源：镜像进度流水线「复制到 B 库」节点' },
    { key: 'rename', label: '英文改名', icon: 'file', target: '镜像同步',
      status: (s.rename_errors?.length || s.untranslated_works?.length) ? 'warn' : (renameN.status || 'idle'),
      metrics: [mk('本轮改名', renameN.value || 0),
                mk('待解析英文名', s.untranslated_works?.length || 0, s.untranslated_works?.length ? 'bad' : ''),
                mk('改名失败', s.rename_errors?.length || 0, s.rename_errors?.length ? 'bad' : '')],
      note: '来源：镜像进度「英文改名」节点 + 未解析/失败诊断表' },
    { key: 'translate', label: '翻译识别', icon: 'image', target: '镜像同步',
      status: identN.status || 'idle',
      metrics: [mk('本轮识别', identN.value || 0, identN.status === 'active' ? 'good' : ''),
                mk('作品总数', identN.total || 0)],
      note: '来源：镜像进度「入库识别」节点（TMDB / AI 元数据）' },
    { key: 'share', label: '生成分享', icon: 'link', target: '镜像同步',
      status: shareN.status || 'idle',
      metrics: [mk('已分享', shareN.value || 0), mk('分享总数', shareN.total || 0)],
      note: '来源：镜像进度「生成分享」节点（目录分享，队列每 30 秒消费）' },
    { key: 'push', label: '分享中心', icon: 'send', target: '推送队列',
      status: q.failed ? 'warn' : ((q.pending || q.retry || q.review) ? 'active' : (q.sent ? 'done' : 'idle')),
      metrics: [mk('待发送', q.pending + q.review, (q.pending + q.review) ? 'good' : ''),
                mk('重试中', q.retry, q.retry ? 'warn' : ''),
                mk('失败', q.failed, q.failed ? 'bad' : ''),
                mk('已发送', q.sent)],
      note: '来源：分享中心任务 + 推送队列接口（真实计数）' },
    { key: 'contribution', label: '投稿处理', icon: 'shield', target: '分发与互动中心',
      status: cPending || cRestorePending ? 'active' : (cDone ? 'done' : 'idle'),
      metrics: [mk('待审核', cPending, cPending ? 'good' : ''),
                mk('转存待处理', cRestorePending, cRestorePending ? 'warn' : ''),
                mk('已通过', cDone)],
      note: '来源：投稿列表 + 互动统计接口（审核态 / 转存态分离）' },
    { key: 'request', label: '求片处理', icon: 'search', target: '分发与互动中心',
      status: mPending ? 'active' : (mDone ? 'done' : 'idle'),
      metrics: [mk('待审核', mPending, mPending ? 'good' : ''),
                mk('已采纳', mDone), mk('已同步', mSynced)],
      note: '来源：求片列表接口（审核态 / 同步态计数）' }
  ]
})

const syntaxHint = computed(() => '{{#if year}}年份 {{year}}{{/if}} · 条件块支持嵌套')
// 说明文字含 {{...}} 字面量，必须由 computed 返回字符串，不能直接写进 Vue 模板
const tagHint = computed(() => 'TMDB 分类打标：{{type}}=movie/tv，{{type_label}}=电影/剧集，{{origin_country}}=制片地区，{{genres}}=类型，{{tags}}=组合标签（示例 #保镖恋人 #电影 #爱情 #惊悚）。按示例打标可写：type={{type}} · origin_country={{origin_country}} · genres={{genres}}')
// v0.8.9：剧集状态 / 集数进度变量，说明含 {{}} 字面量必须由 computed 返回
const episodeHint = computed(() => '剧集状态：{{episode_status}}=追更「更新至 S01E07 · 连载中」/ 收齐「S01E24 · 完结」，{{episode_progress}}=N/M（如 12/24），{{updated_to_episode}}=已更新集号，{{total_episodes}}=TMDB 官方总集数，{{current_episode}}=当前集，{{episode_count}}=本地已收录集数。电影时这些变量为空，配合 {{#if}} 条件块使用')
const templateVars = ['title', 'overview', 'year', 'resolution', 'version', 'season_label', 'episode_count', 'reward_code', 'size', 'share_url',
  'tmdb_id', 'tmdb_url', 'type', 'type_label', 'origin_country', 'origin_country_code',
  'genres', 'tags', 'vote_average', 'original_title', 'season', 'episode', 'episode_tag', 'media_type', 'filename',
  'episode_status', 'episode_progress', 'updated_to_episode', 'total_episodes', 'current_episode',
  // v0.9.9 #202：用户新样式变量（来源/画质/编码/音频文件名解析，上映时间取 TMDB）
  'air_date', 'release_date', 'vote_plain', 'source', 'quality', 'video_codec', 'audio_codec',
  'season_count', 'episode_range', 'buttons_text', 'channel_link', 'group_link']
const previewDemo = {
  title: '保镖恋人', overview: '一个西班牙富商的女儿，在纽约的生活因一次绑架发生彻底颠覆。',
  filename: '保镖恋人.2026.2160p.WEB-DL.mkv', year: '2026', resolution: '2160P',
  version: '2160p · WEB-DL · H.265 · 10bit · DoVi P5 · DDP Atmos 5.1', season_label: '',
  episode_count: '36', size: '14.9GB', share_url: 'https://share.example/xxx', tmdb_id: '1440098',
  tmdb_url: 'https://www.themoviedb.org/movie/1440098',
  type: 'movie', type_label: '电影', origin_country: '西班牙', origin_country_code: 'ES',
  genres: '爱情、惊悚', tags: '#保镖恋人 #电影 #西班牙 #爱情 #惊悚',
  vote_average: '7.0', original_title: 'Marfil', season: '', episode: '',
  episode_tag: '', media_type: 'movie',
  episode_status: '', episode_progress: '', updated_to_episode: '',
  total_episodes: '', current_episode: '',
  // v0.9.9 #202：新样式预览值
  air_date: '2026-11-15', release_date: '2026-11-15', vote_plain: '9',
  source: 'Web-DL', quality: '2160p | 10bit | 60fps | SDR', video_codec: 'H265', audio_codec: 'AAC',
  season_count: '1', episode_range: 'S01 E01-E13', buttons_text: '📢 频道　👥 群组　🔍 投稿/搜索',
  channel_link: '', group_link: ''
}
const previewText = computed(() => {
  const raw = tpl.value.content || ''
  if (!raw) return '在左侧编写模板后显示预览'
  let t = raw.replace(/\{\{#if\s+\w+\}\}/g, '').replace(/\{\{\/if\}\}/g, '')
  for (const [k, v] of Object.entries(previewDemo)) t = t.split('{{' + k + '}}').join(v)
  return t
})

function identifyLabel(s) {
  return { identified: '已识别', ambiguous: '匹配存疑', failed: '识别失败',
           none: '无匹配', unidentified: '未识别' }[s] || s
}
function mediaLabel(t) { return t === 'tv' ? '剧集' : (t === 'movie' ? '电影' : '') }

const filteredResources = computed(() => resources.value.filter(r => {
  const hitKey = !keyword.value || (r.name + (r.media_title || '')).toLowerCase().includes(keyword.value.toLowerCase())
  const hitStatus = !statusFilter.value || r.share_status === statusFilter.value
  const hitType = !typeFilter.value || r.media_type === typeFilter.value
  const hitIdentify = !identifyFilter.value || r.identify_status === identifyFilter.value
  return hitKey && hitStatus && hitType && hitIdentify
}))

const filteredSeries = computed(() => seriesGroups.value.filter(g => {
  const hitKey = !keyword.value || String(g.title || '').toLowerCase().includes(keyword.value.toLowerCase())
  const hitStatus = !statusFilter.value || g.share_status === statusFilter.value
  // v0.7.0：一级状态栏——媒体类型 tab 支持 全部/剧集/电影/完结/追更
  let hitType = true
  if (typeTab.value === 'tv' || typeTab.value === 'movie') hitType = g.media_type === typeTab.value
  else if (typeTab.value === 'ending') hitType = g.finish_status === 'ended'
  else if (typeTab.value === 'returning') hitType = g.finish_status === 'returning'
  // 识别状态 tab（all / identified / unidentified）
  let hitIdentifyTab = true
  if (statusTab.value === 'identified') hitIdentifyTab = g.identify_status === 'identified'
  else if (statusTab.value === 'unidentified') hitIdentifyTab = g.identify_status !== 'identified'
  // 下拉筛选（保留）
  const hitTypeSel = !typeFilter.value || g.media_type === typeFilter.value
  const hitIdentifySel = !identifyFilter.value || g.identify_status === identifyFilter.value
  const hitFinish = !finishFilter.value || g.finish_status === finishFilter.value
  return hitKey && hitStatus && hitType && hitIdentifyTab && hitTypeSel && hitIdentifySel && hitFinish
}))
// v0.7.0：全选/全不选（作用于当前筛选结果）
const allSelected = computed(() => filteredSeries.value.length > 0 && filteredSeries.value.every(g => selectedSeries.value.includes(g.key)))
function toggleSelectAll() {
  selectedSeries.value = allSelected.value ? [] : filteredSeries.value.map(g => g.key)
}

// v2.1.6：资源库按 TMDB 分类分组。
// 依据后端 /api/library/series 的 primary_genre（TMDB 官方中文名）；
// 无分类的作品进「未分类」桶，**不丢弃**（未识别资源仍要能看到、仍可批量操作）。
// 排序：作品数倒序，同数按分类名——「未分类」永远排最后，不与真实分类抢位置。
const seriesGroupsByCategory = computed(() => {
  const buckets = new Map()
  for (const g of filteredSeries.value) {
    const name = String(g.primary_genre || '').trim() || UNGROUPED_NAME
    if (!buckets.has(name)) buckets.set(name, [])
    buckets.get(name).push(g)
  }
  const list = Array.from(buckets.entries()).map(([name, items]) => ({
    name,
    items,
    count: items.length,
    collapsed: collapsedGroups.value.includes(name),
  }))
  list.sort((a, b) => {
    if (a.name === UNGROUPED_NAME) return 1
    if (b.name === UNGROUPED_NAME) return -1
    if (b.count !== a.count) return b.count - a.count
    return a.name.localeCompare(b.name, 'zh-CN')
  })
  return list
})
// 一键折叠/展开所有分类
function toggleAllGroups(collapse) {
  collapsedGroups.value = collapse
    ? seriesGroupsByCategory.value.map(g => g.name)
    : []
}
function toggleGroup(name) {
  collapsedGroups.value = collapsedGroups.value.includes(name)
    ? collapsedGroups.value.filter(n => n !== name)
    : [...collapsedGroups.value, name]
}
const allGroupsCollapsed = computed(() => {
  const groups = seriesGroupsByCategory.value
  return groups.length > 0 && groups.every(g => g.collapsed)
})
function toggleSelect(key) {
  selectedSeries.value = selectedSeries.value.includes(key)
    ? selectedSeries.value.filter(k => k !== key)
    : [...selectedSeries.value, key]
}
function repIdOf(g) { return g.representative_id }
function durLabel(sec) {
  const n = Number(sec || 0)
  if (n === 0) return '永久'
  if (n === 86400) return '1天'
  if (n === 604800) return '7天'
  if (n === 2592000) return '30天'
  return Math.round(n / 86400) + '天'
}

const healthChecks = computed(() => [
  { label: '光鸭 OpenAPI 授权', ok: overview.value.auth === 'ok', hint: auth.value.authenticated ? (auth.value.flow || '已授权') : '尚未完成授权' },
  { label: '网页分享会话', ok: !!shareStatus.value.configured, hint: shareStatus.value.configured ? ('已配置 · ' + (shareStatus.value.source || '')) : '请到「光鸭授权」一键登录' },
  { label: 'TMDB 识别', ok: !!tmdbStatus.value.configured, hint: tmdbStatus.value.configured ? ('已配置 · ' + (tmdbStatus.value.mode || '') + (tmdbStatus.value.format_warning ? ' · 格式可疑' : '')) : '未配置 · 到「TMDB 设置」填密钥（可选，仅影响海报简介）' },
  { label: '队列 Worker', ok: true, hint: '每 30 秒轮询一次' }
])

// v0.9.20：主通道用会话 token；明文密码 header 仅保留给脚本/API 兼容，浏览器日常不再发送。
const headers = () => ({ 'X-Admin-Session': sessionToken.value, 'Content-Type': 'application/json' })
// FastAPI 的 detail 可能是字符串、dict 或 422 校验数组；直接 Error(obj) 会显示 [object Object]
function errText(value, fallback = '请求失败') {
  if (value == null || value === '') return fallback
  if (typeof value === 'string') return value
  if (Array.isArray(value)) return value.map(x => x?.msg || x?.message || JSON.stringify(x)).join('；')
  if (typeof value === 'object') return value.message || value.msg || value.detail || JSON.stringify(value)
  return String(value)
}
// v0.9.20：会话过期统一处理——清 token、回登录页、提示重新输入密码。
// 用 flag 防止并发请求同时弹多个提示。
let _expiryNotified = false
function forceSessionExpired(reason) {
  sessionToken.value = ''
  localStorage.removeItem(SESSION_KEY)
  authed.value = false
  password.value = ''
  if (!_expiryNotified) {
    _expiryNotified = true
    ElMessage.warning({ message: reason || '登录已过期，请重新输入密码', duration: 6000, showClose: true })
    setTimeout(() => { _expiryNotified = false }, 1500)
  }
}
async function api(path, opt = {}, timeoutMs = 0) {
  // v0.7.1：全量重扫可能耗时数分钟，允许单独放宽超时（AbortSignal 不影响其它请求）
  const ctl = timeoutMs ? new AbortController() : null
  const timer = ctl ? setTimeout(() => ctl.abort(), timeoutMs) : null
  try {
    const r = await fetch('/api' + path, { ...opt, ...(ctl ? { signal: ctl.signal } : {}),
      headers: { ...headers(), ...(opt.headers || {}) } })
    const d = await r.json().catch(() => ({}))
    if (!r.ok) {
      const detail = errText(d.detail ?? d.message, r.statusText)
      // v0.9.20：服务端判定会话过期/无效 → 强制回登录页；排除登录接口自身的密码错误
      if (r.status === 401 && path !== '/auth/login' &&
          (detail.includes('登录已过期') || detail.includes('会话无效'))) {
        forceSessionExpired(detail)
      }
      throw Error(detail)
    }
    return d
  } catch (e) {
    if (ctl && ctl.signal.aborted) throw Error('扫描仍在进行中，请稍后刷新资源库查看结果')
    throw e
  } finally { if (timer) clearTimeout(timer) }
}
async function load() {
  // 阶段三 D-07：无依赖的纯 GET 并行拉取（Promise.allSettled，单个失败不影响其它）。
  // 赋值语义与降级口径与改造前逐条 await 完全一致；内部有依赖的 load* 函数保持原顺序。
  const keys = ['overview', 'scan-task', 'resources', 'library/series', 'review', 'queue',
    'templates', 'templates/presets', 'audit', 'diagnostics', 'telegram/config',
    'queue/schedule', 'mirror/config', 'mirror/status', 'scan/batches?limit=5',
    'interaction/config', 'push/routes', 'discover/chats', 'interaction/status',
    'contributions?limit=50', 'media-requests?limit=50', 'logs/overview']
  const settled = await Promise.allSettled(keys.map(k => api('/' + k)))
  const v = i => settled[i]?.status === 'fulfilled' ? settled[i].value : null
  overview.value = v(0) || overview.value
  task.value = v(1) || task.value
  resources.value = v(2) || resources.value
  seriesGroups.value = v(3) || []
  reviews.value = v(4) || reviews.value
  queue.value = v(5) || queue.value
  templates.value = v(6) || templates.value
  tplPresets.value = v(7) || []
  audit.value = v(8) || audit.value
  diagnostics.value = v(9) || { blockers: [] }
  const tg = v(10); if (tg) telegramBase.value = tg.base_url || tg.default || ''
  queueSchedule.value = v(11) || queueSchedule.value
  mirrorCfg.value = v(12) || mirrorCfg.value
  mirrorStatus.value = v(13) || mirrorStatus.value
  scanBatches.value = v(14) || []
  interactionCfg.value = v(15) || interactionCfg.value
  // v0.8.0：每个新模块独立加载，任一接口异常不影响主界面（沿用既有约定）
  try { scanInterval.value = task.value.interval_minutes || 0; scanCustom.value = !task.value.interval_minutes } catch (e) {}
  // v0.9.16 #278：初次加载即同步 Bot 下拉来源（bot_options 来自配置接口）
  syncDiscBots()
  const pr = v(16)
  if (pr) {
    pushCfg.value = { bot: pr.bot || null, max_routes: pr.max_routes || 10 }
    pushRoutes.value = (pr.routes && pr.routes.length)
      ? pr.routes.map(r => ({ dir_id: r.dir_id || '', channel_id: r.channel_id || '', group_id: r.group_id || '',
        targets: (r.targets || []).map(t => ({ kind: t.kind || 'channel', chat_id: t.chat_id || '', name: t.name || '' })),
        note: r.note || '' }))
      : [{ dir_id: '', channel_id: '', group_id: '', targets: [], note: '' }]
  }
  // v0.9.14 #258：自动发现群组 / 统一推送目标
  const dc = v(17); if (dc) discoverChats.value = dc.chats || []
  interactionStatus.value = v(18) || interactionStatus.value
  contributions.value = v(19) || []
  mediaRequests.value = v(20) || []
  const lg = v(21)
  if (lg) {
    logCfg.value = lg.config || logCfg.value
    logStats.value = lg.tables || []
    logOverview.value = { total_rows: lg.total_rows || 0, total_pruneable: lg.total_pruneable || 0,
      container_log: lg.container_log || '' }
  }
  // 分享中心：统一走新数据体系（/api/sc/*），失败不影响主界面
  try { await loadSc() } catch (e) {}
  // 内部有依赖的函数保持原顺序（不再与上面的 GET 混排）
  try { await loadTgExtra() } catch (e) {}
  try { await loadPushStats() } catch (e) {}
  await loadAuth()
  await loadTmdb()
  await loadAi()
}
async function loadLogs() {
  try {
    const d = await api('/logs/overview')
    logCfg.value = d.config || logCfg.value
    logStats.value = d.tables || []
    logOverview.value = { total_rows: d.total_rows || 0, total_pruneable: d.total_pruneable || 0,
      container_log: d.container_log || '' }
  } catch (e) {}
}
async function saveLogCfg() {
  logBusy.value = true
  try {
    const body = { retention_days: Number(logCfg.value.retention_days) || 30,
      max_rows: Number(logCfg.value.max_rows) || 5000, auto_prune: !!logCfg.value.auto_prune }
    await api('/logs/config', { method: 'POST', body: JSON.stringify(body) })
    ElMessage.success('日志保留策略已保存')
    await loadLogs()
  } catch (e) { ElMessage.error(e.message) }
  finally { logBusy.value = false }
}
async function pruneLogs() {
  logBusy.value = true
  try {
    const d = await api('/logs/prune', { method: 'POST' })
    ElMessage.success(`已按策略清理 ${d.deleted || 0} 条`)
    await loadLogs(); await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { logBusy.value = false }
}
async function clearLogs() {
  try {
    await ElMessageBox.confirm(
      '将清空操作审计、扫描批次、镜像任务、版本留痕、已处理分发事件、已推送队列历史与被替代文件记录；待推送队列、未完成批次和业务映射会保留。确认继续？',
      '清空全部日志', { confirmButtonText: '确认清空', cancelButtonText: '返回', type: 'warning' })
  } catch (e) { return }
  logBusy.value = true
  try {
    const d = await api('/logs/clear', { method: 'POST', body: JSON.stringify({ confirm: true }) })
    ElMessage.success(`已清空 ${d.deleted || 0} 条日志`)
    await loadLogs(); await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { logBusy.value = false }
}
async function loadAuth() {
  // 三个请求独立加载：任一端点报错不再拖垮其余状态展示
  try { auth.value = await api('/auth/status') } catch (e) {}
  try { shareStatus.value = await api('/share/status') } catch (e) {}
  await loadSms()
}
async function login() {
  const pwd = password.value
  if (!pwd) { ElMessage.warning('请输入管理员密码'); return }
  try {
    // v0.9.20：用密码换会话 token（登录端点无需鉴权，直接 fetch 避免 headers 带旧 token）
    const r = await fetch('/api/auth/login', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password: pwd }) })
    const d = await r.json().catch(() => ({}))
    if (!r.ok) throw Error(errText(d.detail ?? d.message, r.statusText))
    sessionToken.value = d.token
    localStorage.setItem(SESSION_KEY, d.token)   // 只存 token，绝不写明文密码
    password.value = ''                          // 立即清空登录框里的密码
    await load()
    authed.value = true
    sessionRemaining.value = d.idle_seconds || 0
    ElMessage.success(`已登录（闲置 ${Math.round((d.idle_seconds || 7200) / 60)} 分钟自动退出）`)
  } catch (e) { ElMessage.error(e.message) }
}
async function logout() {
  // 主动登出：通知服务端立即失效该会话，再清本地 token
  try { await api('/auth/logout', { method: 'POST' }) } catch (e) {}
  authed.value = false
  sessionToken.value = ''
  sessionRemaining.value = 0
  password.value = ''
  localStorage.removeItem(SESSION_KEY)
  localStorage.removeItem('ya_tg_password')  // 清理历史版本遗留的明文密码
}
// v0.9.20：定时轮询会话状态（只读、不续期），剩余时间归零即强制重新登录
async function pollSession() {
  if (!authed.value || !sessionToken.value) return
  try {
    const s = await api('/auth/session')
    sessionRemaining.value = s.remaining_seconds || 0
  } catch (e) {
    // 过期/无效已由 api() 统一处理（forceSessionExpired），这里只兜底清理本地态
    if (!authed.value) { sessionToken.value = ''; localStorage.removeItem(SESSION_KEY) }
  }
}
function fmtRemaining(sec) {
  sec = Number(sec || 0)
  if (sec <= 0) return '即将过期'
  const h = Math.floor(sec / 3600), m = Math.ceil((sec % 3600) / 60)
  return h > 0 ? `${h}小时${m}分` : `${m}分钟`
}
async function saveTask() {
  // v0.8.0：默认用预设间隔按钮，只有开启「自定义 Cron」才提交表达式
  // v0.8.3：根目录 ID 已从界面和提交体里完全移除，后端统一引用镜像同步里的 B 库 ID
  const body = { interval_minutes: scanCustom.value ? 0 : Number(scanInterval.value || 0),
    cron: scanCustom.value ? (task.value.cron || '') : '',
    enabled: !!task.value.enabled }
  try { await api('/scan-task', { method: 'PUT', body: JSON.stringify(body) }); ElMessage.success('扫描计划已保存'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
function pickScanInterval(v) { scanInterval.value = v; scanCustom.value = false }
const intervalLabel = v => v >= 1440 ? '每天' : (v >= 60 ? `每 ${v / 60} 小时` : `每 ${v} 分钟`)
// v0.8.7：三个入口统一「读取最新 B 库内容 → 按实际内容重建面板」
// 顶部「刷新」= /library/refresh，「立即扫描」= /scan-now，「重新扫描库」= /library/rescan
// v0.9.10：三个扫描入口统一走后台异步任务（asyncScanRunning 见上方）。
function reconcileTip(d) {
  const rc = d.reconcile || {}
  if (rc.skipped) return `对账已跳过（${rc.skipped}）`
  const empty = rc.empty_seen_allowed ? '（B 库为空，面板已按实际内容清空）' : ''
  return `移除 ${rc.removed || 0} 项、取消分享 ${rc.shares_cancelled || 0} 个${empty}`
}
// v0.9.10：后台异步扫描——立即返回，轮询状态，彻底避开 fetch 超时 / 页面卡死。
const asyncScanRunning = ref(false), asyncScanStage = ref('')
async function runAsyncScan(mode, label) {
  if (asyncScanRunning.value) { ElMessage.warning('已有扫描任务在运行'); return }
  asyncScanRunning.value = true
  asyncScanStage.value = '启动'
  try {
    await api('/scan/async?mode=' + mode, { method: 'POST' })
    ElMessage.info(`${label}已在后台开始，完成后自动刷新页面，可继续使用其它功能`)
    let ticks = 0
    while (true) {
      await new Promise(r => setTimeout(r, 1500))
      const s = await api('/scan/async')
      asyncScanStage.value = s.stage || ''
      if (!s.running) {
        if (s.error) { ElMessage.error(`${label}失败：${s.error}`); break }
        const d = s.result || {}
        const parts = [`${label}完成：${d.files || 0} 个文件`, reconcileTip(d)]
        const idf = d.identify || {}
        if (idf.identified) parts.push(`识别 ${idf.identified} 项`)
        ElMessage.success(parts.join(' · '))
        await load()
        break
      }
      if (++ticks > 2400) { ElMessage.warning('扫描耗时较长，仍在后台运行，请稍后刷新查看'); break }
    }
  } catch (e) { ElMessage.error(e.message) }
  finally { asyncScanRunning.value = false; asyncScanStage.value = '' }
}
async function refreshAll() { await runAsyncScan('refresh', '刷新') }
async function scan() { await runAsyncScan('scan', '扫描') }
// ---------- A/B 镜像 ----------
async function saveMirror() {
  mirrorSaving.value = true
  try {
    // 清空数字框会给出 null，兜底为 1.0 秒，避免后端 422
    const body = { ...mirrorCfg.value,
      file_idle_seconds: Number(mirrorCfg.value.file_idle_seconds) || 1.0,
      max_works_per_run: Number(mirrorCfg.value.max_works_per_run) || 0 }
    await api('/mirror/config', { method: 'POST', body: JSON.stringify(body) })
    ElMessage.success('镜像设置已保存')
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { mirrorSaving.value = false }
}
// v0.8.5：mode = 'auto'（默认，遵循 auto_identify 开关）| 'force' | 'skip'
// 不再把「立即同步」硬编码成不识别；同步完成后会刷新入库 → 识别 → 只推本轮识别成功且有海报的影视
async function runMirror(mode = 'auto') {
  mirrorBusy.value = true
  try {
    const parts = []
    if (mode !== 'auto') parts.push(`post_identify=${mode === 'force' ? 'true' : 'false'}`)
    if (mirrorStrict.value) parts.push('strict=true')  // 阶段二路线B：严格顺序引擎
    const q = parts.length ? '?' + parts.join('&') : ''
    const d = await api(`/mirror/run${q}`, { method: 'POST' }, 300000)
    const extra = []
    if (d.dirs_created) extra.push(`新建目录 ${d.dirs_created}`)
    if (d.dirs_fallback) extra.push(`已降级标准名 ${d.dirs_fallback} 次`)
    if (d.blocked) extra.push(`${d.blocked} 部待建目录（见下方清单）`)
    if (d.retries) extra.push(`重试 ${d.retries} 个未成功文件`)
    if (d.dir_errors?.length) extra.push(`首个建目录错误：${String(d.dir_errors[0]).slice(0, 140)}`)
    // v0.8.4：文件复制失败必须显式提示，不能只显示「同步 N 部」让人误以为成功
    if (d.file_errors?.length) extra.push(`首个文件错误：${String(d.file_errors[0]).slice(0, 160)}`)
    else if (!d.dir_errors?.length && d.errors?.length) extra.push(d.errors[0])
    const p = d.pipeline
    if (p) extra.push(`自动入库 ${p.files || 0} 文件 · 识别成功 ${p.identify?.identified || 0} · 入队 ${p.queued || 0} · 内容变化待处理 ${p.changes_pending || 0}`)
    else if (d.pipeline_enabled === false) extra.push('本次仅同步，未刷新入库')
    // v0.8.8：改名统计必须把「跳过原因 / 整体异常」也带出来，否则
    // 「改名 0 · 待翻译 0 · 失败 0」三个零看不出到底是跳过、无中文还是改名没执行
    const r = d.rename
    if (r) {
      const rp = [`改名 ${r.renamed || 0} · 已达标跳过 ${r.skipped || 0} · 无中文跳过 ${r.ascii || 0} · 待翻译作品 ${r.untranslated || 0} · 改名失败 ${r.failed || 0}`]
      // v0.8.9：TMDB 查不到英文名时直接直译，必须让用户看得到走了哪条兜底
      if (r.literal_ai || r.literal_pinyin) rp.push(`无官方英文名·直译 ${r.literal_ai || 0} 部 · 拼音兜底 ${r.literal_pinyin || 0} 部`)
      if (r.no_rows) rp.push(`无待改名文件 ${r.no_rows} 部`)
      // v0.8.9：长剧集分批是正常进度，单独提示「下一轮自动续改」，别让用户以为改失败
      if (r.notices?.length) rp.push(String(r.notices[0]).slice(0, 160))
      if (r.errors?.length) rp.push(`首个改名错误：${String(r.errors[0]).slice(0, 140)}`)
      extra.push(rp.join(' · '))
    } else if (d.rename_enabled === false) extra.push('本次未自动改名（开关关闭）')
    else extra.push('本次未自动改名（未返回统计）')
    const msg = `同步 ${d.media || 0} 部：新增 ${d.new || 0} · 重试 ${d.retries || 0} · 变化 ${d.changed || 0} · 未变 ${d.unchanged || 0} · 完成 ${d.done || 0} · 复制失败 ${d.failed || 0} · 待确认 ${d.unknown || 0}` + (extra.length ? ' · ' + extra.join(' · ') : '')
    if (d.dir_errors?.length || d.failed || r?.failed || r?.errors?.length) ElMessage.warning(msg)
    else ElMessage.success(msg)
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { mirrorBusy.value = false }
}
// v0.8.6：存量 B 库单集/电影文件按英文剧名批量改名，不重新复制、不动 A 库
async function renameMirrorFiles(force = false) {
  mirrorBusy.value = true
  try {
    const d = await api(`/mirror/rename?force=${force ? 'true' : 'false'}`, { method: 'POST' }, 300000)
    const msg = `改名完成 ${d.renamed || 0} 个文件：已完成跳过 ${d.skipped || 0} · 无中文跳过 ${d.ascii || 0} · 待翻译作品 ${d.untranslated || 0} · 失败 ${d.failed || 0}` + ((d.literal_ai || d.literal_pinyin) ? ` · 无官方英文名·直译 ${d.literal_ai || 0} 部 · 拼音兜底 ${d.literal_pinyin || 0} 部` : '')
    const first = d.errors?.length ? ` · 首个错误：${String(d.errors[0]).slice(0, 140)}` : ''
    const notice = d.notices?.length ? ` · ${String(d.notices[0]).slice(0, 160)}` : ''
    if (d.failed || d.untranslated) ElMessage.warning(msg + notice + first)
    else ElMessage.success(msg + notice)
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { mirrorBusy.value = false }
}
async function pollMirrorTasks() {
  mirrorBusy.value = true
  try {
    const d = await api('/mirror/tasks/poll', { method: 'POST' }, 120000)
    const msg = `轮询 ${d.tasks} 个任务，确认完成 ${d.done || 0} 个，失败 ${d.failed || 0} 个，待确认 ${d.still_unknown || 0} 个`
    if (d.failed) ElMessage.warning(msg + '（失败文件会在下次同步自动重试）')
    else ElMessage.success(msg)
    await load()
  }
  catch (e) { ElMessage.error(e.message) }
  finally { mirrorBusy.value = false }
}
// v0.9.17.2：勾选失败目录定向重建（只 create_dir 选中行，不扫 A 库）
async function retryFailedDirs() {
  const ids = (missingDirSelected.value || []).slice()
  if (!ids.length) { ElMessage.info('请先勾选要重建的失败目录'); return }
  const ok = await confirmBox(
    `将对勾选的 ${ids.length} 个失败目录重新发起 create_dir。\n\n` +
    '只处理选中行、不扫描 A 源头库；已存在的目录不会重复创建。重建成功后下轮同步会自动映射其下文件。')
  if (!ok) return
  mirrorBusy.value = true
  try {
    const d = await api('/mirror/retry/dirs', { method: 'POST', body: JSON.stringify({ ids }) })
    ElMessage.success(`目录重建完成：成功 ${d.recreated || 0} · 无需重建 ${d.already_ok || 0} · 失败 ${d.failed || 0}`)
    if (d.failed) {
      const bad = (d.details || []).filter(x => /失败|无法|跳过/.test(x))
      if (bad.length) ElMessage.warning('部分目录未成功：' + bad.slice(0, 3).join('；'))
    }
    missingDirSelected.value = []
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { mirrorBusy.value = false }
}
// v0.9.17.2：勾选失败文件定向重发复制（只 walk 所属作品，不扫全库）
async function retryFailedFiles() {
  const ids = (fileErrSelected.value || []).slice()
  if (!ids.length) { ElMessage.info('请先勾选要重试的失败文件'); return }
  const ok = await confirmBox(
    `将对勾选的 ${ids.length} 个失败文件定向重发复制。\n\n` +
    '只扫描这些文件所属作品的 A 库目录（不扫整个源头库）；' +
    'B 库已存在的会直接回填、不重复复制。复制成功后自动走入库→识别→推送入队。')
  if (!ok) return
  mirrorBusy.value = true
  try {
    const d = await api('/mirror/retry/files', { method: 'POST', body: JSON.stringify({ ids }) })
    ElMessage.success(`文件重试完成：成功 ${d.done || 0} · B库已存在回填 ${d.reused || 0} · 失败 ${d.failed || 0}`)
    if (d.failed) {
      const bad = (d.details || []).filter(x => /失败|无法|未确认|未就绪/.test(x))
      if (bad.length) ElMessage.warning('部分文件未成功：' + bad.slice(0, 3).join('；'))
    }
    fileErrSelected.value = []
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { mirrorBusy.value = false }
}
// v0.9.6：清空「英文改名待处理」清单——把英文名为空的作品标记 ignored，
// 从待处理清单消失；不改文件、不删映射，点「重新解析英文名并改名」可恢复。
// v2.1.4：二次确认明确告知**后果**——此前文案只说「日后可恢复」，
// 用户在真机上误以为「清空 = 永久不再翻译」，实测这批作品被永久拉黑。
// 现在文案讲清三件事：① 只影响本轮跳过、不删任何东西；
// ② A 库若出现新文件会自动恢复解析；③ 也可随时手动强制恢复。
async function clearUntranslated() {
  const n = mirrorStatus.value.untranslated_works?.length || 0
  if (!n) { ElMessage.info('当前没有待处理项'); return }
  const ok = await confirmBox(
    `将把 ${n} 部「英文名为空」的作品标记为已忽略，从待处理清单移除。\n\n` +
    '【会发生什么】这批作品在本轮同步中不再触发英文名解析，文件保持原中文名。\n' +
    '【不会发生什么】不删映射、不动 A/B 库文件、不影响入库、分享与推送。\n' +
    '【怎么恢复】① 若这些作品之后在 A 库出现新集，系统会自动恢复解析；' +
    '② 也可随时点「重新解析英文名并改名」立即全部恢复。')
  if (!ok) return
  mirrorBusy.value = true
  try {
    const d = await api('/mirror/rename/untranslated/clear', { method: 'POST' })
    ElMessage.success(`已清空 ${d.cleared || 0} 条待处理项` +
      (d.note ? `。${d.note}` : ''))
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { mirrorBusy.value = false }
}
// v0.9.6：彻底删除「已清理待重建」的僵尸目录映射（带 [pruned] 标记的 missing 行）。
async function clearPrunedMappings() {
  const n = mirrorStatus.value.counts?.dirs_pruned_zombie || 0
  if (!n) { ElMessage.info('当前没有可清空的已清理映射'); return }
  const ok = await confirmBox(
    `将物理删除 ${n} 条「已清理待重建」的目录映射记录（B 库已不存在的僵尸行）。\n\n` +
    '只删这类 [pruned] 记录，不碰真正建目录失败的映射，也不删资源 / 分享 / 推送队列。' +
    'A 库仍是权威源：若目录还在，下一轮「立即同步」会自动重建映射。')
  if (!ok) return
  mirrorBusy.value = true
  try {
    const d = await api('/mirror/mappings/clear-pruned', { method: 'POST' })
    ElMessage.success(`已清空 ${d.removed || 0} 条无效映射记录`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { mirrorBusy.value = false }
}
// v0.9.10：清理影视映射清单的无效行——口径改为「核对 B 库，删除网盘文件已整部删除的历史行」。
async function clearInvalidMedia() {
  if (!mirrorStatus.value.media?.length) { ElMessage.info('影视映射清单为空，无需清理'); return }
  const ok = await confirmBox(
    '将只读核对 B 库真实内容，把网盘上已整部删除文件的历史影视映射行清理掉。\n\n' +
    '只有确认列目录完整可信、且 B 库确实一个文件都没有才清理；接口异常或有文件的行一律跳过。' +
    '同时清理本地失败/阻断且无已同步文件的僵尸行。\n\n' +
    '只删 A→B 映射登记，不删资源 / 分享 / 推送队列 / B 库文件。')
  if (!ok) return
  mirrorBusy.value = true
  try {
    const d = await api('/mirror/media/clear-invalid', { method: 'POST' }, 300000)
    const parts = [`已核对 B 库 ${d.b_checked || 0} 部，删除 ${d.removed_media || 0} 条无效影视映射（${d.removed_files || 0} 个文件映射）`]
    if (d.skipped_has_files) parts.push(`${d.skipped_has_files} 部 B 库仍有文件，已保留`)
    if (d.skipped_unverified > 0) parts.push(`${d.skipped_unverified} 部列目录不可信，已跳过`)
    ElMessage.success(parts.join('；'))
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { mirrorBusy.value = false }
}
// v0.8.9：清理 B 库已不存在的文件 / 目录映射。只重置映射，不删资源与分享。
async function pruneMirrorMappings() {
  const ok = await confirmBox(
    '将只读核对 B 库实际内容，清理其中已不存在的文件 / 目录映射。\n\n' +
    '只重置映射（保留 A 库文件名与指纹），下一轮「立即同步」会自动补复制并改名；' +
    '不会删除资源、分享链接或推送队列。\n' +
    '列目录不可信或整部作品 B 库为空时一律跳过，绝不误清。')
  if (!ok) return
  mirrorBusy.value = true
  try {
    const d = await api('/mirror/mappings/prune', { method: 'POST' }, 300000)
    const parts = [`已清理 ${d.files_pruned || 0} 个文件映射 · ${d.dirs_pruned || 0} 个目录映射`]
    const skipped = (d.media_skipped_unverified || 0) + (d.dirs_skipped_unverified || 0)
    if (skipped) parts.push(`列目录不可信跳过 ${skipped} 项`)
    if (d.media_skipped_empty) parts.push(`${d.media_skipped_empty} 部 B 库无文件已跳过，请点「立即同步」补复制`)
    if (d.errors?.length) parts.push(`首个错误：${String(d.errors[0]).slice(0, 120)}`)
    if (d.errors?.length) ElMessage.warning(parts.join(' · '))
    else ElMessage.success(parts.join(' · '))
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { mirrorBusy.value = false }
}
// ---------- 用户互动 ----------
async function savePushRoutes() {
  // v0.9.6.1：交互 Bot / 目录路由 / 使用限制 / 打赏统一保存，避免填了使用限制却没保存。
  const routes = (pushRoutes.value || []).map(r => {
    const targets = (r.targets || []).map(t => ({
      kind: t.kind || 'channel', chat_id: String(t.chat_id || '').trim(), name: String(t.name || '').trim()
    })).filter(t => t.chat_id)
    return {
      dir_id: String(r.dir_id || '').trim(),
      channel_id: String(r.channel_id || '').trim(),
      group_id: String(r.group_id || '').trim(),
      targets,
      note: String(r.note || '').trim()
    }
  }).filter(r => r.channel_id || r.group_id || r.targets.length)
  const body = { routes }
  const token = String(pushBotToken.value || '').trim()
  if (token) body.bot_token = token
  pushSaving.value = true
  let routeInfo = ''
  try {
    // 路由为空也允许保存使用限制 / 打赏，不让「至少一条路由」挡住其它配置
    if (routes.length) {
      const d = await api('/push/routes', { method: 'POST', body: JSON.stringify(body) })
      pushBotToken.value = ''
      routeInfo = ` · ${d.count} 条路由`
    } else {
      routeInfo = ' · 未改路由'
    }
    await postInteractionConfig()
    await api('/reward/config', { method: 'POST', body: JSON.stringify(rewardCfg.value || { code: '' }) })
    ElMessage.success(`分发与互动配置已保存（Bot + 白名单 + 使用限制 + 打赏）${routeInfo}`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { pushSaving.value = false }
}
async function clearPushBotToken() {
  if (!pushCfg.value.bot?.token_masked) { ElMessage.info('当前没有已配置的 Bot Token'); return }
  const ok = await confirmBox('确认清除 Bot Token？清除后 Bot 停用、测试推送不可用；目录路由 / 频道 / 投稿配置全部保留，重新填 Token 即可恢复。')
  if (!ok) return
  pushSaving.value = true
  try {
    await api('/push/bot/token/clear', { method: 'POST' })
    pushBotToken.value = ''
    ElMessage.success('Bot Token 已清除，Bot 已停用')
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { pushSaving.value = false }
}
async function syncBotCommands(clear = false) {
  const tip = clear
    ? '确认清空 Telegram 私聊指令栏？清空后输入框「/」不再弹出任何指令下拉；Reply 键盘菜单不受影响。'
    : '确认把 Telegram 私聊指令栏同步为当前 Ya-TG 指令？会覆盖其它项目残留的旧指令。'
  const ok = await confirmBox(tip)
  if (!ok) return
  pushSaving.value = true
  try {
    const d = await api('/bot/commands/menu', { method: 'POST', body: JSON.stringify({ clear }) })
    ElMessage.success(clear ? '指令栏已清空' : `指令栏已同步（${d.count} 条指令，重启 Telegram 后生效）`)
  } catch (e) { ElMessage.error(e.message) }
  finally { pushSaving.value = false }
}
async function testPushRoute(r, explicitChatId = '') {
  // v0.9.8：一条路由的频道 / 群各有独立测试按钮，传明确 chat_id；兼容旧调用（取 chat_id）
  const chatId = String(explicitChatId || r.chat_id || '').trim()
  const bot = pushCfg.value.bot
  if (!bot) { ElMessage.warning('请先在上方填写 Bot Token 并保存'); return }
  if (!chatId) { ElMessage.warning('请先填写该目标的 ID 并保存'); return }
  try {
    const d = await api(`/bots/${bot.id}/test-message`, { method: 'POST', body: JSON.stringify({ chat_id: chatId }) })
    ElMessage[d.ok ? 'success' : 'error'](d.message || (d.ok ? '测试消息已发送' : '测试失败'))
  } catch (e) { ElMessage.error(e.message) }
}
async function postInteractionConfig() {
  const c = interactionCfg.value
  const body = { contribution_enabled: !!c.contribution_enabled, donation_enabled: !!c.donation_enabled,
    tgto_base_url: c.tgto_base_url || '', tgto_scene: c.tgto_scene || 'ENV_FILTER_GUANGYA',
    // v0.9.13 #254：TgtoDrive 白名单规则 a/b/c（求片联动）
    tgto_rule_resolution: c.tgto_rule_resolution || '4k',
    tgto_rule_effect: c.tgto_rule_effect || 'all',
    tgto_rule_required_keyword: c.tgto_rule_required_keyword || '',
    // v0.9.19：投稿转存配置（光鸭原生 API → 指定目录 ID）
    contribution_restore_enabled: !!c.contribution_restore_enabled,
    contribution_restore_target_dir_id: c.contribution_restore_target_dir_id || '',
    donation_image_url: c.donation_image_url || '', donation_text: c.donation_text || '',
    donation_button_text: c.donation_button_text || '打赏支持',
    // v0.9.4：白名单由「推送目标」启用记录决定，不再单独提交 ID 列表
    interaction_bot_id: String(c.interaction_bot_id || ''),
    interaction_bot_username: String(c.interaction_bot_username || ''),
    bot_restrict_enabled: !!c.bot_restrict_enabled, bot_restrict_cache_seconds: Number(c.bot_restrict_cache_seconds) || 0,
    bot_gate_mode: c.bot_gate_mode || 'whitelist',
    bot_required_channels: c.bot_required_channels || [], bot_required_groups: c.bot_required_groups || [],
    // v0.9.5：使用限制跳转 + 推送消息频道/群跳转
    bot_required_channel_link: c.bot_required_channel_link || '', bot_required_group_link: c.bot_required_group_link || '',
    tg_channel_link: c.tg_channel_link || '', tg_group_link: c.tg_group_link || '',
    tg_bot_link: c.tg_bot_link || '',
    // 自定义指令不含内置四项，内置由后端固定注入
    bot_commands: (c.bot_commands || []).filter(x => !x.builtin).map(x => ({ command: x.command, description: x.description, reply: x.reply })) }
  // 密钥字段只有用户实际输入才提交，避免把返回后的掩码写回凭证
  if (c.tgto_api_key) body.tgto_api_key = c.tgto_api_key
  await api('/interaction/config', { method: 'POST', body: JSON.stringify(body) })
}
async function sendBroadcastNotice() {
  // 管理员侧群发通知：只发目录路由表里的频道 / 群聊，不私聊用户、不进队列。
  const text = String(broadcastText.value || '').trim()
  if (!text) { ElMessage.warning('请先填写通知内容'); return }
  broadcastSending.value = true
  try {
    const d = await api('/broadcast/notify', { method: 'POST', body: JSON.stringify({ text }) })
    ElMessage[d.ok ? 'success' : 'error'](d.message || (d.ok ? '通知已发送' : '通知发送失败'))
  } catch (e) { ElMessage.error(e.message) }
  finally { broadcastSending.value = false }
}
async function saveInteraction() {
  interactionSaving.value = true
  try {
    await postInteractionConfig()
    ElMessage.success('互动设置已保存（密钥加密存储）')
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { interactionSaving.value = false }
}
async function saveJumpLinks() {
  interactionSaving.value = true
  try {
    await postInteractionConfig()
    ElMessage.success('已保存；推送消息正文将统一追加频道/群/机器人三行文字链接，留空的不显示，按钮只保留「获取资源」')
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { interactionSaving.value = false }
}
async function testTgto() {
  tgtoTesting.value = true
  try { const d = await api('/tgto/test', { method: 'POST' }, 30000); ElMessage.success(`${d.message}（现有规则 ${d.rules} 条，未写入）`) }
  catch (e) { ElMessage.error(e.message) }
  finally { tgtoTesting.value = false }
}
async function saveTgto() {
  // v0.9.13 #254：TgtoDrive 板块独立保存按钮（只提交联动配置，复用互动配置接口）
  interactionSaving.value = true
  try {
    await postInteractionConfig()
    ElMessage.success('TgtoDrive 联动配置已保存')
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { interactionSaving.value = false }
}
async function saveContributionRestore() {
  // v0.9.19：投稿转存模块独立保存按钮（复用互动配置接口，只改转存两项）
  interactionSaving.value = true
  try {
    await postInteractionConfig()
    ElMessage.success('投稿转存配置已保存')
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { interactionSaving.value = false }
}
async function handleContribution(id, status) {
  let note = ''
  if (status === 'rejected') {
    try {
      const r = await ElMessageBox.prompt('可填写拒绝原因（会反馈给投稿用户），留空则只通知未通过', '拒绝投稿', {
        confirmButtonText: '确认拒绝', cancelButtonText: '取消', inputPlaceholder: '例如：资源已在库中'
      })
      note = String(r.value || '').trim()
    } catch (e) { return }
  }
  try {
    const d = await api(`/contributions/${id}/handle`, { method: 'POST', body: JSON.stringify({ status, note }) })
    // v0.9.19：审核状态与转存状态分离——通过即审核成功，光鸭转存结果单独如实回显
    if (status === 'approved' && d && 'restore_ok' in d) {
      const ok = !!d.restore_ok
      const msg = ok
        ? `投稿已通过，光鸭转存完成（${d.restore_status || 'success'}）`
        : `投稿已通过（审核不受影响）；转存未完成：${d.restore_result || d.forward_info || '未知原因'}，可在转存状态列手动重试`
      ElMessage[ok ? 'success' : 'warning']({ message: msg, duration: 6500, showClose: true })
    } else ElMessage.success('已处理')
    await load()
  }
  catch (e) { ElMessage.error(e.message) }
}
// v0.9.19：投稿转存状态映射 + 手动重试 + 转存记录查看（光鸭原生 API）
function restoreStatusTag(s) { return ({ success: 'success', restoring: 'warning', pending: 'info', retry_wait: 'warning', failed: 'danger' })[s] || 'info' }
async function retryContributionRestore(row) {
  try {
    const d = await api(`/contributions/${row.id}/restore/retry`, { method: 'POST' })
    ElMessage[d.ok ? 'success' : 'warning']({ message: d.ok ? `已重试转存：${d.info || ''}` : `重试未完成：${d.info || ''}`, duration: 6000, showClose: true })
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function showContributionRestore(row) {
  try {
    const d = await api(`/contributions/${row.id}/restore`)
    if (!d.exists) { ElMessage.info('该投稿尚无转存记录'); return }
    const hist = (d.history || []).map(h => `· ${h.action} ${h.ts || ''} ${h.detail || ''}`).join('\n') || '（无）'
    await ElMessageBox.alert(
      `状态：${d.status_label}（${d.status}）\n尝试次数：${d.attempts}\n转存时间：${d.restored_at || '—'}\n错误：${d.error || '—'}\n核对结果：${d.verify_note || '—'}\n原始响应：${d.last_response || '—'}\n\n转存记录：\n${hist}`,
      '转存记录', { confirmButtonText: '关闭', customClass: 'restore-detail-box' })
  } catch (e) { if (e !== 'cancel' && e !== 'close') ElMessage.error(e.message || String(e)) }
}
async function handleMediaRequest(row, review) {
  let note = ''
  if (review === 'rejected') {
    try {
      const r = await ElMessageBox.prompt('可填写拒绝原因（会反馈给求片用户），留空则只通知未采纳', '拒绝求片', {
        confirmButtonText: '确认拒绝', cancelButtonText: '取消', inputPlaceholder: '例如：版权原因暂不加'
      })
      note = String(r.value || '').trim()
    } catch (e) { return }
  }
  try {
    const d = await api(`/media-requests/${row.id}/handle`, { method: 'POST', body: JSON.stringify({ review_status: review, note }) })
    if (review === 'adopted') {
      // v0.9.17 #280：采纳结果分成功/失败展示真实同步回执——杜绝"提示已采纳、参数却没进 TgtoDrive"的黑洞
      if (d && d.sync_ok === false) {
        ElMessage.error({ message: `已标记采纳，但关键词未写入 TgtoDrive：${d.sync_note || d.sync || '未知原因'}`, duration: 8000, showClose: true })
      } else {
        const a = d && d.applied
        const parts = a ? [a.resolution, a.effect, a.required_keyword ? ('必含:' + a.required_keyword) : '', a.year].filter(Boolean) : []
        ElMessage.success({ message: `已采纳，关键词已写入 TgtoDrive${parts.length ? '（' + parts.join(' · ') + '）' : ''}`, duration: 6000, showClose: true })
      }
    } else {
      ElMessage.success(review === 'rejected' ? '已拒绝，已通知用户' : '已重置为待处理')
    }
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function deleteMediaRequest(row) {
  const ok = await confirmBox(`确认删除求片「${row.title}」？将同时移除本项目写入的 TgtoDrive 白名单规则（不影响 TgtoDrive 原有关键词）。`)
  if (!ok) return
  try { await api(`/media-requests/${row.id}`, { method: 'DELETE' }); ElMessage.success('已删除'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
function wantReviewLabel(s) { return ({ pending: '待处理', adopted: '已采纳', rejected: '已拒绝' })[s] || '待处理' }
// v0.9.10：投稿审核状态中文映射（真实状态 pending/approved/rejected/transferred），不再显示英文原始值
function contributionStatusLabel(s) { return ({ pending: '待处理', approved: '已通过', rejected: '已拒绝', transferred: '已转存' })[s] || s || '待处理' }
function contributionStatusTag(s) { return ({ approved: 'success', transferred: 'success', rejected: 'danger', pending: 'warning' })[s] || 'info' }
// v0.9.10：投稿标题直接作为资源链接展示——只对 http/https 放行，杜绝 javascript: 注入
function safeHttpUrl(u) { const s = String(u || '').trim(); return /^https?:\/\//i.test(s) ? s : '' }
async function deleteContribution(row) {
  const ok = await confirmBox(`确认删除投稿「${row.title}」？只删登记记录，不影响用户与其它数据。`)
  if (!ok) return
  try { await api(`/contributions/${row.id}`, { method: 'DELETE' }); ElMessage.success('已删除'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
async function addWant() {
  const t = String(wantTitle.value || '').trim()
  if (!t) { ElMessage.warning('请填写影视名称'); return }
  try {
    const d = await api('/media-requests', { method: 'POST', body: JSON.stringify({ title: t, media_type: 'tv' }) }, 60000)
    ElMessage[d.status === 'synced' ? 'success' : 'warning'](`${t}：${d.status === 'synced' ? '关键词已生效' : (d.note || '仅登记，未同步')}`)
    wantTitle.value = ''
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
// ---------- v0.6.1：立即推送、反代地址（v0.9.5 Bot/频道/规则管理已并入推送配置卡） ----------
async function runQueue() {
  runningQueue.value = true
  try {
    const d = await api('/queue/run', { method: 'POST' })
    ElMessage.success(`已处理队列：发送 ${d.sent || 0} 条，失败 ${d.failed || 0} 条，新建分享 ${d.shares_created || 0} 个`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { runningQueue.value = false }
}
async function publishResource(row) {
  publishing.value = row.id
  try {
    const d = await api(`/resources/${row.id}/publish`, { method: 'POST' })
    const base = `已入队 ${d.queued || 0} 条，发送 ${d.sent || 0} 条，失败 ${d.failed || 0} 条`
    // v1.5.2 P0 兜底：skipped 历史上被后端契约覆盖成 bool（锁冲突那轮），
    // 直接展开会抛 TypeError: 'bool' object is not iterable。这里强制数组归一化，
    // 并把 detail[].error 一并收进来——后端已把锁冲突 reason 并进 detail，
    // 前端据此显示「另一次队列处理进行中」正常提示，而不是 JS 异常。
    const reasons = [
      ...(Array.isArray(d.skipped) ? d.skipped : []),
      ...(Array.isArray(d.detail) ? d.detail : []).map(x => x && x.error).filter(Boolean)
    ]
    if (reasons.length) {
      ElMessage({ type: 'warning', message: `${base}｜${reasons[0]}`, duration: 6000, showClose: true })
    } else {
      ElMessage.success(base + (d.queued ? '， Telegram 频道稍后即可收到' : ''))
    }
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { publishing.value = 0 }
}
async function reidentifyAll() {
  try {
    await ElMessageBox.confirm(
      '按目录剧名重新识别库中资源（含已识别项，用于修正误配）。升级后请先到「资源库」执行一次「立即扫描」补全目录线索。',
      '按目录重识别', { confirmButtonText: '开始', cancelButtonText: '取消', type: 'warning' })
  } catch (e) { return }
  reidentifying.value = true
  try {
    const d = await api('/resources/identify-all?force=true&limit=50', { method: 'POST' })
    ElMessage.success(`已重扫 ${d.scanned} 项：识别 ${d.identified} 项，需人工确认 ${d.pending} 项`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { reidentifying.value = false }
}
async function saveTelegramBase() {
  telegramSaving.value = true
  try {
    await api('/telegram/config', { method: 'POST', body: JSON.stringify({ base_url: telegramBase.value.trim() }) })
    ElMessage.success('Telegram API 地址已保存，留空恢复默认 api.telegram.org')
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { telegramSaving.value = false }
}
async function saveQueueSchedule() {
  scheduleSaving.value = true
  try {
    const d = await api('/queue/schedule', { method: 'POST', body: JSON.stringify({
      interval_seconds: queueSchedule.value.interval_seconds, cron: queueSchedule.value.cron || '',
      send_delay: queueSchedule.value.send_delay ?? 2 }) })
    queueSchedule.value = d
    ElMessage.success(d.cron ? `Cron 已生效：${d.cron}` : `推送间隔已生效：每 ${d.interval_seconds} 秒`)
  } catch (e) { ElMessage.error(e.message) }
  finally { scheduleSaving.value = false }
}
async function createShare(row) {
  try {
    const { value } = await ElMessageBox.prompt('0=永久 · 86400=1天 · 604800=7天 · 2592000=30天', '创建光鸭分享', { inputValue: '0' })
    const d = await api(`/resources/${row.id}/share?duration=${value}`, { method: 'POST' })
    ElMessage.success(d.share_url ? '分享已创建并保存链接' : '分享已创建')
    await load()
  } catch (e) { if (e !== 'cancel') ElMessage.error(e.message) }
}
// v0.6.4：取消分享（整剧共用一条时，后端会同步清空同分享的所有单集）
async function cancelShare(row) {
  try {
    await ElMessageBox.confirm(`取消「${row.media_title || row.name}」的分享链接？已推送到 Telegram 的消息不受影响。`, '取消分享', { confirmButtonText: '确认取消', cancelButtonText: '返回', type: 'warning' })
  } catch (e) { return }
  try {
    await api(`/resources/${row.id}/share`, { method: 'DELETE' })
    ElMessage.success('分享已取消，链接已失效')
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
// v0.7.1：重新扫描库——全量重扫 B 库 + 对账移除已删除影视（并取消其分享）+ 自动识别
// v0.9.10：统一走后台异步任务
async function rescanLibrary() { await runAsyncScan('rescan', '重新扫描库') }
// v0.6.4：清空当前扫描结果，避免旧的识别失败/重试残留干扰重新扫描
async function clearLibrary() {
  try {
    await ElMessageBox.confirm('将清空所有资源、推送队列、审核项与扫描断点（保留 Bot/频道/规则/模板/凭证与已建分享）。清空后需重新扫描，确认继续？',
      '清空扫描结果', { confirmButtonText: '确认清空', cancelButtonText: '返回', type: 'warning' })
  } catch (e) { return }
  try {
    const d = await api('/library/clear', { method: 'POST' })
    ElMessage.success(`已清空 ${d.resources} 个资源、${d.queue} 条队列、${d.cursors} 个扫描断点；请重新扫描`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
// 作品视图操作：用组内已识别的代表资源执行，避免对散落单集逐个操作
function seriesRepresentative(g) { return resources.value.find(r => r.id === g.representative_id) || null }
// ---------- LitePan 风格「重新匹配元数据」 ----------
const matchVisible = ref(false), matchRow = ref(null), matchType = ref('tv'), matchQuery = ref('')
const matchCandidates = ref([]), matchSelected = ref(null), matchBusy = ref(false)

const matchClue = computed(() => {
  const n = matchRow.value?.name || ''
  const ep = n.match(/S(\d{1,2})[ ._-]?E(\d{1,3})/i)
  if (ep) return `检测到季集信息 S${ep[1]}E${ep[2]}：按「剧集」识别，不会把单集当成电影`
  if (/第\d+\s*[集期话]|EP\d+|Season\s*\d+/i.test(n)) return '检测到集号/季信息：按「剧集」识别'
  return matchType.value === 'tv' ? '当前按「剧集」搜索候选' : '当前按「电影」搜索候选'
})
function guessType(name) {
  return /S\d{1,2}[ ._-]?E\d{1,3}|第\d+\s*[集期话]|EP\d+|Season\s*\d+|\d{1,2}[xX]\d{2}|电视剧|剧集/i.test(name || '') ? 'tv' : 'movie'
}
function cleanName(name) {
  return String(name || '')
    .replace(/\.[A-Za-z0-9]{1,5}$/, '')
    .replace(/\b(?:19|20)\d{2}\b/g, ' ')
    .replace(/\bS\d{1,2}[ ._-]?E\d{1,3}\b/gi, ' ')
    .replace(/\b(?:2160p|1080p|720p|480p|4K|WEB-?DL|BluRay|BDRip|REMUX|x265|H\.?264|DDP5\.?1|DTS|10bit)\b/gi, ' ')
    .replace(/[._]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}
// v0.6.1：从「谷雨 (2026)(1)/Season 1」里取中文剧名，作为手动匹配默认搜索词
function dirTitle(path) {
  const season = /^(season|series|s)\s*[._\- ]?\d{1,3}$|^第\s*\d{1,2}\s*[季部]$/i
  const generic = /^(电影|影片|电视剧|剧集|连续剧|动漫|动画|番剧|纪录片|综艺|合集|资源|下载|媒体|媒体库|影视|4K|2160p|1080p|movies?|films?|tv|tvshows?|series|shows?|media|downloads?|videos?|anime)$/i
  let title = ''
  for (const s of String(path || '').split(/[\\/]+/).map(x => x.trim()).filter(Boolean)) {
    if (season.test(s) || generic.test(s)) continue
    title = s.replace(/[（(]\s*\d{1,2}\s*[)）]\s*$/, ' ')
             .replace(/[（(]?\s*(19|20)\d{2}\s*[)）]?/g, ' ')
             .replace(/tmdb[-_:= ]?\d+/gi, ' ')
             .replace(/[._]+/g, ' ').replace(/\s+/g, ' ').trim()
  }
  return title
}
async function openMatch(row) {
  matchRow.value = row
  matchType.value = (row.media_type === 'movie' || row.media_type === 'tv') ? row.media_type : guessType(row.name)
  matchQuery.value = dirTitle(row.path_hint) || cleanName(row.media_title || row.name)
  matchCandidates.value = []
  matchTried.value = []
  matchTranslated.value = []
  matchSelected.value = null
  matchVisible.value = true
  await searchMatch(true)
}
async function searchMatch(silent = false) {
  const q = (matchQuery.value || '').trim() || dirTitle(matchRow.value?.path_hint) || cleanName(matchRow.value?.name || '')
  if (!q) { if (!silent) ElMessage.warning('请填写中文片名或 TMDB ID'); return }
  matchBusy.value = true
  matchSelected.value = null
  matchTried.value = []
  matchTranslated.value = []
  try {
    let d
    if (/^\d{2,12}$/.test(q) || /^tmdb[-_:= ]?\d+/i.test(q)) {
      const raw = await api(`/tmdb/search?q=${encodeURIComponent(q)}&media_type=${matchType.value}`)
      d = { candidates: Array.isArray(raw) ? raw : (raw.candidates || []), message: '' }
    } else {
      // 中文标题自动翻译成英/日/韩标题后多语搜索，返回带海报的候选
      d = await api('/tmdb/translate-search', {
        method: 'POST',
        body: JSON.stringify({ title: q, media_type: matchType.value })
      })
    }
    matchCandidates.value = d.candidates || []
    matchTried.value = d.tried || []
    matchTranslated.value = d.translated || []
    if (!silent) {
      if (!matchCandidates.value.length) ElMessage.warning(d.message || '没有找到候选，换个片名或类型再试')
      else ElMessage.success(d.message || `找到 ${matchCandidates.value.length} 个候选`)
    }
  } catch (e) { if (!silent) ElMessage.error(e.message) }
  finally { matchBusy.value = false }
}
async function applyMatch() {
  if (!matchSelected.value) { ElMessage.warning('请先选择一个候选'); return }
  matchBusy.value = true
  try {
    const d = await api(`/resources/${matchRow.value.id}/identify`, {
      method: 'POST',
      body: JSON.stringify({ tmdb_id: matchSelected.value.tmdb_id,
                             media_type: matchSelected.value.media_type, note: '手动匹配' })
    })
    ElMessage.success('已应用匹配：' + (d.media_title || ''))
    matchVisible.value = false
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { matchBusy.value = false }
}
async function markNoMatch() {
  try {
    await ElMessageBox.confirm(
      '标记为「无匹配」后会清空已识别的元数据，且不再被自动识别误配。可随时重新手动匹配。',
      '确认无匹配', { confirmButtonText: '确认', cancelButtonText: '取消', type: 'warning' })
    await api(`/resources/${matchRow.value.id}/match-none`, { method: 'POST' })
    ElMessage.success('已标记无匹配')
    matchVisible.value = false
    await load()
  } catch (e) { if (e !== 'cancel' && e !== 'close') ElMessage.error(e.message) }
}
async function identify(row, auto) {
  if (!auto) return openMatch(row)
  try {
    const d = await api(`/resources/${row.id}/auto-identify`, { method: 'POST' })
    if (d.ok) {
      ElMessage.success(`识别完成：${d.applied?.title || ''}${d.source === 'ai' ? '（AI 辅助）' : ''}`)
      await load()
      return
    }
    try {
      await ElMessageBox.confirm(
        `${d.reason || '未能自动识别'}\n\n可以手动指定电影/剧集类型、片名或 TMDB ID，凭海报和类型选择。`,
        '需要手动匹配', { confirmButtonText: '去手动匹配', cancelButtonText: '稍后', type: 'warning' })
      await openMatch(row)
    } catch (_) {}
    await load()
  } catch (e) {
    const msg = e.message || ''
    if (/TMDB 密钥|尚未配置 TMDB|Invalid API key|401/.test(msg)) {
      try {
        await ElMessageBox.confirm(
          `${msg}\n\n识别只影响海报和简介，资源入库、建分享与推送不受影响。现在去「TMDB 设置」页配置密钥？`,
          'TMDB 无法识别', { confirmButtonText: '去设置', cancelButtonText: '稍后', type: 'warning' })
        setTab('TMDB 设置')
        await loadTmdb()
      } catch (_2) {}
    } else ElMessage.error(msg)
  }
}
async function approve(id) { await api(`/review/${id}/approve`, { method: 'POST' }); ElMessage.success('已通过'); await load() }
async function reject(id) { await api(`/review/${id}/reject`, { method: 'POST' }); ElMessage.success('已拒绝'); await load() }
async function retry(id) { await api(`/queue/${id}/retry`, { method: 'POST' }); ElMessage.success('已重新入队'); await load() }
// 分享中心：所有状态来自后端 /api/sc/*，前端不承载计划、不启定时器。
async function loadSc() {
  const paths = [`/sc/overview?period=${scTrendPeriod.value}`, '/sc/tasks?limit=200', '/sc/push-lists?limit=50',
    '/sc/categories', '/sc/plans', '/sc/cursors', '/sc/ai/suggestions?limit=100',
    '/sc/pacing-rules', '/sc/hour-windows', '/sc/categories/catalog',
    '/sc/category-schedules', '/sc/basic-rules', '/sc/cycle-plan',
    // v2.1.1：分享管理改用独立真实端点（口径与统计卡一致，不再混用 overview.views）
    '/sc/share-records?limit=200',
    // v2.1.1：分类偏好排名（真实派生），用于默认时间表的时段分配
    '/sc/preference']
  const settled = await Promise.allSettled(paths.map(p => api(p)))
  const val = i => settled[i]?.status === 'fulfilled' ? settled[i].value : null
  const ov = val(0); if (ov) scOverview.value = ov
  const tk = val(1); if (tk) scTasks.value = tk.items || []
  const pl = val(2); if (pl) scLists.value = pl.items || []
  const ct = val(3); if (ct) scCategories.value = ct.items || []
  const pn = val(4); if (pn) scPlans.value = pn
  const cu = val(5); if (cu) scCursors.value = cu.items || []
  const ai = val(6); if (ai) scAI.value = ai.items || []
  const pc = val(7); if (pc && (pc.items || []).length) {
    const group = (pc.items || []).find(x => x.scope_key === 'global:group')
    if (group) scPacingForm.value = { ...scPacingForm.value, ...group }
  }
  const hw = val(8); if (hw) {
    scWindows.value = hw.items || []
    const activeVid = ov?.active_version_id || 0
    const rows = activeVid ? (hw.items || []).filter(x => x.plan_version_id === activeVid) : (hw.items || [])
    scHourRows.value = rows.map(x => ({ ...x })).sort((a, b) => a.start_hour - b.start_hour)
  }
  // v2.0.5 设计稿数据
  const catalog = val(9); if (catalog) scGenreCatalog.value = catalog.items || []
  const sched = val(10); if (sched) scSchedRows.value = sched.items || []
  const basic = val(11); if (basic) scBasicRules.value = { ...scBasicRules.value, ...basic }
  const cycle = val(12); if (cycle) scCyclePlan.value = { ...scCyclePlan.value, ...cycle }
  const sr = val(13); if (sr) scShareRecords.value = sr.items || []
  const pf = val(14); if (pf) scPreference.value = { ...scPreference.value, ...pf }
}

// ---------- v2.1.7 #241：阅读量统计 ----------
// 单独一个 loader，不混进 loadSc()——阅读量是**辅助能力**，
// 它的端点失败（未配置 / 未装依赖）绝不能影响发布中心的正常加载。
async function loadScStats() {
  scStatsLoading.value = true
  try {
    const [st, tr, items, runs] = await Promise.allSettled([
      api('/sc/stats/status'),
      api('/sc/stats/trend?days=' + scStatsDays.value),
      api('/sc/stats/items?limit=50'),
      api('/sc/stats/runs?limit=5'),
    ])
    if (st.status === 'fulfilled') {
      scStatsStatus.value = st.value || scStatsStatus.value
      // 配置表单只回填「非密文」项（凭证永远不回显明文）。
      scStatsConfig.value = {
        ...scStatsConfig.value,
        enabled: !!scStatsStatus.value.enabled,
        lookback_hours: Number(scStatsStatus.value.lookback_hours || 72),
        max_per_run: Number(scStatsStatus.value.max_per_run || 200),
      }
    }
    if (tr.status === 'fulfilled') scStatsTrend.value = tr.value || scStatsTrend.value
    if (items.status === 'fulfilled') scStatsItems.value = items.value?.items || []
    if (runs.status === 'fulfilled') scStatsRuns.value = runs.value?.runs || []
  } catch (e) {
    // 阅读量加载失败不提示错误弹窗——它是可选功能，静默降级即可。
  } finally { scStatsLoading.value = false }
}
async function saveScStatsConfig() {
  scStatsSaving.value = true
  try {
    const body = {
      enabled: !!scStatsConfig.value.enabled,
      lookback_hours: Number(scStatsConfig.value.lookback_hours || 72),
      max_per_run: Number(scStatsConfig.value.max_per_run || 200),
    }
    // 凭证只在**用户实际填写时**才提交；留空表示不修改。
    if (scStatsConfig.value.api_id) body.api_id = scStatsConfig.value.api_id
    if (scStatsConfig.value.api_hash) body.api_hash = scStatsConfig.value.api_hash
    if (scStatsConfig.value.phone) body.phone = scStatsConfig.value.phone
    const d = await api('/sc/stats/config', { method: 'POST', body: JSON.stringify({ payload: body }) })
    // 保存后清空凭证输入框——明文不留存在页面状态里。
    scStatsConfig.value.api_id = ''
    scStatsConfig.value.api_hash = ''
    ElMessage.success('阅读量采集配置已保存')
    await loadScStats()
    return d
  } catch (e) { ElMessage.error(e.message) }
  finally { scStatsSaving.value = false }
}
async function scStatsSendLoginCode() {
  scStatsSaving.value = true
  try {
    const d = await api('/sc/stats/login/code', { method: 'POST', body: JSON.stringify({ payload: {} }) })
    if (d.ok && d.sent) {
      scStatsLoginCodeSent.value = true
      ElMessage.success('验证码已发送到该手机号，请填写后提交')
    } else if (d.ok) {
      ElMessage.info('该账号已登录，无需重复验证')
    } else {
      ElMessage.error(d.error || '获取验证码失败')
    }
  } catch (e) { ElMessage.error(e.message) }
  finally { scStatsSaving.value = false }
}
async function scStatsSubmitLoginCode() {
  const code = String(scStatsLoginCode.value || '').trim()
  if (!code) { ElMessage.warning('请填写验证码'); return }
  scStatsSaving.value = true
  try {
    const d = await api('/sc/stats/login/verify', { method: 'POST', body: JSON.stringify({ payload: { code } }) })
    if (d.ok) {
      scStatsLoginCode.value = ''
      scStatsLoginCodeSent.value = false
      ElMessage.success('登录成功')
      await loadScStats()
    } else {
      ElMessage.error(d.error || '验证码校验失败')
    }
  } catch (e) { ElMessage.error(e.message) }
  finally { scStatsSaving.value = false }
}
async function scStatsCollect() {
  scStatsCollecting.value = true
  try {
    const d = await api('/sc/stats/collect', { method: 'POST', body: JSON.stringify({ payload: {} }) })
    if (d.ok) ElMessage.success(`采集完成：新采集 ${d.collected || 0} 条，跳过 ${d.skipped || 0} 条`)
    else ElMessage.warning(d.error || d.reason || '采集未执行，请检查配置与登录状态')
    await loadScStats()
    return d
  } catch (e) { ElMessage.error(e.message) }
  finally { scStatsCollecting.value = false }
}
async function scStatsSwitchDays(days) {
  scStatsDays.value = days
  await loadScStats()
}
// 采集轮次状态的中文标签（页面不暴露后端状态码）
function scStatsRunLabel(status) {
  return ({ success: '成功', running: '进行中', failed: '失败',
            disabled: '未开启', unavailable: '通道不可用' })[status] || '未知'
}
async function scSwitchTrend(period) { scTrendPeriod.value = period; await loadSc() }
// 切换分享中心子页。阅读量子页的数据**按需加载**——
// 它是辅助能力，不该拖慢其它子页的打开速度。
async function scSwitchSubNav(key) {
  scSubNav.value = key
  if (key === '阅读量') { try { await loadScStats() } catch (e) {} }
}
async function scRun() {
  try {
    const d = await api('/sc/run', { method: 'POST' })
    ElMessage.success(`调度完成：清单 ${d.lists || 0} · 项 ${d.items || 0} · 任务 ${d.tasks || 0}`)
    await loadSc(); return d
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scTogglePause() {
  const next = !scOverview.value.paused
  try {
    await ElMessageBox.confirm(
      next ? '确认暂停发布？将停止新一轮建分享与提交，在途任务与消息发送不受影响。' : '确认恢复发布？下一轮调度将正常建分享并提交。',
      next ? '暂停发布' : '恢复发布', { type: 'warning' })
    await api('/sc/pause', { method: 'POST', body: JSON.stringify({ payload: { paused: next } }) })
    ElMessage.success(next ? '已暂停发布' : '已恢复发布')
    await loadSc()
  } catch (e) { if (e !== 'cancel' && e?.message !== 'cancel') ElMessage.error(String(e?.message || e)) }
}
async function scOpenList(row) {
  scActiveList.value = row
  try { const d = await api(`/sc/push-lists/${row.id}/items?limit=200`); scListItems.value = d.items || [] }
  catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scSavePacing() {
  try {
    await api('/sc/pacing-rules', { method: 'POST', body: JSON.stringify({ payload: scPacingForm.value }) })
    ElMessage.success('推送间隔已保存（低于官方下限的请求会被后端夹逼）')
    await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scGenerateAI() {
  try { const d = await api('/sc/ai/generate', { method: 'POST', body: '{}' }); ElMessage.success(`智能建议：新增 ${d.created || 0} 条（来源 ${d.source || 'local'}）`); await loadSc() }
  catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scApplyAI(id, auto) {
  try { const d = await api(`/sc/ai/suggestions/${id}/apply`, { method: 'POST', body: JSON.stringify({ payload: { auto: !!auto } }) }); if (!d.ok) ElMessage.warning(d.reason || '未应用'); else ElMessage.success('建议已应用'); await loadSc() }
  catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scRejectAI(id) {
  try { await api(`/sc/ai/suggestions/${id}/reject`, { method: 'POST' }); ElMessage.success('已忽略该建议'); await loadSc() }
  catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scTaskAction(id, action) {
  try { await api(`/sc/tasks/${id}/${action}`, { method: 'POST' }); ElMessage.success(action === 'retry' ? '已重试' : '已取消'); await loadSc() }
  catch (e) { ElMessage.error(String(e?.message || e)) }
}
// KPI / 链接健康点击 → 切到发布队列并按真实状态/链接态筛选
function scFilterQueue(status, linkState) {
  scSubNav.value = '推送任务'
  scTaskStatusFilter.value = status || ''
  scLinkFilter.value = linkState || ''
}
function scClearQueueFilter() { scTaskStatusFilter.value = ''; scLinkFilter.value = '' }
// 任务详情：调用真实端点 /sc/tasks/{id}（含执行记录），不编造
async function scOpenTaskDetail(id) {
  try {
    const d = await api(`/sc/tasks/${id}`)
    scDetailTask.value = d.item || null
    scDetailExecutions.value = d.executions || []
    scDetailVisible.value = true
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
// v2.1.2：分享详情改为隐藏式弹窗——点「详情」才弹出，平时不占版面
function scOpenShareDetail(row) {
  scSelectedDetail.value = row || null
  scShareDetailVisible.value = true
}
// v2.1.2：清理无效推送任务（归档，不删历史行）
async function scCleanupTasks() {
  try {
    await ElMessageBox.confirm(
      '清理无效任务？只归档「已取消」以及「资源已从资源库移除、重试也不会成功」的失败任务；历史记录与执行日志保留，不会删除数据。',
      '清理无效任务', { type: 'warning' })
    scCleaningTasks.value = true
    const d = await api('/sc/tasks/cleanup', { method: 'POST', body: JSON.stringify({ payload: {} }) })
    if (!d.ok) { ElMessage.warning(d.reason || '清理失败'); return }
    ElMessage.success(`已清理 ${d.cleaned || 0} 个无效任务${d.skipped ? `，跳过 ${d.skipped} 个仍可重试的任务` : ''}`)
    await loadSc()
  } catch (e) { if (e !== 'cancel' && e?.message !== 'cancel') ElMessage.error(String(e?.message || e)) }
  finally { scCleaningTasks.value = false }
}
// v2.1.2：清理无效分享链接（归档，不删 B 库资源、不删远端分享）
async function scCleanupLinks() {
  try {
    await ElMessageBox.confirm(
      '清理无效链接？只归档「链接已判失效」以及「作品已从资源库移除」的分享记录；不会删除 B 库文件，也不会删除远端分享。',
      '清理无效链接', { type: 'warning' })
    scCleaningLinks.value = true
    const d = await api('/sc/share-records/cleanup', { method: 'POST', body: JSON.stringify({ payload: {} }) })
    if (!d.ok) { ElMessage.warning(d.reason || '清理失败'); return }
    ElMessage.success(`已清理 ${d.cleaned || 0} 条无效链接${d.skipped ? `，跳过 ${d.skipped} 条仍有效的链接` : ''}`)
    await loadSc()
  } catch (e) { if (e !== 'cancel' && e?.message !== 'cancel') ElMessage.error(String(e?.message || e)) }
  finally { scCleaningLinks.value = false }
}
// v2.1.2：批量采纳 AI 建议时段（后端按各分类真实历史派生，无历史的不编造）
// v2.1.6：批量采纳 AI 建议时段——按勾选生效（未勾选或全勾选 = 全部）。
// 每个分类都有建议（实测优先，无实测用默认时间表兜底），因此采纳率不再为零。
async function scAdoptAllAi() {
  const keys = scSchedTargetKeys()
  if (!keys.length) { ElMessage.warning('没有可采纳的分类'); return }
  const scope = keys.length >= scSchedRows.value.length ? '全部分类' : `选中的 ${keys.length} 个分类`
  try {
    await ElMessageBox.confirm(
      `批量采纳${scope}的建议时段？建议优先取该分类的真实发布分布，无实测时用默认时间表兜底；落在静默保护区间内的分类会被跳过。`,
      '批量采纳 AI 建议', { type: 'warning' })
    scAdoptingAllAi.value = true
    const d = await api('/sc/category-schedules/adopt-ai-all', { method: 'POST', body: JSON.stringify({ payload: {
      category_keys: keys } }) })
    if (!d.ok) { ElMessage.warning(d.reason || '批量采纳失败'); return }
    ElMessage.success(`已采纳 ${d.adopted || 0} 个分类的建议时段${d.skipped ? `，跳过 ${d.skipped} 个与静默时段冲突的分类` : ''}`)
    await loadSc()
  } catch (e) { if (e !== 'cancel' && e?.message !== 'cancel') ElMessage.error(String(e?.message || e)) }
  finally { scAdoptingAllAi.value = false }
}
async function scBatchTask(action) {
  // v2.1.1：分享管理的勾选走 scSelectedShares，推送任务页走 scSelectedTasks。
  // 原实现只读 scSelectedTasks，导致分享管理页勾选后「批量分享/批量取消」点了没反应。
  const pool = scSubNav.value === '分享管理' ? scSelectedShares.value : scSelectedTasks.value
  const ids = pool.map(t => t.id)
  if (!ids.length) { ElMessage.warning('请先勾选要操作的记录'); return }
  try {
    await ElMessageBox.confirm(`确认对 ${ids.length} 个任务执行「${action === 'retry' ? '重试' : '取消'}」？`, '批量操作', { type: 'warning' })
    for (const id of ids) await api(`/sc/tasks/${id}/${action}`, { method: 'POST' })
    ElMessage.success(`已批量${action === 'retry' ? '重试' : '取消'} ${ids.length} 个任务`)
    scSelectedTasks.value = []
    scSelectedShares.value = []
    await loadSc()
  } catch (e) { if (e !== 'cancel' && e?.message !== 'cancel') ElMessage.error(String(e?.message || e)) }
}
// v2.1.1：导出当前筛选下的推送任务结果（真实行导出为 CSV，不含任何编造字段）
function scExportTasks() {
  const rows = scTaskViewRows.value || []
  if (!rows.length) { ElMessage.warning('当前筛选条件下没有可导出的任务'); return }
  const head = ['任务ID', '资源名称', '类型', '状态', '链接态', '分享链接', '重试次数', '最后执行时间', '错误原因']
  const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`
  const body = rows.map(r => [
    r.id, r.title || r.work_key || '', r.kind === 'first' ? '首次发布' : '追更',
    r.status || '', r.link_state || '', r.share_url || '',
    r.attempts ?? 0, fmtTime(r.updated_at, true), r.error || '',
  ].map(esc).join(','))
  const csv = '﻿' + [head.map(esc).join(','), ...body].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `推送任务结果_${new Date().toISOString().slice(0, 10)}.csv`
  document.body.appendChild(a); a.click(); document.body.removeChild(a)
  URL.revokeObjectURL(url)
  ElMessage.success(`已导出 ${rows.length} 条任务结果`)
}
// v2.1.1：复制分享链接（分享管理表操作列）
async function scCopyShareLink(row) {
  const url = row?.share_url || ''
  if (!url) { ElMessage.warning('该记录还没有分享链接'); return }
  try {
    await navigator.clipboard.writeText(url)
    ElMessage.success('分享链接已复制')
  } catch (e) {
    // 剪贴板不可用时降级为选中文本，避免功能直接失效
    ElMessage.warning('复制失败，请手动选择链接复制')
  }
}
async function scSaveCategory() {
  if (!scCatForm.value.category_key) { ElMessage.warning('请填写分类键'); return }
  try {
    const d = await api('/sc/categories', { method: 'POST', body: JSON.stringify({ payload: scCatForm.value }) })
    if (!d.ok) { ElMessage.warning(d.reason || '保存失败'); return }
    ElMessage.success('分类已保存'); scCatForm.value = { category_key: '', name: '', media_type: '', priority: 0 }
    await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scReassignCategories() {
  try {
    await ElMessageBox.confirm('按当前资源 TMDB 元数据重新归类（只更新主归属，不改清单/任务）？', '重新归类', { type: 'warning' })
    const d = await api('/sc/categories/reassign', { method: 'POST' })
    ElMessage.success(`重新归类完成：新建版本 ${d.versions_created || 0}`)
    await loadSc()
  } catch (e) { if (e !== 'cancel' && e?.message !== 'cancel') ElMessage.error(String(e?.message || e)) }
}
async function scCreatePlan() {
  try {
    const d = await api('/sc/plans', { method: 'POST', body: JSON.stringify({ payload: { name: '新发布计划', plan_type: 'daily', enabled: true, config: {} } }) })
    if (!d.ok) { ElMessage.warning(d.reason || '新建失败'); return }
    ElMessage.success(`计划 #${d.item?.id} 已创建（含 v1 草稿版本）`)
    scPlanVersionForm.value.plan_id = d.item?.id || 1
    await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scCreatePlanVersion() {
  const { plan_id, config } = scPlanVersionForm.value
  let cfg = {}
  try { cfg = JSON.parse(config || '{}') } catch (e) { ElMessage.warning('配置 JSON 格式错误'); return }
  try {
    const d = await api(`/sc/plans/${plan_id}/versions`, { method: 'POST', body: JSON.stringify({ payload: cfg }) })
    if (!d.ok) { ElMessage.warning(d.reason || '新建版本失败'); return }
    ElMessage.success(`版本 v${d.item?.version_no} 已创建（草稿）`)
    await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scActivateVersion(id) {
  try {
    await ElMessageBox.confirm('激活该版本将归档当前 active 版本，确认？', '激活计划版本', { type: 'warning' })
    const d = await api(`/sc/plans/${id}/activate`, { method: 'POST' })
    if (!d.ok) { ElMessage.warning(d.reason || '激活失败'); return }
    ElMessage.success('版本已激活'); await loadSc()
  } catch (e) { if (e !== 'cancel' && e?.message !== 'cancel') ElMessage.error(String(e?.message || e)) }
}
async function scRestoreWindowTemplate() {
  try {
    await ElMessageBox.confirm('恢复默认 00:00–24:00 共 12 段（每 2 小时一段）模板？当前各时段的分类绑定将清空（改为按计划轮转）。', '恢复默认模板', { type: 'warning' })
    const d = await api('/sc/hour-windows/reset-template', { method: 'POST', body: JSON.stringify({ payload: { plan_version_id: scOverview.value.active_version_id } }) })
    if (!d.ok) { ElMessage.warning(d.reason || '恢复失败'); return }
    ElMessage.success(`已恢复默认模板 ${d.segments} 段`); await loadSc()
  } catch (e) { if (e !== 'cancel' && e?.message !== 'cancel') ElMessage.error(String(e?.message || e)) }
}
async function scSaveHourRow(row) {
  try {
    const d = await api('/sc/hour-windows', { method: 'POST', body: JSON.stringify({ payload: {
      plan_version_id: row.plan_version_id, start_hour: row.start_hour, end_hour: row.end_hour,
      category_key: row.category_key || '', quota: row.quota || 0, enabled: !!row.enabled } }) })
    if (!d.ok) { ElMessage.warning(d.reason || '保存失败'); return }
    ElMessage.success(`${row.start_hour}:00 时段已保存`)
    await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scSaveAllHourRows() {
  if (!scHourRows.value.length) { ElMessage.warning('暂无时段可保存'); return }
  scHourSaving.value = true
  try {
    for (const row of scHourRows.value) {
      const d = await api('/sc/hour-windows', { method: 'POST', body: JSON.stringify({ payload: {
        plan_version_id: row.plan_version_id, start_hour: row.start_hour, end_hour: row.end_hour,
        category_key: row.category_key || '', quota: row.quota || 0, enabled: !!row.enabled } }) })
      if (!d.ok) throw new Error(d.reason || '保存失败')
    }
    ElMessage.success(`已保存 ${scHourRows.value.length} 个时段`)
    await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
  finally { scHourSaving.value = false }
}
// ---- v2.0.5 设计稿：发布规则 / 基础规则 / 周期计划 / 分享管理交互 ----
// v2.1.6：批量操作的目标集合——**按勾选生效**。
// 口径：勾选为空 → 全部；勾选数 == 总行数 → 全部；否则只处理勾选的行。
// （用户口径：「若都未勾选或者全勾选则表示全部分类选中」）
function scSchedTargetRows() {
  const rows = scPrefSortRows(scSchedRows.value)
  const picked = scSchedSelection.value || []
  if (!picked.length || picked.length >= rows.length) return rows
  const keys = new Set(picked.map(r => r.ref || r.category_key))
  return rows.filter(r => keys.has(r.ref || r.category_key))
}
// 目标分类 key 列表（供后端批量端点按分类过滤）
function scSchedTargetKeys() {
  return scSchedTargetRows().map(r => r.ref || r.category_key).filter(Boolean)
}
// v2.1.6：分类排期保存**不再携带 hourly_limit / daily_limit**——
// 分类级上限已从表中移除，速率统一由周期计划管理约束。
// 若继续携带，历史值会被原样写回（后端只在显式携带时写入），前端无从清理；
// 故此处彻底不带这两个字段。
async function scSaveSchedRow(row) {
  try {
    const d = await api('/sc/category-schedules', { method: 'POST', body: JSON.stringify({ payload: {
      plan_version_id: scOverview.value.active_version_id, category_key: row.ref || row.category_key,
      window_start: row.window_start, window_end: row.window_end,
      ai_adopted: !!row.ai_adopted, enabled: !!row.enabled } }) })
    if (!d.ok) { ElMessage.warning(d.reason || '保存失败'); return }
    ElMessage.success(`${row.name || '分类'} 排期已保存`)
    await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scSaveAllSchedRows() {
  const targets = scSchedTargetRows()
  if (!targets.length) { ElMessage.warning('没有可保存的分类'); return }
  scSchedSaving.value = true
  try {
    for (const row of targets) {
      const d = await api('/sc/category-schedules', { method: 'POST', body: JSON.stringify({ payload: {
        plan_version_id: scOverview.value.active_version_id, category_key: row.ref || row.category_key,
        window_start: row.window_start, window_end: row.window_end,
        ai_adopted: !!row.ai_adopted, enabled: !!row.enabled } }) })
      if (!d.ok) throw new Error(d.reason || '保存失败')
    }
    ElMessage.success(`已保存 ${targets.length} 个分类排期`)
    await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
  finally { scSchedSaving.value = false }
}
async function scAdoptSchedAI(row) {
  try {
    const d = await api('/sc/category-schedules/adopt-ai', { method: 'POST', body: JSON.stringify({ payload: { id: row.id } }) })
    if (!d.ok) { ElMessage.warning(d.reason || '暂无可采纳建议'); return }
    ElMessage.success(`已采纳建议时段 ${d.window_start}:00–${d.window_end}:00`)
    await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
// v2.1.6：「复制」按钮已下线（用户口径：去掉每个分类后的复制按钮）。
// 批量场景改用表格勾选 + 「保存」，无需逐行复制。
// v2.1.6：语义由「一键应用默认（只填空行）」改为「一键恢复默认」——
// 把所选分类的时段**重置回默认时间表**（热门白天 / 冷门夜间，2 小时一段，
// 自动避开静默时段），并重新纳入系统托管（此后跟随默认时间表刷新）。
// 未勾选或全勾选 = 全部；只处理勾选行。
async function scApplySchedDefaults() {
  const keys = scSchedTargetKeys()
  if (!keys.length) { ElMessage.warning('没有可恢复的分类'); return }
  const scope = keys.length >= scSchedRows.value.length ? '全部分类' : `选中的 ${keys.length} 个分类`
  try {
    await ElMessageBox.confirm(
      `把${scope}的推送时段恢复为默认时间表？手工设定过的时段会被覆盖（启用状态保留）。`,
      '一键恢复默认', { type: 'warning' })
    scSchedSaving.value = true
    const d = await api('/sc/category-schedules/apply-defaults', { method: 'POST', body: JSON.stringify({ payload: {
      category_keys: keys, force: true } }) })
    if (!d.ok) { ElMessage.warning(d.reason || '恢复失败'); return }
    ElMessage.success(`已恢复默认时段：${d.applied || 0} / ${d.total || 0} 类`)
    await loadSc()
  } catch (e) { if (e !== 'cancel' && e?.message !== 'cancel') ElMessage.error(String(e?.message || e)) }
  finally { scSchedSaving.value = false }
}
// v2.1.6：「按偏好分配」按钮已下线（用户口径：去掉无意义的按偏好分配时间段功能）——
// 默认时间表（热门白天 / 冷门夜间）本就由偏好排名派生，
// 「一键恢复默认」即等价于「按偏好重新分配」，无需两个入口。
// 偏好排名 → 分类引用映射（真实派生，供表格展示排序与高峰档位）
// v2.1.4：统一用后端下发的中性引用名 ref 作键，页面不出现后端字段名。
const scPrefRank = computed(() => {
  const m = {}
  for (const it of scPreference.value.items || []) m[it.ref ?? it.category_key] = it
  return m
})
function scPrefSortRows(rows) {
  // v2.1.4：整表按**配置时间段**升序（0-2 → 2-4 → 4-6 …），
  // 与用户巡检时「按时间顺序看每个时段」的直觉一致；
  // 同起点的按止点升序，再按偏好排名、分类键稳定排序。
  const m = scPrefRank.value
  return [...rows].sort((a, b) => {
    const sa = Number(a.window_start ?? 0), sb = Number(b.window_start ?? 0)
    if (sa !== sb) return sa - sb
    const ea = Number(a.window_end ?? 0), eb = Number(b.window_end ?? 0)
    if (ea !== eb) return ea - eb
    const ra = m[a.ref]?.rank ?? 9999, rb = m[b.ref]?.rank ?? 9999
    if (ra !== rb) return ra - rb
    return String(a.ref ?? '').localeCompare(String(b.ref ?? ''))
  })
}

// v2.1.4：静默保护开启时，配置时段不得选到静默区间内的小时。
// 静默区间支持跨午夜（22:00–次日 6:00）。
const scQuietHours = computed(() => {
  const cp = scCyclePlan.value || {}
  if (!cp.quiet_enabled) return new Set()
  let qs = Number(cp.quiet_start ?? 0), qe = Number(cp.quiet_end ?? 0)
  if (!Number.isFinite(qs) || !Number.isFinite(qe) || qs === qe) return new Set()
  const set = new Set()
  if (qs < qe) { for (let h = qs; h < qe; h++) set.add(h) }
  else { for (let h = qs; h < 24; h++) set.add(h); for (let h = 0; h < qe; h++) set.add(h) }
  return set
})
const scQuietOn = computed(() => scQuietHours.value.size > 0)
// 起始小时候选（0-23）；止点候选（1-24）
const scStartHourOptions = computed(() => Array.from({ length: 24 }, (_, i) => i))
const scEndHourOptions = computed(() => Array.from({ length: 24 }, (_, i) => i + 1))
const scHourDisabled = (h) => scQuietHours.value.has(Number(h))
// 起止下拉的可用项：起始用 0-23，止点用 1-24（止点按「结束那一小时」判定是否落在静默）
const scEndHourDisabled = (h) => scQuietHours.value.has((Number(h) - 1 + 24) % 24)

// v2.1.6：周维度辅助函数已下线——用户口径「只推荐时间段（2 小时颗粒度），
// 不用到周维度」。后端仍兼容读取 week_windows（历史数据），但页面不再展示。
async function scResetSchedDefault(row) {
  try {
    await ElMessageBox.confirm(`把「${row.name}」的排期恢复为内置默认（按媒体类型分档）？`, '恢复默认', { type: 'warning' })
    const d = await api('/sc/category-schedules/reset-default', { method: 'POST', body: JSON.stringify({ payload: { id: row.id } }) })
    if (!d.ok) { ElMessage.warning(d.reason || '恢复失败'); return }
    ElMessage.success('已恢复默认排期'); await loadSc()
  } catch (e) { if (e !== 'cancel' && e?.message !== 'cancel') ElMessage.error(String(e?.message || e)) }
}
async function scSaveBasicRules() {
  try {
    await api('/sc/basic-rules', { method: 'POST', body: JSON.stringify({ payload: scBasicRules.value }) })
    ElMessage.success('基础规则已保存'); await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
async function scSaveCyclePlan() {
  try {
    const d = await api('/sc/cycle-plan', { method: 'POST', body: JSON.stringify({ payload: scCyclePlan.value }) })
    if (!d.ok) { ElMessage.warning(d.reason || '保存失败'); return }
    ElMessage.success('周期计划已保存（静默时段立即对调度生效）'); await loadSc()
  } catch (e) { ElMessage.error(String(e?.message || e)) }
}
function scSchedName(key) {
  return scGenreCatalog.value.find(c => c.category_key === key)?.name || key
}
// ---- 设计稿可视化：内联 SVG，数据全部来自 /sc/overview 真实聚合 ----
const SC_DONUT_COLORS = ['#6d5efc', '#4a90e2', '#8e7cf0', '#f5a623', '#e8917a', '#b8bdd1']
function scDonutColor(key) {
  const items = scOverview.value.category_ratio?.items || []
  const i = items.findIndex(x => x.category_key === key)
  return SC_DONUT_COLORS[(i < 0 ? 0 : i) % SC_DONUT_COLORS.length]
}
function scTrendCoords() {
  const series = (scOverview.value.trend?.series || []).slice(-8)
  if (!series.length) return []
  const max = Math.max(1, ...series.map(s => s.published || 0))
  const n = series.length
  return series.map((s, i) => ({
    x: n === 1 ? 210 : (20 + i * (380 / (n - 1))),
    y: 140 - ((s.published || 0) / max) * 115
  }))
}
function scTrendPoints() { return scTrendCoords().map(p => `${p.x},${p.y}`).join(' ') }
function scTrendArea() {
  const pts = scTrendCoords()
  if (!pts.length) return ''
  return `20,150 ${pts.map(p => `${p.x},${p.y}`).join(' ')} ${pts[pts.length-1].x},150`
}
function scDonutSegments() {
  const items = (scOverview.value.category_ratio?.items || []).slice(0, 6)
  const total = items.reduce((s, x) => s + (x.count || 0), 0) || 1
  const circumference = 2 * Math.PI * 46
  let offset = 0
  return items.map((it, i) => {
    const frac = (it.count || 0) / total
    const dash = `${(frac * circumference).toFixed(1)} ${circumference.toFixed(1)}`
    const seg = { dash, offset: -offset.toFixed(1), color: SC_DONUT_COLORS[i % SC_DONUT_COLORS.length] }
    offset += frac * circumference
    return seg
  })
}
function isBoolOn(v) { return ['1', 'true', 'yes', 'on'].includes(String(v ?? '').toLowerCase()) }
// v0.9.2：网页端扫码登录——光鸭 App 扫一扫，免开放平台应用凭证
async function startWebqr() {
  try {
    const d = await api('/auth/webqr/start', { method: 'POST' })
    webqr.value = d
    // 二维码载荷是 App 内授权地址，只能用光鸭 App 扫一扫，不能用浏览器打开
    const payload = d.verification_uri_complete || d.verification_url || ''
    if (payload) {
      webqrPng.value = await QRCode.toDataURL(payload, { width: 240, margin: 2,
        color: { dark: '#1f2d5a', light: '#ffffff' } })
    } else {
      webqrPng.value = ''
      ElMessage.warning('授权地址为空，请点「取消」后重试')
    }
    startWebqrPolling()
    ElMessage.success('请用光鸭 App 扫一扫下方二维码授权')
  } catch (e) { ElMessage.error(e.message) }
}
function startWebqrPolling() {
  clearInterval(webqrTimer)
  const interval = Math.max(2, Number(webqr.value?.interval) || 2) * 1000
  webqrTimer = setInterval(async () => {
    try {
      const d = await api('/auth/webqr/poll', { method: 'POST' })
      if (d.status === 'authorized') { clearInterval(webqrTimer); webqr.value = null; webqrPng.value = ''; ElMessage.success('扫码授权成功，凭证已加密保存，到期前将自动刷新'); await load(); await loadSms() }
      else if (d.status !== 'pending') clearInterval(webqrTimer)
    } catch (e) { clearInterval(webqrTimer) }
  }, interval)
}
function cancelWebqr() { clearInterval(webqrTimer); webqr.value = null; webqrPng.value = '' }
async function copyWebqrCode() {
  try { await navigator.clipboard.writeText(webqr.value.user_code); ElMessage.success('授权码已复制') }
  catch (e) { ElMessage.error('复制失败，请手动选择') }
}
async function refreshToken() {
  try { await api('/auth/refresh', { method: 'POST' }); ElMessage.success('Token 已刷新'); await loadAuth() }
  catch (e) { ElMessage.error(e.message) }
}
async function verify() {
  try {
    const d = await api('/auth/openapi/verify')
    ElMessage.success(d.message || (d.channel === 'web'
      ? '光鸭网页端连接正常 · 短信登录'
      : `官方 OpenAPI 可用 · 用户 ${d.user_id || ''}`))
  } catch (e) { ElMessage.error(e.message) }
}
async function saveTpl() {
  try {
    if (tpl.value.id) await api(`/templates/${tpl.value.id}`, { method: 'PUT', body: JSON.stringify(tpl.value) })
    else await api('/templates', { method: 'POST', body: JSON.stringify(tpl.value) })
    tpl.value = { name: '', content: '', enabled: true, is_default: false }
    tplPresetPick.value = ''
    ElMessage.success('模板已保存'); await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function restoreDefaultTpl() {
  try {
    const d = await api('/templates/default', { method: 'POST' })
    tpl.value = { id: d.id || 0, name: '默认图文模板', content: d.content, enabled: true, is_default: true }
    ElMessage.success('已载入整剧风格默认模板（含分类标签），已设为默认')
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
function insertVar(key) { tpl.value.content = (tpl.value.content || '') + '{{' + key + '}}' }

// ============ v0.7.0：批量管理 + TG 配置 ============
function selectedRepIds() {
  return filteredSeries.value.filter(g => selectedSeries.value.includes(g.key)).map(g => g.representative_id)
}
async function batchAction(action, finish = '') {
  const ids = selectedRepIds()
  if (!ids.length) { ElMessage.warning('请先勾选作品'); return }
  let duration = 0
  if (action === 'set_duration' || action === 're_share') {
    duration = Number(batchDuration.value || 0)
  }
  const labels = { set_duration: '设置效期', cancel_share: '取消分享', re_share: '重新分享', identify: '批量重识别', set_finish: '修正完结状态' }
  if (action === 'cancel_share' && !await confirmBox(`确定取消勾选的 ${ids.length} 部作品的分享？`)) return
  batchBusy.value = true
  try {
    const r = await api('/library/batch', { method: 'POST', body: JSON.stringify({ ids, action, duration, finish }) })
    ElMessage.success(`${labels[action]}完成：成功 ${r.succeeded}，失败 ${r.failed}${r.errors?.length ? '（' + r.errors[0] + '）' : ''}`)
    selectedSeries.value = []
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { batchBusy.value = false }
}
// v0.8.0：完结剧识别错误时手动打标，整部作品同步并锁定，TMDB 回填不再覆盖
async function setFinish(g, finish) {
  try {
    await api('/library/batch', { method: 'POST', body: JSON.stringify({ ids: [g.representative_id], action: 'set_finish', finish }) })
    const hint = finish === 'ended' ? '已标记完结' : finish === 'returning' ? '已标记追更' : finish === 'auto' ? '已恢复自动判断' : '已清除状态'
    ElMessage.success(`${g.title} ${hint}`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function loadFinishCheck() {
  finishChecking.value = true
  try {
    const d = await api('/library/finish-check?limit=30', {}, 120000)
    finishChecks.value = d.items || []
    ElMessage.success(`已核对 ${d.checked} 部剧（只读，不自动改状态）`)
  } catch (e) { ElMessage.error(e.message) }
  finally { finishChecking.value = false }
}
async function confirmBox(msg) {
  try { await ElMessageBox.confirm(msg, '确认操作', { type: 'warning' }); return true }
  catch (e) { return false }
}
async function loadTgExtra() {
  try { tgExtra.value = await api('/tg/extra-config') } catch (e) {}
  try { rewardCfg.value = await api('/reward/config') } catch (e) {}
}
async function saveTgExtra() {
  tgExtraSaving.value = true
  try { tgExtra.value = await api('/tg/extra-config', { method: 'POST', body: JSON.stringify(tgExtra.value) }); ElMessage.success('TG 分发配置已保存') }
  catch (e) { ElMessage.error(e.message) }
  finally { tgExtraSaving.value = false }
}
async function saveReward() {
  rewardSaving.value = true
  try {
    rewardCfg.value = await api('/reward/config', { method: 'POST', body: JSON.stringify(rewardCfg.value) })
    await postInteractionConfig()
    ElMessage.success('打赏配置已保存')
    await load()
  }
  catch (e) { ElMessage.error(e.message) }
  finally { rewardSaving.value = false }
}
async function verifyPushBot() {
  if (!pushCfg.value.bot) { ElMessage.warning('请先保存 Bot Token'); return }
  try {
    const d = await api(`/bots/${pushCfg.value.bot.id}/test`, { method: 'POST' })
    ElMessage[d.ok ? 'success' : 'error'](d.message || (d.ok ? 'Token 有效' : '验证失败'))
  } catch (e) { ElMessage.error(e.message) }
}
function tick() {
  clock.value = new Date().toLocaleTimeString('zh-CN', { hour12: false })
  if (authed.value && smsStatus.value && smsStatus.value.logged_in) updateCountdown()
}

onMounted(async () => {
  tick(); clockTimer = setInterval(tick, 1000)
  localStorage.removeItem('ya_tg_password')  // v0.9.20：清理历史版本遗留的明文密码
  // v0.9.20：有 token 则尝试恢复会话（load 会验证 token 有效性；过期自动回登录页）
  if (sessionToken.value) {
    try { await load(); authed.value = true }
    catch (e) { sessionToken.value = ''; localStorage.removeItem(SESSION_KEY) }
  }
  // 会话状态 30s 轮询一次（只读，不刷新活跃时间），剩余时间到期由服务端 401 强制退出
  sessionPollTimer = setInterval(pollSession, 30000)
  // v0.9.10.2：实时进度轮询，登录后才启动；2 秒一次不影响服务器（单条只读查询）
  mirrorPollTimer = setInterval(pollMirrorStatus, 2000)
})
onUnmounted(() => { clearInterval(clockTimer); clearInterval(webqrTimer); clearInterval(mirrorPollTimer); clearInterval(sessionPollTimer) })
</script>

<template>
  <div v-if="!authed" class="login-screen">
    <div class="login-deco d1"></div><div class="login-deco d2"></div>
    <div class="login-card">
      <div class="login-brand"><img class="brand-logo lg" src="/logo.svg" alt="Ya-TG"/><div><h1>Ya-TG</h1><p>光鸭资源分发工作台</p></div></div>
      <div class="login-divider"></div>
      <h3>欢迎回来</h3>
      <p class="login-hint">请输入管理员密码以访问控制台。持续 2 小时无操作将自动退出并要求重新输入。</p>
      <el-input v-model="password" type="password" show-password placeholder="ADMIN_PASSWORD" @keyup.enter="login" size="large"/>
      <el-button type="primary" size="large" class="login-btn" @click="login">进入工作台</el-button>
      <div class="login-foot"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('shield')"/></svg> 仅保存临时会话，不保存明文密码 · 北京时间调度</div>
    </div>
  </div>

  <div v-else class="app-shell">
    <aside class="sidebar">
      <div class="brand">
        <img class="brand-logo" src="/logo.svg" alt="Ya-TG"/>
        <div><div class="brand-name">Ya-TG</div><div class="brand-sub">资源分发工作台</div></div>
      </div>
      <div class="nav-groups">
        <div v-for="g in navGroups" :key="g.label" class="nav-group">
          <div class="nav-group-label">{{ g.label }}</div>
          <button v-for="it in g.items" :key="it.key" class="nav-item" :class="{ active: tab === it.key }" @click="setTab(it.key)">
            <svg class="ico" viewBox="0 0 24 24"><path :d="icon(it.icon)"/></svg>
            <span class="nav-text">{{ it.label }}</span>
          </button>
        </div>
      </div>
      <div class="sidebar-foot">
        <div class="foot-status"><span class="dot" :class="overview.auth === 'ok' ? 'on' : 'off'"></span>{{ overview.auth === 'ok' ? '光鸭已授权' : '光鸭未授权' }}</div>
        <div class="foot-version">
          <span>v2.1.7 · Asia/Shanghai</span>
          <a class="foot-gh" href="https://github.com/Abson2/ya-tg" target="_blank" rel="noopener noreferrer" title="在 GitHub 查看 Ya-TG 开源项目">GitHub</a>
        </div>
      </div>
    </aside>

    <div class="main-wrap">
      <header class="topbar">
        <div class="crumb"><span>Ya-TG</span><b>/</b><strong>{{ tab }}</strong></div>
        <div class="top-actions">
          <span class="clock" :title="'距自动退出还剩 ' + fmtRemaining(sessionRemaining)">{{ clock }}</span>
          <el-button size="small" text :title="isDark ? '切换浅色主题' : '切换深色主题'" @click="toggleTheme">
            <svg class="ico" viewBox="0 0 24 24"><path :d="icon(isDark ? 'sun' : 'moon')"/></svg>
          </el-button>
          <span class="clock" :class="{ 'session-warn': sessionRemaining > 0 && sessionRemaining < 300 }">
            <svg class="ico" viewBox="0 0 24 24" style="width:12px;height:12px;vertical-align:-2px"><path :d="icon('clock')"/></svg>
            会话 {{ fmtRemaining(sessionRemaining) }}
          </span>
          <el-button size="small" plain @click="verify">验证连接</el-button>
          <el-button size="small" type="primary" plain :loading="asyncScanRunning" @click="refreshAll">
            <svg class="ico" viewBox="0 0 24 24"><path :d="icon('refresh')"/></svg> 刷新
          </el-button>
          <el-button size="small" text @click="logout">
            <svg class="ico" viewBox="0 0 24 24"><path :d="icon('logout')"/></svg>
          </el-button>
        </div>
      </header>

      <main class="content">
        <section v-if="tab === '概览'" class="fade">
          <div class="hero">
            <div class="hero-copy"><div class="hero-tag">DASHBOARD</div><h2>运行总览</h2><p>扫描 · 识别 · 建分享 · 分发，全流程状态一目了然</p></div>
            <div class="hero-actions"><span class="hero-pill" :class="overview.auth === 'ok' ? 'pill-on' : 'pill-off'">{{ overview.auth === 'ok' ? '授权正常' : '待授权' }}</span><el-button type="primary" @click="scan"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('bolt')"/></svg> 立即扫描</el-button></div>
          </div>
          <div class="stat-grid">
            <div v-for="s in [
              { k:'resources', label:'资源总数', icon:'folder', tone:'indigo' },
              { k:'sent', label:'已推送', icon:'send', tone:'teal' },
              { k:'pending', label:'待推送', icon:'bolt', tone:'amber' },
              { k:'review', label:'待审核', icon:'check', tone:'violet' },
              { k:'retry', label:'重试中', icon:'clock', tone:'rose' },
              { k:'failed', label:'失败', icon:'shield', tone:'slate' }
            ]" :key="s.k" class="stat-card" :class="'tone-' + s.tone">
              <div class="stat-icon"><svg class="ico" viewBox="0 0 24 24"><path :d="icon(s.icon)"/></svg></div>
              <div class="stat-num">{{ overview[s.k] || 0 }}</div><div class="stat-label">{{ s.label }}</div>
              <div class="stat-bar"><i></i></div>
            </div>
          </div>
          <div class="two-col">
            <div class="panel">
              <div class="panel-head"><h3>系统健康</h3><span>实时检查</span></div>
              <div class="check-list">
                <div v-for="c in healthChecks" :key="c.label" class="check-row">
                  <span class="check-dot" :class="c.ok ? 'ok' : 'no'"><svg class="ico" viewBox="0 0 24 24"><path :d="icon(c.ok ? 'check' : 'plus')"/></svg></span>
                  <div class="check-copy"><b>{{ c.label }}</b><small>{{ c.hint }}</small></div>
                  <span class="check-state" :class="c.ok ? 'ok' : 'no'">{{ c.ok ? '正常' : '待配置' }}</span>
                </div>
              </div>
            </div>
            <div class="panel">
              <div class="panel-head"><h3>队列分布</h3><span>{{ (overview.pending||0)+(overview.review||0)+(overview.retry||0)+(overview.sent||0)+(overview.failed||0) }} 项</span></div>
              <div class="dist-list">
                <div v-for="p in [
                  { k:'sent', label:'已推送', c:'#19b3a6' }, { k:'pending', label:'待推送', c:'#f2a33c' },
                  { k:'review', label:'待审核', c:'#8b6cf0' }, { k:'retry', label:'重试中', c:'#e5526b' },
                  { k:'failed', label:'失败', c:'#a3abc4' }
                ]" :key="p.k" class="dist-row">
                  <span class="dist-label">{{ p.label }}</span>
                  <div class="dist-track"><i :style="{ width: Math.min(100, (overview[p.k]||0) * 12) + '%', background: p.c }"></i></div>
                  <b>{{ overview[p.k] || 0 }}</b>
                </div>
              </div>
            </div>
          </div>
          <!-- v0.9.8 #194：推送效果报表（首页概览，只展示真实可得指标） -->
          <div class="panel">
            <div class="panel-head">
              <h3>推送效果报表</h3>
              <div class="head-tools">
                <el-select v-model="statsPeriod" size="small" style="width:96px" @change="loadPushStats">
                  <el-option label="按日" value="day"/><el-option label="按周" value="week"/><el-option label="按月" value="month"/>
                </el-select>
                <el-select v-model="statsDays" :value="30" size="small" style="width:110px" @change="loadPushStats">
                  <el-option :value="7" label="近 7 天"/><el-option :value="30" label="近 30 天"/><el-option :value="90" label="近 90 天"/>
                </el-select>
                <el-button size="small" :loading="statsLoading" @click="loadPushStats">刷新</el-button>
              </div>
            </div>
            <div class="stats-cards">
              <div class="stat-card"><div class="stat-num">{{ pushStats.totals.sent || 0 }}</div><div class="stat-label">发送成功</div></div>
              <div class="stat-card"><div class="stat-num">{{ pushStats.totals.failed || 0 }}</div><div class="stat-label">发送失败</div></div>
              <div class="stat-card"><div class="stat-num">{{ pushStats.totals.success_rate || 0 }}%</div><div class="stat-label">成功率</div></div>
              <div class="stat-card"><div class="stat-num">{{ pushStats.totals.retries || 0 }}</div><div class="stat-label">重试次数</div></div>
              <div class="stat-card"><div class="stat-num">{{ pushStats.totals.events || 0 }}</div><div class="stat-label">分发更新</div></div>
            </div>
            <div v-if="pushStats.series && pushStats.series.length" class="chart-wrap">
              <div class="chart-bars">
                <div v-for="row in pushStats.series" :key="row.label" class="chart-col" :title="`${row.label}：成功 ${row.sent} / 失败 ${row.failed} / 成功率 ${row.success_rate}%`">
                  <div class="chart-stack">
                    <i class="bar-fail" :style="{ height: statBarHeight(row.failed, Math.max(1, ...pushStats.series.map(x => x.sent + x.failed))) + '%' }"></i>
                    <i class="bar-sent" :style="{ height: statBarHeight(row.sent, Math.max(1, ...pushStats.series.map(x => x.sent + x.failed))) + '%' }"></i>
                  </div>
                  <small>{{ row.label.slice(5) }}</small>
                </div>
              </div>
              <div class="chart-legend"><span><i class="lg-sent"></i>成功</span><span><i class="lg-fail"></i>失败</span></div>
            </div>
            <div v-else class="empty-hint">所选时间范围内暂无推送记录</div>
            <el-table v-if="pushStats.series && pushStats.series.length" :data="pushStats.series" size="small" style="margin-top:14px">
              <el-table-column prop="label" label="时间"/>
              <el-table-column prop="sent" label="发送成功" width="100"/>
              <el-table-column prop="failed" label="发送失败" width="100"/>
              <el-table-column prop="retries" label="重试次数" width="100"/>
              <el-table-column prop="events" label="分发更新" width="100"/>
              <el-table-column prop="success_rate" label="成功率" width="100"><template #default="s">{{ s.row.success_rate }}%</template></el-table-column>
            </el-table>
            <div v-if="pushStats.unavailable && pushStats.unavailable.length" class="unavailable-box">
              <b>暂不可得的指标（不造假数）：阅读数 / 点击数 / 转存数</b>
              <div v-for="x in pushStats.unavailable" :key="x.key" class="unavailable-row"><span>{{ x.label }}</span><small>{{ x.reason }}</small></div>
            </div>
          </div>
          <!-- v0.9.8 #195：首页概览可见的镜像处理流程节点 -->
          <div class="panel" v-if="mirrorStatus.pipeline && mirrorStatus.pipeline.length">
            <div class="panel-head"><h3>镜像 → 入库 → 推送 进度</h3><button class="link-btn" @click="tab='镜像同步'">查看详情 →</button></div>
            <div class="pipeline-flow home-flow">
              <div v-for="node in mirrorStatus.pipeline" :key="node.key" class="pipe-node" :class="'is-' + (node.status || 'idle')">
                <div class="pipe-label">{{ node.label }}</div>
                <div class="pipe-value">{{ node.value }}<span v-if="node.total">/{{ node.total }}</span></div>
              </div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-head"><h3>最近操作</h3><button class="link-btn" @click="tab='操作审计'">查看全部 →</button></div>
            <div class="timeline">
              <div v-for="a in audit.slice(0, 6)" :key="a.id" class="tl-row">
                <span class="tl-dot"></span><span class="tl-time">{{ a.ts.slice(11, 19) }}</span>
                <b class="tl-action">{{ a.action }}</b><span class="tl-detail">{{ a.detail || a.target_type }}</span>
              </div>
              <div v-if="!audit.length" class="empty">暂无操作记录</div>
            </div>
          </div>
        </section>

        <!-- 阶段六：任务中心——8 类任务的纯聚合视图，数据源全部来自已加载 ref，不造数 -->
        <section v-else-if="tab === '任务中心'" class="fade">
          <div class="page-head"><div><h2>任务中心</h2><p>镜像 / 复制 / 改名 / 翻译 / 分享 / 推送 / 投稿 / 求片，分类聚合真实任务状态</p></div>
            <el-button plain :loading="asyncScanRunning" @click="refreshAll"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('refresh')"/></svg> 刷新数据</el-button></div>
          <div class="task-notice">本页为跨模块聚合视图：所有数字来自各原始页面已加载的真实数据，点击「查看明细」跳转到对应页面操作；无数据的分类显示 0，不做估算。</div>
          <div class="task-grid">
            <div v-for="c in taskCenter" :key="c.key" class="panel task-card" :class="'task-' + c.status">
              <div class="task-card-head">
                <div class="task-title"><svg class="ico" viewBox="0 0 24 24"><path :d="icon(c.icon)"/></svg><b>{{ c.label }}</b></div>
                <span class="task-state" :class="'st-' + c.status">{{ ({ active:'进行中', done:'已完成', warn:'需关注', idle:'空闲' })[c.status] || c.status }}</span>
              </div>
              <div class="task-metrics">
                <div v-for="m in c.metrics" :key="m.label" class="task-metric">
                  <span class="tm-value" :class="m.tone ? 'tm-' + m.tone : ''">{{ m.value }}</span>
                  <span class="tm-label">{{ m.label }}</span>
                </div>
              </div>
              <div class="task-card-foot"><small>{{ c.note }}</small><button class="link-btn" @click="setTab(c.target)">查看明细 →</button></div>
            </div>
          </div>
        </section>

        <section v-else-if="tab === '光鸭授权'" class="fade">
          <div class="page-head"><div><h2>光鸭授权</h2><p>推荐用短信一键登录，自动完成全部凭证；下方为高级备用方式</p></div><el-button plain @click="loadAuth">刷新状态</el-button></div>

          <div class="panel connect-hero">
            <div class="connect-head">
              <div><h3>一键连接光鸭</h3><p>手机号 + 短信验证码，自动获取并定期刷新全部凭证；内置实现，不依赖任何第三方项目</p></div>
              <el-tag :type="smsStatus.logged_in ? 'success' : 'info'" size="small">{{ smsStatus.logged_in ? '已连接' : '待连接' }}</el-tag>
            </div>
            <div class="connect-steps">
              <div class="cstep" :class="{active: smsStep===1, done: smsStep>1}"><b>1</b><span>填写手机号</span></div>
              <div class="cstep" :class="{active: smsStep===2, done: smsStep>2}"><b>2</b><span>输入验证码</span></div>
              <div class="cstep" :class="{active: smsStep===3, done: smsStep===3}"><b>3</b><span>连接完成</span></div>
            </div>
            <div v-if="smsStep===1" class="connect-form">
              <el-input v-model="smsPhone" placeholder="光鸭账号手机号（如 13800138000）" maxlength="20"/>
              <div class="connect-actions"><el-button type="primary" :loading="smsBusy" @click="sendSms">获取验证码</el-button></div>
              <p class="hint">验证码由光鸭官方账号服务发送。只需手机号，无需申请应用、无需抓包。</p>
            </div>
            <div v-else-if="smsStep===2" class="connect-form">
              <el-input v-model="smsCode" placeholder="短信验证码" maxlength="10"/>
              <div class="connect-actions"><el-button :loading="smsBusy" type="primary" @click="verifySms">登录并连接</el-button><el-button plain @click="resetSms">上一步</el-button></div>
              <p class="hint">验证码验证后将自动登录，凭证加密保存在本机数据库。</p>
            </div>
            <div v-else class="connect-done">
              <el-result icon="success" title="光鸭已连接" sub-title="凭证已加密保存，系统会自动定期刷新；扫描与建分享无需再配置。">
                <template #extra><el-button type="primary" @click="resetSms">重新登录</el-button><el-button plain @click="refreshCredential">立即刷新凭证</el-button></template>
              </el-result>
            </div>
            <div class="connect-foot">
              <span>状态：{{ smsStatus.message || '—' }}</span>
              <span v-if="smsStatus.account">账号：{{ smsStatus.account }}</span>
              <el-tag v-if="smsStatus.logged_in" :type="smsStatus.auto_refresh ? 'success' : 'warning'" size="small" effect="plain">
                {{ smsStatus.auto_refresh ? '自动刷新：开启' : '自动刷新：不可用' }}
              </el-tag>
            </div>
          </div>

          <!-- v0.9.2：网页端扫码登录，与短信并列，免开放平台应用凭证 -->
          <div class="panel connect-hero">
            <div class="connect-head">
              <div><h3>扫码登录（光鸭 App）</h3><p>无需手机号、无需申请开放平台应用；用光鸭 App 扫一扫，在 App 内确认即可完成授权</p></div>
              <el-tag :type="auth.webqr_supported ? 'success' : 'info'" size="small">{{ auth.webqr_supported ? '可用' : '不可用' }}</el-tag>
            </div>
            <div v-if="!webqr" class="connect-form">
              <div class="connect-actions"><el-button type="primary" :disabled="!auth.webqr_supported" @click="startWebqr">发起扫码登录</el-button></div>
              <p class="hint">点击后系统会申请一个一次性授权码；用光鸭 App 扫一扫完成确认。二维码地址仅供 App 内授权，不能直接用浏览器打开。</p>
            </div>
            <div v-else class="device-panel">
              <div class="panel-head"><h3>请在光鸭 App 中扫码授权</h3><span class="pulse">等待确认…</span></div>
              <div v-if="webqrPng" class="webqr-box"><img :src="webqrPng" alt="光鸭扫码授权二维码"/></div>
              <div class="webqr-code">授权码：<b>{{ webqr.user_code }}</b></div>
              <div class="device-actions"><el-button plain @click="copyWebqrCode">复制授权码</el-button><el-button plain @click="cancelWebqr">取消</el-button></div>
              <p class="hint">用<b>光鸭 App 的扫一扫</b>扫描上方二维码（不能用浏览器打开）；扫码后在 App 内核对并确认。二维码有效期 {{ webqr.expires_in }} 秒，系统每 {{ webqr.interval }} 秒自动查询一次，确认后自动完成连接。</p>
            </div>
          </div>

          <!-- 凭证有效期与自动刷新状态：以短信登录状态为准 -->
          <div v-if="smsStatus.logged_in" class="credential-bar">
            <div class="cred-cell">
              <small>凭证状态</small>
              <b class="good"><span class="dot live"></span>已连接</b>
            </div>
            <div class="cred-cell">
              <small>到期倒计时</small>
              <b :class="smsStatus.needs_refresh ? 'warn' : ''">{{ smsCountdown || '计算中…' }}</b>
            </div>
            <div class="cred-cell">
              <small>自动刷新</small>
              <b :class="smsStatus.auto_refresh ? 'good' : 'bad'">{{ smsStatus.auto_refresh ? '已开启 · 到期前自动续期' : '不可用 · 请重新登录' }}</b>
            </div>
            <div class="cred-cell">
              <small>过期时间（北京时间）</small>
              <b>{{ fmtBj(smsStatus.expires_at) }}</b>
            </div>
          </div>

          <div class="auth-summary">
            <div class="auth-summary-item"><small>授权状态</small><b :class="smsStatus.logged_in || auth.authenticated ? 'good' : 'bad'">{{ smsStatus.logged_in ? '已授权（短信登录）' : (auth.authenticated ? '已授权' : '未授权') }}</b></div>
            <div class="auth-summary-item"><small>授权方式</small><b>{{ smsStatus.logged_in ? '短信验证码（自研）' : (auth.flow || '—') }}</b></div>
            <div class="auth-summary-item"><small>用户 ID（已脱敏）</small><b>{{ smsStatus.account || auth.sub || '—' }}</b></div>
            <div class="auth-summary-item"><small>过期时间</small><b>{{ fmtBj(smsStatus.expires_at) || auth.expires_at || '—' }}</b></div>
          </div>
          <div class="panel">
            <div class="panel-head"><h3>网页分享会话</h3><el-tag :type="shareStatus.configured ? 'success' : 'warning'" size="small">{{ shareStatus.configured ? '已配置' : '未配置' }}</el-tag></div>
            <div class="kv-grid"><div><small>来源</small><b>{{ shareStatus.source || '—' }}</b></div><div><small>DID</small><b>{{ shareStatus.did_set ? '已设置' : '未设置' }}</b></div><div><small>SMID</small><b>{{ shareStatus.smid_set ? '已设置' : '未设置' }}</b></div><div><small>刷新</small><el-button size="small" :disabled="!auth.has_refresh_token" @click="refreshCredential">立即刷新</el-button></div></div>
            <p class="hint">{{ shareStatus.note }}</p>
          </div>
        </section>

        <section v-else-if="tab === '镜像同步'" class="fade">
          <div class="page-head"><div><h2>镜像同步</h2><p>A 源头库只读 → B 镜像库；单集/电影文件按英文剧名改名 → 自动刷新入库 → 识别 → 按分发规则推送本轮新识别影视</p></div><el-tag :type="mirrorCfg.enabled ? 'success' : 'info'" size="small">{{ mirrorCfg.enabled ? '已启用' : '未启用' }}</el-tag></div>
          <!-- v0.9.8 #195：流程节点 + 同步进度，用户能看出卡在哪一步 -->
          <div class="panel">
            <div class="panel-head"><h3>处理流程与进度</h3><span class="hint">{{ mirrorStatus.progress?.running ? '任务运行中 · 每 2 秒自动刷新' : '实时状态 · 每 2 秒自动刷新' }}</span></div>
            <div class="sync-progress">
              <div class="sync-progress-track"><i :style="{ width: (mirrorStatus.progress?.percent || 0) + '%' }"></i></div>
              <span>{{ mirrorStatus.progress?.percent || 0 }}%<template v-if="mirrorStatus.progress?.running"> · {{ stageLabel(mirrorStatus.progress?.stage) }}</template><template v-else>（{{ mirrorStatus.progress?.synced || 0 }}/{{ mirrorStatus.progress?.total || 0 }} 文件已同步）</template></span>
            </div>
            <div class="pipeline-flow">
              <div v-for="node in (mirrorStatus.pipeline || [])" :key="node.key" class="pipe-node" :class="'is-' + (node.status || 'idle')">
                <div class="pipe-label">{{ node.label }}</div>
                <div class="pipe-value">{{ node.value }}<span v-if="node.total">/{{ node.total }}</span></div>
                <div class="pipe-detail">{{ node.detail }}</div>
              </div>
            </div>
            <p class="hint" style="margin-top:12px">任务运行中每 2 秒自动刷新，实时显示复制 / 改名 / 入库 / 分享 / 入队各阶段数量。「入库识别 / 生成分享 / 推送入队」按 TMDB 作品去重显示<b>剧集部数 / 电影部数</b>（非集数）；进度条只有分享队列清空、全部推送完成后才到 100%。若本轮入队为 0，节点会显示拦截原因（无海报 / 路由未匹配 / 未配置目标等）。</p>
          </div>
          <!-- v0.9.9 #200：A/B 库设置与同步状态并列，减少纵向滚动 -->
          <div class="two-col mirror-two-col">
          <div class="panel mirror-col">
            <div class="panel-head"><h3>A / B 库设置</h3></div>
            <div class="form-grid">
              <div class="field"><label>A 源头库目录 ID（只读）</label><el-input v-model="mirrorCfg.source_root_id" placeholder="光鸭网页端目录 fileId"/></div>
              <div class="field"><label>B 镜像库目录 ID（用于分享与推送）</label><el-input v-model="mirrorCfg.target_root_id" placeholder="光鸭网页端目录 fileId"/></div>
            </div>
            <div class="field"><label>同步间隔</label>
              <div class="preset-row">
                <button v-for="v in (mirrorCfg.interval_presets || [])" :key="v" class="preset-btn" :class="{on: mirrorCfg.interval_minutes === v}" @click="mirrorCfg.interval_minutes = v">{{ intervalLabel(v) }}</button>
              </div>
            </div>
            <div class="form-grid">
              <div class="field inline"><label>启用定时同步</label><el-switch v-model="mirrorCfg.enabled"/></div>
              <div class="field inline"><label>B 库主目录带 · 命名</label><el-switch v-model="mirrorCfg.star_naming"/></div>
              <div class="field inline"><label>B 库缺目录自动创建</label><el-switch v-model="mirrorCfg.auto_create_dirs"/></div>
              <div class="field inline"><label>同步后自动入库识别并推送</label><el-switch v-model="mirrorCfg.auto_identify"/></div>
              <div class="field inline"><label>B 库单集/电影文件英文改名</label><el-switch v-model="mirrorCfg.rename_files"/></div>
              <div class="field"><label>每个文件操作后的空闲时间（秒）</label><el-input-number v-model="mirrorCfg.file_idle_seconds" :min="mirrorCfg.min_idle_seconds || 0.5" :max="60" :step="0.5" :precision="1" controls-position="right" style="width:150px"/><small>硬下限 {{ mirrorCfg.min_idle_seconds || 0.5 }} 秒来自官方写接口 2 次/秒限频，不可调低；复制、建目录、改名、变化更新的每个文件操作前都会等待。</small></div>
              <div class="field"><label>单轮最多整理整部剧 / 电影（部）</label><el-input-number v-model="mirrorCfg.max_works_per_run" :min="0" :max="1000" :step="1" controls-position="right" style="width:150px"/><small>0 = 不限量（一次整理全部）。设 N 则每轮只整理 N 部就停，剩余等下一次定时同步接着做；同一部剧会一次整理完再休息，不会拆到两轮。进度按作品轮转，每部都能轮到。</small></div>
              <div class="field"><label>镜像模式（A 库已删文件时）</label>
                <el-select v-model="mirrorCfg.mirror_mode" style="width:220px">
                  <el-option label="全镜像（默认，删 B 库对应文件）" value="full"/>
                  <el-option label="半镜像（保留 B 库文件与映射）" value="half"/>
                </el-select>
                <small><b>A 库始终只读，两种模式都不写 A。</b>全镜像：A 库文件/作品已删除时，单轮预扫描会<b>自动删除 B 库对应文件并清理映射清单</b>（仅 A 目录树完整读取成功时才删，避免误删）；B 与 A 保持一致。半镜像：A 已删时 B 库文件与映射清单<b>都原样保留</b>，B 作为 A 的超集，不做任何删除。删除不可逆，请按你的备份策略选择。</small>
              </div>
            </div>
            <el-collapse v-model="mirrorDiagOpen" class="mirror-fold">
              <el-collapse-item name="config-help">
                <template #title><span class="fold-title">配置说明 · 改名规则 · 链路口径</span></template>
            <p class="hint">开启「同步后自动入库识别并推送」后，同步结束会刷新 B 库入库、识别影视，并把<b>本轮识别成功且拿到海报</b>的影视按 TG 分发规则入队；不会对全库群发，也不会批量补推历史内容变化事件。关闭后定时同步与「立即同步」都只复制文件，需要到资源库手动点「立即扫描」。</p>
            <p class="hint"><b>v0.8.9 英文改名</b>：开启后按英文剧名改 B 库<b>单集 / 单电影文件名</b>，<b>只替换中文片名段</b>，季集号 / 年份 / 画质 / 压制组 / 扩展名一律原样保留。A 库只读、B 库分类目录与影视主目录命名不变；A→B 映射靠 source_file_id + 文件指纹保持，改名不影响后期变量更新比对。存量已同步文件点「按英文规范改名」一键处理，不重新复制。<b>TMDB 上查不到官方英文名时不再等待重试</b>：直接用 AI 直译，AI 未配置或超时则本地拼音兜底，保证永远有可用剧名、不卡住复制 / 入库 / 分享。同步、改名、任务轮询共用一把锁，同一时间只跑一个长任务；重复点击会提示「已有任务正在运行」。</p>
            <p class="hint">{{ mirrorCfg.note }}</p>
              </el-collapse-item>
            </el-collapse>
            <div v-if="mirrorStatus.lock?.busy && !mirrorBusy" class="mirror-lock-tip"><el-tag type="warning" size="small">后台任务运行中</el-tag> {{ mirrorStatus.lock.owner || '镜像任务' }} 已运行约 {{ mirrorStatus.lock.held_seconds }} 秒；同步 / 改名共用一把锁，等它结束再点「立即同步」即可，无需刷新页面。</div>
            <!-- 阶段二路线B C4：严格顺序引擎与批次控制 -->
            <div class="batch-control-row">
              <span class="hint">严格顺序引擎</span>
              <el-switch v-model="mirrorStrict" size="small"/>
              <span class="hint">一个文件复制确认完成后才提交下一个，可暂停 / 恢复 / 跳过 / 取消。</span>
              <el-tag v-if="batchInfo.batch_id" size="small" :type="batchInfo.status === 'paused' ? 'warning' : batchInfo.status === 'running' ? 'success' : 'info'">{{ batchInfo.status || 'idle' }} · {{ batchInfo.done || 0 }}/{{ batchInfo.total || 0 }} · 序列 {{ batchInfo.current_sequence || 0 }}</el-tag>
              <el-button size="small" :disabled="!mirrorStrict" @click="batchControl('pause')">暂停</el-button>
              <el-button size="small" :disabled="!mirrorStrict" @click="batchControl('resume')">恢复</el-button>
              <el-button size="small" :disabled="!mirrorStrict" @click="batchControl('skip')">跳过当前</el-button>
              <el-button size="small" type="danger" :disabled="!mirrorStrict" @click="batchControl('cancel')">取消批次</el-button>
            </div>
            <div class="panel-foot"><el-button type="primary" :loading="mirrorSaving" @click="saveMirror">保存设置</el-button><el-button type="success" :loading="mirrorBusy" :disabled="mirrorBusy" @click="runMirror('auto')">立即同步</el-button><el-button :loading="mirrorBusy" :disabled="mirrorBusy" @click="runMirror('force')">强制入库识别</el-button><el-button :loading="mirrorBusy" :disabled="mirrorBusy" @click="runMirror('skip')">仅同步</el-button><el-button :loading="mirrorBusy" :disabled="mirrorBusy" @click="renameMirrorFiles(false)">按英文规范改名</el-button><el-button :loading="mirrorBusy" :disabled="mirrorBusy" @click="pollMirrorTasks">继续轮询任务</el-button></div>
          </div>
          <div class="panel mirror-col">
            <div class="panel-head"><h3>同步状态</h3><el-tag size="small">{{ mirrorStatus.rule?.last_status || 'never' }}</el-tag></div>
            <div class="stat-group">
              <div class="stat-group-title"><span class="grp-ico">⇄</span>文件传输</div>
              <div class="mini-stat-grid">
                <div class="mini-stat"><small>影视映射</small><b>{{ mirrorStatus.counts?.media || 0 }}</b></div>
                <div class="mini-stat"><small>文件映射</small><b>{{ mirrorStatus.counts?.files || 0 }}</b></div>
                <div class="mini-stat"><small>已同步</small><b>{{ mirrorStatus.counts?.synced || 0 }}</b></div>
                <div class="mini-stat"><small>复制失败</small><b :style="mirrorStatus.counts?.files_failed ? 'color:#d63f59' : ''">{{ mirrorStatus.counts?.files_failed || 0 }}</b></div>
                <div class="mini-stat"><small>待重试文件</small><b>{{ mirrorStatus.counts?.files_retryable || 0 }}</b></div>
                <div class="mini-stat"><small>被替代</small><b>{{ mirrorStatus.counts?.superseded || 0 }}</b></div>
              </div>
            </div>
            <div class="stat-group">
              <div class="stat-group-title"><span class="grp-ico g-teal">文</span>英文改名</div>
              <div class="mini-stat-grid">
                <div class="mini-stat"><small>已改英文名</small><b>{{ mirrorStatus.counts?.files_renamed || 0 }}</b></div>
                <div class="mini-stat"><small>改名失败</small><b :style="mirrorStatus.counts?.files_rename_failed ? 'color:#d63f59' : ''">{{ mirrorStatus.counts?.files_rename_failed || 0 }}</b></div>
                <div class="mini-stat"><small>待翻译作品</small><b :style="mirrorStatus.counts?.works_without_english ? 'color:#d63f59' : ''">{{ mirrorStatus.counts?.works_without_english || 0 }}</b></div>
                <!-- v2.1.4：被标记忽略的作品单独统计并透明展示，避免「清空后红字仍在」的错觉 -->
                <div class="mini-stat"><small>已忽略解析</small><b :style="mirrorStatus.counts?.works_english_ignored ? 'color:#cf7f1c' : ''">{{ mirrorStatus.counts?.works_english_ignored || 0 }}</b></div>
              </div>
            </div>
            <div class="stat-group">
              <div class="stat-group-title"><span class="grp-ico g-amber">▤</span>目录</div>
              <div class="mini-stat-grid">
                <div class="mini-stat"><small>已映射</small><b>{{ mirrorStatus.counts?.dirs_mapped || 0 }}</b></div>
                <div class="mini-stat"><small>自动创建</small><b>{{ mirrorStatus.counts?.dirs_created || 0 }}</b></div>
                <div class="mini-stat"><small>待创建</small><b>{{ mirrorStatus.counts?.dirs_missing || 0 }}</b></div>
              </div>
            </div>
            <div class="stat-group">
              <div class="stat-group-title"><span class="grp-ico g-violet">◷</span>任务 / 风控</div>
              <div class="mini-stat-grid">
                <div class="mini-stat"><small>待确认任务</small><b>{{ mirrorStatus.counts?.tasks_unknown || 0 }}</b></div>
                <div class="mini-stat"><small>失败任务</small><b>{{ mirrorStatus.counts?.tasks_failed || 0 }}</b></div>
                <div class="mini-stat"><small>文件操作下限</small><b>{{ mirrorStatus.min_idle_seconds || 0.5 }}s</b></div>
              </div>
            </div>
            <p class="hint">{{ mirrorStatus.rule?.last_message || '尚未执行同步' }}</p>
            <el-collapse v-model="mirrorDiagOpen" class="mirror-fold">
              <el-collapse-item name="status-detail">
                <template #title><span class="fold-title">生效配置 · 已确认接口 · 待验证接口</span></template>
            <div class="info-chips">
              <span class="info-chip" :class="mirrorStatus.config?.auto_create_dirs === false ? 'off' : 'on'">缺目录自动创建 <b>{{ mirrorStatus.config?.auto_create_dirs === false ? '关闭·会全部 blocked' : '开启' }}</b></span>
              <span class="info-chip">主目录 · 标记 <b>{{ mirrorStatus.config?.star_naming === false ? '关闭' : '开启' }}</b></span>
              <span class="info-chip" :class="mirrorStatus.config?.auto_identify === false ? 'off' : 'on'">同步后入库识别推送 <b>{{ mirrorStatus.config?.auto_identify === false ? '关闭' : '开启' }}</b></span>
              <span class="info-chip" :class="mirrorStatus.config?.rename_files === false ? 'off' : 'on'">单集/电影英文改名 <b>{{ mirrorStatus.config?.rename_files === false ? '关闭' : '开启' }}</b></span>
              <span class="info-chip">B 库目录 ID <b>{{ mirrorStatus.config?.target_root_id || '未填写' }}</b></span>
              <span class="info-chip">文件空闲 <b>{{ mirrorStatus.config?.file_idle_seconds ?? 1.0 }}s</b></span>
              <span class="info-chip">单轮作品上限 <b>{{ mirrorStatus.config?.max_works_per_run ? mirrorStatus.config.max_works_per_run + ' 部' : '不限量' }}</b></span>
            </div>
            <p class="hint" style="margin-top:14px">已确认接口（来自 HAR 取证）</p>
            <div class="endpoint-list"><code v-for="e in (mirrorCfg.confirmed_endpoints || [])" :key="e">{{ e }}</code></div>
            <p class="hint" style="margin-top:12px">待验证接口（未实现，不猜测参数）</p>
            <div class="endpoint-list"><code v-for="e in (mirrorStatus.unverified_endpoints || [])" :key="e">{{ e }}</code></div>
              </el-collapse-item>
            </el-collapse>
          </div>
          </div>
          <el-collapse v-model="mirrorDiagOpen" class="mirror-fold">
            <el-collapse-item name="diagnostics">
              <template #title>
                <span class="fold-title">问题诊断</span>
                <el-tag v-if="diagCount" type="warning" size="small" style="margin-left:8px">{{ diagCount }} 项待处理</el-tag>
                <span v-else class="fold-sub">无待处理项</span>
              </template>
          <div v-if="mirrorStatus.missing_dirs?.length" class="panel">
            <div class="panel-head"><h3>待创建目录</h3><el-tag type="warning" size="small">{{ mirrorStatus.missing_dirs.length }} 个</el-tag><el-button size="small" type="primary" plain style="margin-left:auto" :loading="mirrorBusy" :disabled="mirrorBusy || !missingDirSelected.length" @click="retryFailedDirs">批量重建选中 ({{ missingDirSelected.length }})</el-button></div>
            <div class="table-scroll"><el-table :data="mirrorStatus.missing_dirs" size="small" row-key="id" @selection-change="v => missingDirSelected = v.map(x => x.id)">
              <el-table-column type="selection" width="42" align="center"/>
              <el-table-column prop="name" label="B 库应建目录名" min-width="200"/>
              <el-table-column prop="kind" label="类别" width="100"/>
              <el-table-column prop="parent_id" label="父目录 ID" min-width="180"/>
              <el-table-column label="失败原因 / 处理建议" min-width="320"><template #default="s"><span style="color:#c0392b">{{ s.row.error || s.row.tip }}</span></template></el-table-column>
            </el-table></div>
            <p class="hint">这里显示的是 create_dir 接口的真实报错（或「自动创建已关闭」），不再用通用文案掩盖。v0.8.3 起主目录直接用 ·（网盘拒绝 * 返回 code=141）；只有连 · 也拒绝时，才会降级为无标记标准名重试一次。<b>v0.9.17.2：勾选后点「批量重建选中」只对这些目录重发 create_dir，不扫描 A 源头库；重建成功后下轮同步自动映射其下文件。</b></p>
          </div>
          <div v-if="mirrorStatus.counts?.dirs_pruned_zombie" class="panel">
            <div class="panel-head"><h3>已清理待重建目录映射</h3><el-tag type="warning" size="small">{{ mirrorStatus.counts.dirs_pruned_zombie }} 条</el-tag><el-button size="small" type="danger" plain :loading="mirrorBusy" :disabled="mirrorBusy" @click="clearPrunedMappings">清空已清理映射</el-button></div>
            <p class="hint">这些是历史「清理无效映射」留下的僵尸行：B 库目录已不存在、映射已清，下轮同步会从 A 库自动重建，<b>不需要也不应该手工建目录</b>。旧版本无标记的行现在同样识别；确认后点「清空已清理映射」即可从清单彻底移除，不影响资源 / 分享 / 队列。</p>
          </div>
          <div v-if="mirrorStatus.file_errors?.length" class="panel">
            <div class="panel-head"><h3>文件复制失败 / 待重试</h3><el-tag type="danger" size="small">{{ mirrorStatus.file_errors.length }} 个</el-tag><el-button size="small" type="primary" plain style="margin-left:auto" :loading="mirrorBusy" :disabled="mirrorBusy || !fileErrSelected.length" @click="retryFailedFiles">批量重试选中 ({{ fileErrSelected.length }})</el-button></div>
            <div class="table-scroll"><el-table :data="mirrorStatus.file_errors" size="small" row-key="id" @selection-change="v => fileErrSelected = v.map(x => x.id)">
              <el-table-column type="selection" width="42" align="center"/>
              <el-table-column prop="work" label="作品" min-width="160"/>
              <el-table-column prop="file" label="文件名" min-width="200"/>
              <el-table-column prop="task_id" label="任务 ID" min-width="140"/>
              <el-table-column label="失败原因" min-width="320"><template #default="s"><span style="color:#c0392b">{{ s.row.error || '未记录原因' }}</span></template></el-table-column>
            </el-table></div>
            <p class="hint">这里显示 copy_file / get_task_status 的真实报错，已从数据库持久化，刷新页面仍在。v0.8.4 起失败任务会标 status=3 或 detail.code≠0 为明确失败（旧版误记为「待确认」）；<b>v0.9.17.2：勾选后点「批量重试选中」只对这些文件定向重发复制——只扫描其所属作品的 A 库目录、不扫整个源头库，B 库已存在的直接回填不重复复制，成功后自动走入库→识别→推送入队</b>；也可等下一轮「立即同步」全量重试。若全部报同一错误，请按「待验证接口」提示补抓一次网页端「复制到」的 HAR。</p>
          </div>
          <div v-if="mirrorStatus.untranslated_works?.length || mirrorStatus.rename_errors?.length || mirrorStatus.counts?.works_english_ignored" class="panel">
            <div class="panel-head"><h3>英文改名待处理 / 失败</h3><el-tag type="warning" size="small">{{ mirrorStatus.untranslated_works?.length || 0 }} 部待翻译 · {{ mirrorStatus.rename_errors?.length || 0 }} 个失败</el-tag><el-tag v-if="mirrorStatus.counts?.works_english_ignored" type="info" size="small">已忽略 {{ mirrorStatus.counts.works_english_ignored }} 部</el-tag><el-tag v-if="mirrorStatus.counts?.files_rename_skipped_legacy" type="info" size="small">历史不回刷 {{ mirrorStatus.counts.files_rename_skipped_legacy }} 个</el-tag></div>
            <div v-if="mirrorStatus.untranslated_works?.length" class="table-scroll"><el-table :data="mirrorStatus.untranslated_works" size="small">
              <el-table-column prop="work" label="作品（A 库原名）" min-width="220"/>
              <el-table-column prop="reason" label="原因 / 处理建议" min-width="340"/>
            </el-table></div>
            <div v-if="mirrorStatus.rename_errors?.length" class="table-scroll" style="margin-top:10px"><el-table :data="mirrorStatus.rename_errors" size="small">
              <el-table-column prop="work" label="作品" min-width="160"/>
              <el-table-column prop="file" label="A 库原名" min-width="200"/>
              <el-table-column prop="target" label="目标英文名" min-width="200"/>
              <el-table-column label="失败原因" min-width="280"><template #default="s"><span style="color:#c0392b">{{ s.row.error || '未记录原因' }}</span></template></el-table-column>
            </el-table></div>
            <!-- v2.1.4：按钮不再随清单为空自动消失——否则用户清空后就再也无法从界面
                 手动恢复（只剩「重新解析英文名并改名」一条路）。清单为空时按钮禁用，
                 并在下方提示已忽略作品数（A 库出现新集会自动恢复解析）。 -->
            <div class="panel-foot"><el-button :loading="mirrorBusy" :disabled="mirrorBusy" @click="renameMirrorFiles(true)">重新解析英文名并改名</el-button><el-button :loading="mirrorBusy" :disabled="mirrorBusy || !mirrorStatus.untranslated_works?.length" plain @click="clearUntranslated">清空待处理清单</el-button></div>
            <p class="hint">英文剧名按 TMDB en-US 官方片名优先、<b>查不到就直接直译</b>（AI 直译 → 本地拼音兜底），<b>每部作品只解析一次并缓存</b>；点「重新解析英文名并改名」会清缓存重查。v0.9.6.1 按你的口径改为：<b>旧版只改文件夹、不改单集的历史条目不再回刷</b>，升级前未完成改名的行标记「历史不回刷」，只有升级后新同步的文件自动改英文名；确需处理历史行时点「重新解析英文名并改名」显式解除。改名失败通常是网盘拒绝了文件名字符或 B 库 fileId 失效，修正后点「按英文规范改名」重试即可；<b>不会重新复制文件、不会动 A 库，也不影响入库与分享</b>。</p>
            <p v-if="mirrorStatus.counts?.works_english_ignored" class="hint" style="color:#cf7f1c"><b>有 {{ mirrorStatus.counts.works_english_ignored }} 部作品被标记为「已忽略」英文名解析</b>（此前点「清空待处理清单」所致）。它们<b>不会被永久拉黑</b>：一旦 A 库出现新集，同步会自动解除标记并重新解析；也可点上方「重新解析英文名并改名」立即全部恢复。</p>
          </div>
            </el-collapse-item>
            <el-collapse-item name="mappings">
              <template #title><span class="fold-title">目录映射 / 影视映射清单</span></template>
          <div class="panel">
            <div class="panel-head"><h3>目录映射清单</h3><el-tag size="small" type="info">分类目录原名 1:1 · 影视主目录带 ·</el-tag><el-tag v-if="mirrorStatus.counts?.dirs_pruned_zombie" size="small" type="warning">{{ mirrorStatus.counts.dirs_pruned_zombie }} 条已清理待重建</el-tag><el-button size="small" :loading="mirrorBusy" :disabled="mirrorBusy" @click="pruneMirrorMappings">清理无效映射</el-button><el-button size="small" type="danger" plain :loading="mirrorBusy" :disabled="mirrorBusy || !mirrorStatus.counts?.dirs_pruned_zombie" @click="clearPrunedMappings">清空已清理映射</el-button></div>
            <p class="hint">「清理无效映射」只读核对 B 库，把已不存在的文件 / 目录映射重置为待同步（保留 A 库文件名与指纹），下一轮「立即同步」自动补复制；不删资源、分享与队列。被清理的目录标记「待重建」后不再占用上方「待创建目录」清单——确认后点「清空已清理映射」可彻底删除这些僵尸行（A 库仍在时下轮同步会自动重建）。</p>
            <div class="table-scroll"><el-table :data="mirrorStatus.dirs || []" size="small">
              <el-table-column prop="name" label="目录名" min-width="200"/>
              <el-table-column prop="kind" label="类别" width="100"/>
              <el-table-column prop="category_path" label="所属分类" min-width="160"/>
              <el-table-column label="状态" width="120"><template #default="s"><el-tag v-if="s.row.pruned" type="warning" size="small">待重建</el-tag><span v-else>{{ s.row.status }}</span></template></el-table-column>
              <el-table-column prop="target_dir_id" label="B 库目录 ID" min-width="180"/>
              <el-table-column prop="error" label="备注 / 失败原因" min-width="200"/>
            </el-table></div>
            <p class="hint">分类目录保持 A 库原名、不加标记；只有影视主目录按 · 规则命名，括号里的年份与 TMDB 标记保持原样。</p>
          </div>
          <div class="panel">
            <div class="panel-head"><h3>影视映射清单</h3><el-button size="small" type="danger" plain :loading="mirrorBusy" :disabled="mirrorBusy || !mirrorStatus.media?.length" @click="clearInvalidMedia">清理无效影视映射</el-button></div>
            <div class="table-scroll"><el-table :data="mirrorStatus.media || []" size="small">
              <el-table-column prop="category_path" label="分类目录" min-width="140"/>
              <el-table-column prop="title" label="标准影视名" min-width="180"/>
              <el-table-column prop="mirror_name" label="B 库期望目录名" min-width="180"/>
              <el-table-column prop="media_type" label="类型" width="90"/>
              <el-table-column prop="files" label="文件" width="80"/>
              <el-table-column label="状态" width="120"><template #default="s"><span>{{ s.row.status }}</span></template></el-table-column>
            </el-table></div>
            <p class="hint">B 库目录名里的 · 只存在于网盘（v0.8.3 起网盘拒绝 *）；资源库、分享标题与 Telegram 推送一律使用标准影视名。<b>点「清理无效影视映射」会只读核对 B 库</b>，把网盘上已整部删除文件的历史映射行清掉（列目录不可信或有文件的行一律保留）；只删 A→B 映射登记，不删资源 / 分享 / 队列 / B 库文件。</p>
          </div>
            </el-collapse-item>
          </el-collapse>
        </section>

        <section v-else-if="tab === '分发与互动中心'" class="fade">
          <div class="page-head"><div><h2>分发与互动中心</h2><p>Bot 统一分发 · 频道 / 群聊 · 推送白名单 · 投稿求片 · TgtoDrive 联动</p></div><div class="head-tools"><el-tag size="small">待审投稿 {{ interactionStatus.contributions_pending || 0 }}</el-tag></div></div>

          <!-- ===== a 板块：交互 Bot / 推送白名单 / 使用限制 / 打赏与消息跳转 ===== -->
          <div class="sec-band"><span class="sec-key">a</span><div><h3>交互 Bot · 推送白名单 · 使用限制 · 打赏</h3><p>全局单 Bot 既推送影视又收私聊指令；不同目录经同一 Bot 推到不同频道 / 群聊；私聊只提供求片 / 投稿 / 搜索 / 打赏</p></div><span class="sec-line"></span></div>

          <!-- ===== 交互 Bot 与推送配置（全局单 Bot：token 填写 / 测试 / 管理） ===== -->
          <div class="bot-card">
            <div class="bot-card-head"><span class="bot-ico">🧩</span><h3>交互 Bot 与推送配置</h3><span class="restrict-sub">全局单 Bot：既推送影视，也接收私聊指令</span></div>
            <div class="form-grid">
              <div class="field">
                <label>Bot Token（@BotFather 获取，加密存储）</label>
                <el-input v-model="pushBotToken" type="password" show-password placeholder="留空表示不修改当前 Token；如 123456:ABC-DEF...；要删除请点下方「清除 Bot Token」"/>
                <small>{{ pushCfg.bot ? ('当前 Bot：' + pushCfg.bot.name + ' · ' + (pushCfg.bot.token_masked || '未配置')) : '尚未配置 Bot，请填写 Token 后点「保存推送配置」' }}</small>
              </div>
              <div class="field">
                <label>Bot 用户名（@username，可留空自动读取）</label>
                <el-input v-model="interactionCfg.interaction_bot_username" placeholder="如 my_tg_bot，不带 @；用于生成加群授权链接，留空则由 getMe 自动获取"/>
                <small>生成「加入群组」链接需要 Bot 用户名；通常点击「生成授权链接」会自动从 Telegram 读取，此处仅作网络异常时的兜底。</small>
              </div>
              <div class="field inline"><label>启用指令监听</label><el-switch v-model="interactionCfg.contribution_enabled"/></div>
            </div>
            <div class="form-grid" style="margin-top:10px">
              <div class="field">
                <label>验证 / 测试推送</label>
                <div class="inline-form">
                  <el-button size="small" :disabled="!pushCfg.bot" @click="verifyPushBot">验证 Token</el-button>
                  <el-button size="small" type="primary" :disabled="!pushCfg.bot" @click="testPushRoute({}, (pushRoutes[0]?.channel_id || pushRoutes[0]?.group_id || ''))">测试推送（第一条目标）</el-button>
                </div>
                <div class="inline-form" style="margin-top:6px">
                  <el-button size="small" plain :disabled="!pushCfg.bot?.token_masked" :loading="pushSaving" @click="syncBotCommands(false)">同步指令栏（覆盖旧项目残留）</el-button>
                  <el-button size="small" plain type="danger" :disabled="!pushCfg.bot?.token_masked" :loading="pushSaving" @click="syncBotCommands(true)">清空指令栏</el-button>
                </div>
                <small>验证只检查 Token；同步/清空指令栏处理 Telegram「/」下拉里的旧指令，输入框上方的 Reply 键盘菜单不受影响。</small>
              </div>
              <div class="field">
                <label>管理员自定义群发通知（仅频道 / 群，不私聊用户）</label>
                <el-input v-model="broadcastText" type="textarea" :rows="2" placeholder="输入要发到全部推送目标的通知内容"/>
                <small>用户侧测试直接私聊 Bot 点 Reply 键盘按钮即可；这里只给管理员发通知，目标取「推送白名单（目录路由）」里的频道 / 群，不进队列。</small>
                <el-button size="small" type="primary" plain :disabled="!pushCfg.bot || !broadcastText" :loading="broadcastSending" @click="sendBroadcastNotice">发送到全部推送目标</el-button>
              </div>
            </div>
            <div class="panel-foot"><el-button type="primary" :loading="pushSaving" @click="savePushRoutes">保存分发与互动配置（Bot + 白名单 + 使用限制 + 打赏）</el-button><el-button type="danger" plain :loading="pushSaving" :disabled="!pushCfg.bot?.token_masked" @click="clearPushBotToken">清除 Bot Token</el-button><span class="hint" style="margin-left:10px">上方三块配置共用此按钮；保存后刷新页面仍保留。</span></div>
          </div>

          <!-- ===== v0.9.14 #258 / v0.9.15 #276：自动发现群组 / 统一推送目标 / 官方加群 ===== -->
          <div class="bot-card">
            <div class="bot-card-head"><span class="bot-ico">🔍</span><h3>自动发现群组 · 统一推送目标</h3><span class="restrict-sub">Bot 收到群消息 / 成员变化自动登记；勾选加入白名单、检查权限、测试推送</span><el-tag size="small" style="margin-left:auto">{{ discoverChats.length }} 个目标</el-tag></div>

            <!-- 加入群组 / 添加频道（官方 Deep Linking，不主动加群） -->
            <div class="disc-addtarget">
              <div class="disc-addtarget-col">
                <div class="disc-addtarget-title"><el-tag size="small" type="success">群组</el-tag> 用官方授权链接把 Bot 加进群</div>
                <p class="hint" style="margin:6px 0">点「生成授权链接」→ 在 Telegram 打开 → 选择目标群添加。Bot 入群后由成员变化事件自动登记群 ID / 名称 / 类型，不会自动加白名单。</p>
                <div class="discover-bar">
                  <el-select v-model="inviteBotId" placeholder="主交互 Bot" clearable style="width:180px">
                    <el-option v-for="b in discBots" :key="b.id" :label="b.name + (b.enabled ? '' : '（停用）')" :value="b.id"/>
                  </el-select>
                  <el-button :loading="inviteLoading" @click="genInviteLink(inviteBotId)">生成授权链接</el-button>
                  <el-button type="primary" :disabled="!inviteLink" @click="openStartgroup">加入群组</el-button>
                </div>
                <template v-if="inviteLink">
                  <div class="disc-invite-box">
                    <div><span class="disc-sub">Bot 用户名：</span><b>@{{ inviteLink.bot_username }}</b>
                      <el-button size="small" text @click="copyText(inviteLink.bot_username, 'Bot 用户名')">复制用户名</el-button></div>
                    <div><span class="disc-sub">授权链接：</span><span class="disc-url">{{ inviteLink.startgroup_url }}</span>
                      <el-button size="small" text @click="copyText(inviteLink.startgroup_url, '授权链接')">复制授权链接</el-button></div>
                    <div class="disc-sub">有效期至 {{ inviteLink.expires_at }} · 加入后点上方「发现 / 刷新」或进群发 /id 登记</div>
                  </div>
                </template>
              </div>
              <div class="disc-addtarget-col">
                <div class="disc-addtarget-title"><el-tag size="small" type="warning">频道</el-tag> 把 Bot 添加为频道管理员</div>
                <p class="hint" style="margin:6px 0">频道不支持 startgroup 授权链接，需在 Telegram 频道管理里把 Bot 加为管理员并授予发布权限。Bot 入频道后由事件自动登记。</p>
                <el-button type="warning" plain @click="channelGuideVisible = true">添加频道 · 查看引导</el-button>
              </div>
            </div>

            <!-- 手动智能添加 + 发现刷新 -->
            <div class="discover-bar" style="margin-top:12px">
              <el-input v-model="manualChatId" placeholder="输入群组 ID / @用户名 / Telegram 链接" style="max-width:280px" @keyup.enter="refreshDiscover"/>
              <el-input v-model="manualChatName" placeholder="备注名称（选填）" style="max-width:150px"/>
              <el-select v-model="manualChatKind" style="width:110px"><el-option label="自动识别" value="auto"/><el-option label="群组" value="group"/><el-option label="频道" value="channel"/></el-select>
              <el-button type="primary" :loading="discovering" @click="refreshDiscover">发现 / 刷新</el-button>
            </div>
            <p class="hint warn" style="margin:6px 0 10px">自动发现前提：Bot 已加入目标 + 有成员状态变化或新消息；隐私模式下需设 Bot 为管理员或在群里 @Bot 说话。私有邀请链接（t.me/+…）无法解析，请让 Bot 入群后发 /id。Telegram 不提供「列出已加入全部群」接口，只能事件驱动记录。</p>
            <p v-if="discPollError" class="hint" style="color:#d03050;margin:0 0 10px">轮询异常：{{ discPollError }}（若为 409 冲突，说明有另一个实例用同一 Token 在取更新，请停掉后重试）</p>

            <!-- 筛选 + 批量操作 -->
            <div class="discover-bar" style="margin-bottom:10px">
              <el-select v-model="discFilterType" placeholder="全部类型" clearable style="width:130px">
                <el-option label="全部类型" value=""/><el-option label="普通群" value="group"/><el-option label="超级群" value="supergroup"/><el-option label="频道" value="channel"/>
              </el-select>
              <el-select v-model="discFilterBot" placeholder="全部 Bot" clearable style="width:160px">
                <el-option label="全部 Bot" :value="null"/><el-option v-for="b in discBots" :key="b.id" :label="b.name" :value="b.id"/>
              </el-select>
              <el-input v-model="discKeyword" placeholder="名称 / ID / Username 搜索" clearable style="max-width:220px" @keyup.enter="loadDiscover"/>
              <el-checkbox v-model="discShowInactive" @change="loadDiscover">显示已退出</el-checkbox>
              <el-button @click="loadDiscover">筛选</el-button>
              <el-button type="success" plain :disabled="!discSelected.length" @click="batchWhitelist(true)">批量加入白名单 ({{ discSelected.length }})</el-button>
              <el-button type="danger" plain :disabled="!discSelected.length" @click="batchWhitelist(false)">批量移出白名单</el-button>
              <el-button type="danger" :disabled="!discSelected.length" @click="batchDeleteDiscover">批量从列表移除</el-button>
            </div>

            <el-table :data="discoverChats" row-key="id" size="small" border v-if="discoverChats.length" style="width:100%" @selection-change="v => discSelected = v.map(x => x.id)">
              <el-table-column type="selection" width="42" align="center"/>
              <el-table-column label="目标" min-width="170">
                <template #default="{row}"><div class="disc-name">{{ row.title || row.username || row.chat_id }}</div><div class="disc-sub">{{ row.chat_id }}<span v-if="row.username"> · @{{ row.username }}</span></div></template>
              </el-table-column>
              <el-table-column label="类型" width="96"><template #default="{row}"><el-tag size="small" :type="row.chat_type==='channel'?'warning':'info'">{{ TYPE_TAG[row.chat_type] || row.type_label || '未知' }}</el-tag></template></el-table-column>
              <el-table-column label="Bot" min-width="90"><template #default="{row}"><span class="disc-sub">{{ row.bot_name || '—' }}</span></template></el-table-column>
              <el-table-column label="成员状态" width="100"><template #default="{row}"><el-tag size="small" :type="memberTagType(row)">{{ row.member_label || MEMBER_LABEL[row.member_status] || '未知' }}</el-tag></template></el-table-column>
              <el-table-column label="权限" width="96"><template #default="{row}"><el-tag size="small" :type="permTagType(row)">{{ permLabel(row) }}</el-tag></template></el-table-column>
              <el-table-column label="白名单" width="76" align="center"><template #default="{row}"><el-switch :model-value="!!row.in_whitelist" @change="v => toggleDiscoverWhitelist(row, v)"/></template></el-table-column>
              <el-table-column label="最后活动" min-width="130"><template #default="{row}"><span class="disc-sub">{{ fmtTime(row.last_seen_at || row.updated_at) }}</span></template></el-table-column>
              <el-table-column label="操作" width="230" fixed="right">
                <template #default="{row}">
                  <el-button size="small" text :loading="discoverChecking===row.id" @click="checkDiscoverPerm(row)">检查权限</el-button>
                  <el-button size="small" text @click="testDiscoverPush(row)">测试推送</el-button>
                  <el-button size="small" text type="danger" @click="deleteDiscoverChat(row)">删除</el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-else description="暂无发现记录。请确保 Bot 已加入群组，并等待收到成员状态变化或新消息事件。" :image-size="60"/>
          </div>

          <!-- 频道添加引导弹窗 -->
          <el-dialog v-model="channelGuideVisible" title="如何把 Bot 添加到频道" width="480px">
            <ol class="disc-guide-list">
              <li v-for="(step, i) in (inviteLink && inviteLink.channel_guide) || [
                '在 Telegram 打开目标频道 → 频道管理 → 管理员 → 添加管理员',
                '搜索你的 Bot 并添加，至少勾选「发布消息」权限（否则无法推送）',
                '添加后频道有新消息时由 my_chat_member / channel_post 事件自动登记；需要推送时再勾选加入白名单'
              ]" :key="i">{{ step }}</li>
            </ol>
            <p class="hint warn">若尚未生成授权链接，弹窗里的 Bot 用户名可能为空；请先到上方「交互 Bot 与推送配置」填写 Bot 用户名，或点「生成授权链接」自动读取。</p>
            <template #footer><el-button type="primary" @click="channelGuideVisible = false">知道了</el-button></template>
          </el-dialog>

          <!-- ===== 推送白名单 = 目录路由表（默认1条，+添加最多10条） ===== -->
          <div class="bot-card">
            <div class="bot-card-head"><span class="bot-ico">📺</span><h3>推送白名单（目录路由）</h3><span class="restrict-sub">不同 B 库目录 → 同一 Bot → 不同频道 / 群聊；默认 1 条，最多 10 条</span><el-tag size="small" style="margin-left:auto">{{ pushRoutes.length }} / {{ pushCfg.max_routes || 10 }}</el-tag></div>
            <div v-for="(r, i) in pushRoutes" :key="i" class="route-row">
              <div class="field"><label>目录 ID（影视库 B 的下属目录，留空=根目录全部）</label><el-input v-model="r.dir_id" placeholder="留空表示整个 B 库；填目录 fileId 则只推该目录及其子目录"/></div>
              <div class="field"><label>备注（选填，便于肉眼区分目录）</label><el-input v-model="r.note" maxlength="120" placeholder="如：国产剧 / 动画电影 / 欧美剧集"/></div>
              <div class="field"><label>频道 ID（可留空）</label><el-input v-model="r.channel_id" placeholder="频道 chat_id，如 -1001234567890；频道需 Bot 为管理员"/><el-button size="small" text :disabled="!r.channel_id || !pushCfg.bot" @click="testPushRoute(r, r.channel_id)">测试频道</el-button></div>
              <div class="field"><label>群 ID（可留空，兼容旧版单目标）</label><el-input v-model="r.group_id" placeholder="群 chat_id，如 -1001234567890；群只需 Bot 已加入"/><el-button size="small" text :disabled="!r.group_id || !pushCfg.bot" @click="testPushRoute(r, r.group_id)">测试群</el-button></div>
              <div class="field route-targets" v-if="(r.targets || []).length">
                <label>额外推送目标（一个目录可绑定多个频道/群；上方单目标字段也会一并推送）</label>
                <div v-for="(t, ti) in r.targets" :key="ti" class="route-target-row">
                  <el-select v-model="t.chat_id" filterable allow-create placeholder="选已发现目标或手填 chat_id（已退出/Bot 不在群的目标不可选）" style="flex:1" @change="onRouteTargetPick(r, ti)">
                    <el-option v-for="c in pushableDiscoverChats" :key="c.chat_id" :label="(c.title || c.username || c.chat_id) + ' · ' + (TYPE_TAG[c.chat_type] || c.chat_type)" :value="c.chat_id"/>
                  </el-select>
                  <el-select v-model="t.kind" style="width:96px"><el-option label="频道" value="channel"/><el-option label="群组" value="group"/></el-select>
                  <el-button size="small" text type="danger" @click="removeRouteTarget(r, ti)">移除</el-button>
                </div>
              </div>
              <div class="field"><el-button size="small" text type="primary" @click="addRouteTarget(r)">+ 添加推送目标（多目标）</el-button></div>
              <div class="route-actions"><el-button size="small" text type="danger" :disabled="pushRoutes.length <= 1" @click="removePushRoute(i)">删除</el-button></div>
            </div>
            <div class="panel-foot"><el-button size="small" type="primary" plain :disabled="pushRoutes.length >= (pushCfg.max_routes || 10)" @click="addPushRoute">+ 添加路由</el-button><span class="hint" style="margin-left:10px">频道需 Bot 为管理员；群聊只需 Bot 已加入。保存后新识别的影视按目录路由推送。</span></div>
          </div>

          <!-- 内置指令速查（交互 Bot 的一部分） -->
          <div class="bot-card">
            <div class="bot-card-head"><span class="bot-ico">&gt;_</span><h3>内置指令速查</h3><span class="restrict-sub">私聊指令入口 · 只回复指令来源，不主动推送</span><el-tag size="small" type="info" style="margin-left:auto">固定四项 · 不可删除</el-tag></div>
            <div class="cmd-add" style="border-top:1px dashed var(--yg-line);margin-top:14px;padding-top:14px">
              <div class="bot-card-head" style="margin-bottom:10px"><h3 style="font-size:14px">内置指令速查</h3><el-tag size="small" type="info">固定四项 · 不可删除</el-tag></div>
            </div>
            <div class="cmd-grid">
              <div class="cmd-item" v-for="c in (interactionCfg.bot_commands || [])" :key="c.command">
                <span class="cmd-chip">/{{ c.command }}</span><span class="cmd-desc">{{ c.description }}</span>
                <el-button v-if="!c.builtin" size="small" text type="danger" @click="removeCustomCommand(c)">移除</el-button>
              </div>
            </div>
            <div class="cmd-add">
              <el-input v-model="newCmd.command" placeholder="指令名（如 sign）" style="width:180px"/>
              <el-input v-model="newCmd.description" placeholder="说明（如 每日签到）" style="width:220px"/>
              <el-input v-model="newCmd.reply" placeholder="回复文本" style="width:260px"/>
              <el-button size="small" type="primary" plain @click="addCustomCommand">添加自定义指令</el-button>
            </div>
            <p class="hint">求片 / 投稿 / 搜索 / 打赏为内置固定指令，其实现不可改；这里只能追加「收到指令 → 返回固定文本」的自定义功能。</p>

            <!-- v0.9.6.1：打赏模块归入内置指令速查（/reward 内置指令） -->
            <div class="panel narrow" style="margin-top:14px">
              <div class="panel-head"><h3 style="font-size:14px">打赏（内置指令 /reward）</h3><span>用户点「打赏」后 Bot 直接把这张图片发给他；与上方配置共用顶部统一保存按钮</span><el-switch v-model="interactionCfg.donation_enabled" style="margin-left:auto"/></div>
              <div class="form-grid">
                <div class="field"><label>赞赏码图片地址（直接发给用户）</label><el-input v-model="interactionCfg.donation_image_url" placeholder="https://.../reward.png"/></div>
                <div class="field"><label>打赏按钮文字</label><el-input v-model="interactionCfg.donation_button_text" placeholder="打赏支持"/></div>
              </div>
              <div class="field"><label>打赏说明（随图片一起发送）</label><el-input v-model="interactionCfg.donation_text" type="textarea" :rows="2" placeholder="感谢支持！"/></div>
              <p class="hint">只需填上面一个图片地址：用户点「打赏」时 Bot 直接发送该图片，不再显示网址文字。关闭开关后打赏入口不启用。</p>
            </div>
          </div>

          <!-- 使用限制：独立功能，关注频道 / 加群任填其一即可（v0.9.5 不再与白名单二选一） -->
          <div class="bot-card restrict-card">
            <div class="bot-card-head"><span class="bot-ico">🔒</span><h3>使用限制</h3><span class="restrict-sub">独立功能：用户须关注频道或加群才能用 Bot，二者任填其一即可</span><el-switch v-model="interactionCfg.bot_restrict_enabled" style="margin-left:auto"/></div>
            <div class="restrict-row">
              <div class="restrict-label"><b>检查结果缓存时间</b><small>用户通过校验后结果会被缓存，缓存期内无需重复检查。<br/>0=每次都检查；建议 60-120 秒。</small></div>
              <el-input-number v-model="interactionCfg.bot_restrict_cache_seconds" :min="0" :max="86400" controls-position="right" style="width:130px"/><span class="unit">秒</span>
            </div>
            <div class="restrict-row col">
              <div class="restrict-label"><b>须关注的频道 ID</b><small>一行一个，支持 @用户名 或频道 ID；机器人需为该频道管理员。与「须加入的群」任填其一即可。</small></div>
              <el-input v-model="requiredChannelsText" type="textarea" :rows="2" placeholder="@EmbyHub_News&#10;-1001234567890"/>
            </div>
            <div class="restrict-row col">
              <div class="restrict-label"><b>须加入的群 ID</b><small>一行一个群聊 ID。满足频道或群任意一项即可使用 Bot。</small></div>
              <el-input v-model="requiredGroupsText" type="textarea" :rows="2" placeholder="-1001234567890"/>
            </div>
            <div class="form-grid" style="margin-top:10px">
              <div class="field"><label>频道跳转链接（未关注时推给用户）</label><el-input v-model="interactionCfg.bot_required_channel_link" placeholder="@官方频道 或 https://t.me/官方频道"/></div>
              <div class="field"><label>群跳转链接（未加群时推给用户）</label><el-input v-model="interactionCfg.bot_required_group_link" placeholder="@交流群 或 https://t.me/交流群"/></div>
            </div>
            <p class="hint warn">未满足限制的用户使用 Bot 时，会收到带上方跳转按钮的提示。此功能与推送白名单相互独立，互不影响。</p>
          </div>


          <!-- 推送调度 + 链路自检 -->
          <div class="bot-card" style="margin-bottom:14px">
            <div class="bot-card-head"><span class="bot-ico">📤</span><h3>推送调度 · 间隔时间</h3><el-button size="small" type="primary" :loading="runningQueue" @click="runQueue">立即推送队列</el-button></div>
            <div class="schedule-controls"><span class="schedule-label">检查队列</span><el-input-number v-model="queueSchedule.interval_seconds" :min="5" :max="86400" controls-position="right" style="width:110px"/><span class="schedule-label">秒 · 或 Cron</span><el-input v-model="queueSchedule.cron" placeholder="留空用间隔；如 */1 * * * * 每分钟" style="max-width:230px"/><span class="schedule-label">每条间隔</span><el-input-number v-model="queueSchedule.send_delay" :min="0" :max="60" controls-position="right" style="width:100px"/><span class="schedule-label">秒</span><el-button type="primary" :loading="scheduleSaving" @click="saveQueueSchedule">保存调度</el-button></div>
            <p class="hint" style="margin:10px 0 0">「检查队列」= 多久扫一次待推队列（默认 30 秒）；「每条间隔」= 两条 TG 消息之间隔几秒（防风控，默认 2 秒）。Cron 非空时优先于检查间隔，保存即生效。</p>
            <div class="match-controls" style="margin-top:12px"><el-input v-model="telegramBase" placeholder="Telegram API 地址，默认 https://api.telegram.org（国内网络可填反代）"/><el-button :loading="telegramSaving" @click="saveTelegramBase">保存地址</el-button></div>
          </div>

          <!-- v0.9.6.1：打赏已移入「内置指令速查」；推送频道/群跳转已移入「消息模板」模块 -->

          <!-- ===== b 板块：用户投稿 / 用户求片 ===== -->
          <div class="sec-band"><span class="sec-key">b</span><div><h3>用户投稿 · 用户求片</h3><p>私聊 Bot 提交投稿与求片；求片审核通过后自动写入 TgtoDrive 监控规则，删除/拒绝时联动移除</p></div><span class="sec-line"></span></div>
          <div class="two-col">
            <div class="panel">
              <div class="panel-head"><h3>用户求片（我想看）</h3></div>
              <div class="inline-form"><el-input v-model="wantTitle" placeholder="影视名称" style="width:220px"/><el-button type="primary" @click="addWant">加入监控</el-button></div>
              <div class="table-scroll"><el-table :data="mediaRequests" size="small">
                <el-table-column label="影视名称（TMDB 候选）" min-width="180"><template #default="s">
                  {{ s.row.title }}<span v-if="s.row.year" class="muted"> · {{ s.row.year }}</span>
                  <el-tag v-if="s.row.tmdb_id" size="small" type="info" effect="plain">#{{ s.row.tmdb_id }}</el-tag>
                  <el-tag v-if="s.row.submit_count > 1" size="small" type="warning" effect="plain" :title="'累计 ' + s.row.submit_count + ' 次求片，已合并为一条'">×{{ s.row.submit_count }}</el-tag>
                </template></el-table-column>
                <el-table-column label="提交用户" width="120"><template #default="s">{{ s.row.tg_username || s.row.tg_user_id || '—' }}</template></el-table-column>
                <el-table-column label="审核状态" width="96"><template #default="s"><el-tag size="small" :type="s.row.review_status === 'adopted' ? 'success' : s.row.review_status === 'rejected' ? 'danger' : 'warning'">{{ wantReviewLabel(s.row.review_status) }}</el-tag></template></el-table-column>
                <el-table-column prop="review_note" label="审核备注" min-width="120"/>
                <el-table-column label="操作" min-width="210"><template #default="s">
                  <el-button size="small" type="primary" plain :disabled="s.row.review_status === 'adopted'" @click="handleMediaRequest(s.row, 'adopted')">采纳</el-button>
                  <el-button size="small" :disabled="s.row.review_status === 'rejected'" @click="handleMediaRequest(s.row, 'rejected')">拒绝</el-button>
                  <el-button size="small" type="danger" plain @click="deleteMediaRequest(s.row)">删除</el-button>
                </template></el-table-column>
              </el-table></div>
              <p class="hint">TgtoDrive 同步失败原因只在后台备注可见，不会发给用户；审核状态与关键词同步状态互不影响。</p>
            </div>
            <div class="panel">
              <div class="panel-head"><h3>用户投稿</h3></div>
              <div class="table-scroll"><el-table :data="contributions" size="small">
                <el-table-column label="标题（点击剧名打开资源）" min-width="180"><template #default="s">
                  <a v-if="safeHttpUrl(s.row.share_url)" :href="safeHttpUrl(s.row.share_url)" target="_blank" rel="noopener noreferrer" class="title-link" :title="s.row.share_url">{{ s.row.title || '未命名投稿' }}</a>
                  <span v-else>{{ s.row.title || '未命名投稿' }}</span><span v-if="s.row.year" class="muted"> · {{ s.row.year }}</span>
                  <el-tag v-if="s.row.tmdb_id" size="small" type="info" effect="plain">#{{ s.row.tmdb_id }}</el-tag>
                  <el-tag v-if="s.row.submit_count > 1" size="small" type="warning" effect="plain" :title="'累计 ' + s.row.submit_count + ' 次投稿，已合并为一条'">×{{ s.row.submit_count }}</el-tag>
                </template></el-table-column>
                <el-table-column label="用户" width="120"><template #default="s">{{ s.row.tg_username || s.row.tg_user_id || '—' }}</template></el-table-column>
                <el-table-column label="审核状态" width="92"><template #default="s">
                  <el-tag size="small" :type="contributionStatusTag(s.row.status)">{{ contributionStatusLabel(s.row.status) }}</el-tag>
                </template></el-table-column>
                <!-- v0.9.19：转存态与审核态分离，独立展示（光鸭原生 API） -->
                <el-table-column label="转存状态" width="110"><template #default="s">
                  <el-tooltip v-if="s.row.restore_error || s.row.restore_verify_note" :content="s.row.restore_error || s.row.restore_verify_note" placement="top">
                    <el-tag size="small" :type="restoreStatusTag(s.row.restore_status)">{{ s.row.restore_status_label || '未转存' }}</el-tag>
                  </el-tooltip>
                  <el-tag v-else size="small" :type="restoreStatusTag(s.row.restore_status)">{{ s.row.restore_status_label || '未转存' }}</el-tag>
                </template></el-table-column>
                <el-table-column label="转存时间/尝试" width="140"><template #default="s">
                  <div class="muted" style="line-height:1.4">{{ s.row.restore_status === 'success' ? '光鸭目录已核对' : '—' }}</div>
                  <div class="muted" style="line-height:1.4; font-size:11px">{{ s.row.restored_at ? new Date(s.row.restored_at).toLocaleString('zh-CN') : (s.row.restore_attempts ? `尝试 ${s.row.restore_attempts} 次` : '—') }}</div>
                </template></el-table-column>
                <el-table-column label="操作" width="290"><template #default="s">
                  <el-button size="small" type="primary" plain @click="handleContribution(s.row.id, 'approved')">通过</el-button>
                  <el-button size="small" @click="handleContribution(s.row.id, 'rejected')">拒绝</el-button>
                  <el-button v-if="s.row.restore_status" size="small" type="warning" plain @click="showContributionRestore(s.row)">记录</el-button>
                  <el-button v-if="['failed','retry_wait'].includes(s.row.restore_status)" size="small" type="success" plain @click="retryContributionRestore(s.row)">重试</el-button>
                  <el-button size="small" type="danger" plain @click="deleteContribution(s.row)">删除</el-button>
                </template></el-table-column>
              </el-table></div>
            </div>
          </div>

          <!-- ===== c / d 板块并排：TgtoDrive 联动 + 投稿转存（窄屏自动堆叠） ===== -->
          <div class="link-modules">
          <div class="link-module">
          <!-- c 板块：TgtoDrive 项目联动调用配置（仅求片白名单联动） -->
          <div class="sec-band"><span class="sec-key">c</span><div><h3>TgtoDrive 项目联动</h3><p>求片审核通过后写入频道监控白名单、删单联动移除；投稿转存已改走右侧「投稿转存」模块的光鸭原生 API，不再依赖 TgtoDrive</p></div><span class="sec-line"></span></div>
          <div class="panel narrow">
            <div class="panel-head"><h3>TgtoDrive 联动配置</h3>
              <div class="head-tools">
                <el-button :loading="tgtoTesting" size="small" @click="testTgto">测试连接（只读）</el-button>
                <el-button type="primary" :loading="interactionSaving" size="small" @click="saveTgto">保存 TgtoDrive 配置</el-button>
              </div>
            </div>
            <div class="form-grid">
              <div class="field"><label>TgtoDrive 地址</label><el-input v-model="interactionCfg.tgto_base_url" placeholder="http://127.0.0.1:端口"/></div>
              <div class="field"><label>场景名</label><el-input v-model="interactionCfg.tgto_scene" placeholder="ENV_FILTER_GUANGYA"/></div>
              <div class="field"><label>OpenAPI Token（留空保持原值）</label><el-input v-model="interactionCfg.tgto_api_key" placeholder="已配置则留空" show-password/></div>
              <div class="field"><label>a. 分辨率</label><el-select v-model="interactionCfg.tgto_rule_resolution" style="width:100%"><el-option label="4K" value="4k"/><el-option label="1080p" value="1080p"/><el-option label="720p" value="720p"/><el-option label="不限" value="all"/></el-select></div>
              <div class="field"><label>b. 画面效果</label><el-select v-model="interactionCfg.tgto_rule_effect" style="width:100%"><el-option label="全部效果" value="all"/><el-option label="杜比视界" value="dolby"/><el-option label="HDR" value="hdr"/><el-option label="SDR" value="sdr"/></el-select></div>
              <div class="field"><label>c. 自定义必含关键词</label><el-input v-model="interactionCfg.tgto_rule_required_keyword" placeholder="默认 265；留空则不启用必含条件"/></div>
            </div>
            <p class="hint">白名单采用「读取全量 → 合并 → 回写 → 回读校验」，只增量新增/删除本项目 kw_ 幂等规则，不覆盖、不清空 TgtoDrive 原有关键词；管理员在求片列表删除或拒绝时联动移除本项目规则。</p>
          </div>

          </div><!-- /link-module c -->
          <div class="link-module">
          <!-- d 板块：投稿转存（光鸭原生 API，不经 TgtoDrive） -->
          <div class="sec-band" style="margin-top:0"><span class="sec-key">d</span><div><h3>投稿转存</h3><p>审核通过后直接调用光鸭转存 API，把用户投稿保存到下方目录 ID；不经 TgtoDrive，不依赖 Bot 转发</p></div><span class="sec-line"></span></div>
          <div class="panel narrow">
            <div class="panel-head"><h3>投稿转存配置</h3>
              <div class="head-tools"><el-button type="primary" :loading="interactionSaving" size="small" @click="saveContributionRestore">保存投稿转存配置</el-button></div>
            </div>
            <div class="form-grid">
              <div class="field"><label>启用投稿转存</label><el-switch v-model="interactionCfg.contribution_restore_enabled"/></div>
              <div class="field"><label>目标目录 ID（光鸭 fileId）</label><el-input v-model="interactionCfg.contribution_restore_target_dir_id" placeholder="填目标目录 fileId，如 1943905243358466080"/></div>
            </div>
            <p class="hint">关闭开关时审核通过只改审核状态、不转存。转存链路：解析光鸭分享链接 → 取 accessToken → 列分享根文件 → restore_share 到目标目录 → 列目标目录按「文件名+大小」核对，接口提交成功但核对不到不会标记完成。若分享由当前光鸭账号自己创建（SHARE_SELF_RESTORE），光鸭会拒绝自转存，需用户换账号分享。</p>
          </div>
          </div><!-- /link-module d -->
          </div><!-- /link-modules -->

          <p class="hint" style="text-align:right;margin:0">a/b/c 与投稿转存配置也可随页面顶部「保存分发与互动配置」一起保存。</p>

        </section>

        <section v-else-if="tab === '资源库'" class="fade">
          <div class="page-head"><div><h2>资源库</h2><p>按作品聚合 · 整剧目录分享 · TMDB 识别 · 批量管理</p></div><div class="head-tools"><el-button size="small" type="primary" :loading="asyncScanRunning" @click="scan">立即扫描</el-button><el-button size="small" :loading="asyncScanRunning" @click="rescanLibrary">重新扫描库</el-button><el-button size="small" @click="showScanPanel=!showScanPanel">{{ showScanPanel ? '收起扫描计划' : '扫描计划' }}</el-button><el-button size="small" :loading="finishChecking" @click="loadFinishCheck">完结校验</el-button><el-button size="small" :loading="reidentifying" @click="reidentifyAll">按目录重识别</el-button><el-button size="small" type="danger" plain @click="clearLibrary">清空扫描结果</el-button><el-input v-model="keyword" placeholder="搜索作品名" clearable style="width:200px"><template #prefix><svg class="ico" viewBox="0 0 24 24"><path :d="icon('search')"/></svg></template></el-input><el-select v-model="statusFilter" placeholder="分享状态" clearable style="width:120px"><el-option label="已分享" value="active"/><el-option label="未分享" value="not_created"/><el-option label="分享失败" value="failed"/><el-option label="已取消" value="cancelled"/></el-select><el-select v-model="identifyFilter" placeholder="识别状态" clearable style="width:120px"><el-option label="已识别" value="identified"/><el-option label="匹配存疑" value="ambiguous"/><el-option label="识别失败" value="failed"/><el-option label="无匹配" value="none"/><el-option label="未识别" value="unidentified"/></el-select></div></div>

          <!-- v0.7.0：一级状态栏（参考海报墙布局）：识别状态 + 媒体类型/完结追更 并列 -->
          <div class="status-tabs">
            <div class="status-tab-group">
              <span class="stab-label">识别</span>
              <button :class="{on:statusTab==='all'}" @click="statusTab='all'">全部状态</button>
              <button :class="{on:statusTab==='identified'}" @click="statusTab='identified'">已识别</button>
              <button :class="{on:statusTab==='unidentified'}" @click="statusTab='unidentified'">待识别</button>
            </div>
            <div class="status-tab-group">
              <span class="stab-label">类型</span>
              <button :class="{on:typeTab==='all'}" @click="typeTab='all'">全部类型</button>
              <button :class="{on:typeTab==='movie'}" @click="typeTab='movie'">电影</button>
              <button :class="{on:typeTab==='tv'}" @click="typeTab='tv'">剧集</button>
              <button :class="{on:typeTab==='ending'}" @click="typeTab='ending'">完结</button>
              <button :class="{on:typeTab==='returning'}" @click="typeTab='returning'">追更</button>
            </div>
          </div>

          <!-- v0.7.0：批量操作条（勾选作品后出现） -->
          <div v-if="selectedSeries.length" class="batch-bar">
            <span>已选 <b>{{ selectedSeries.length }}</b> 部</span>
            <el-select v-model="batchDuration" placeholder="分享效期" style="width:120px"><el-option label="永久" :value="0"/><el-option label="1 天" :value="86400"/><el-option label="7 天" :value="604800"/><el-option label="30 天" :value="2592000"/></el-select>
            <el-button size="small" type="primary" plain :loading="batchBusy" @click="batchAction('set_duration')">设置效期</el-button>
            <el-button size="small" :loading="batchBusy" @click="batchAction('re_share')">重新分享</el-button>
            <el-button size="small" :loading="batchBusy" @click="batchAction('identify')">批量重识别</el-button>
            <el-button size="small" :loading="batchBusy" @click="batchAction('set_finish', 'ended')">标记完结</el-button>
            <el-button size="small" :loading="batchBusy" @click="batchAction('set_finish', 'returning')">标记追更</el-button>
            <el-button size="small" type="danger" plain :loading="batchBusy" @click="batchAction('cancel_share')">取消分享</el-button>
            <el-button size="small" text @click="selectedSeries=[]">清空选择</el-button>
          </div>

          <div v-if="finishChecks.length" class="panel narrow">
            <div class="panel-head"><h3>完结校验（TMDB 集数 vs 本地文件）</h3><el-button size="small" text @click="finishChecks=[]">收起</el-button></div>
            <div class="table-scroll"><el-table :data="finishChecks" size="small">
              <el-table-column prop="title" label="影视" min-width="160"/>
              <el-table-column prop="tmdb_status" label="TMDB 状态" width="110"/>
              <el-table-column prop="expected_episodes" label="应有集" width="90"/>
              <el-table-column prop="local_episodes" label="本地集" width="90"/>
              <el-table-column prop="current_finish" label="当前标记" width="100"/>
              <el-table-column prop="verdict" label="结论" width="100"/>
              <el-table-column prop="suggestion" label="建议" min-width="200"/>
            </el-table></div>
            <p class="hint">只读核对，不自动改状态；TMDB 状态滞后时用卡片上的「完结状态」手动修正并锁定。</p>
          </div>

          <!-- 作品视图：整部剧/电影一张海报卡，支持勾选批量管理
               v2.1.6：按 TMDB 分类分组展示 + 每个分类可折叠；
               「按分类分组」关闭时回到原来的平铺海报墙（功能完全一致）。 -->
          <div class="grid-toolbar">
            <label class="select-all"><input type="checkbox" :checked="allSelected" @change="toggleSelectAll"/> 全选本页（{{ filteredSeries.length }}）</label>
            <div class="grid-toolbar-right">
              <label class="select-all"><input type="checkbox" :checked="groupByCategory" @change="groupByCategory = !groupByCategory"/> 按 TMDB 分类分组</label>
              <template v-if="groupByCategory && seriesGroupsByCategory.length">
                <el-button size="small" text @click="toggleAllGroups(!allGroupsCollapsed)">{{ allGroupsCollapsed ? '展开所有' : '折叠所有' }}</el-button>
              </template>
            </div>
          </div>

          <!-- 分组视图：每个 TMDB 分类一块，可单独折叠 -->
          <div v-if="groupByCategory" class="lib-groups">
            <div v-for="grp in seriesGroupsByCategory" :key="grp.name" class="lib-group">
              <button class="lib-group-head" @click="toggleGroup(grp.name)">
                <span class="lib-group-caret">{{ grp.collapsed ? '▸' : '▾' }}</span>
                <b>{{ grp.name }}</b>
                <span class="lib-group-count">{{ grp.count }} 部</span>
                <span class="lib-group-hint">{{ grp.collapsed ? '展开' : '折叠' }}</span>
              </button>
              <div v-show="!grp.collapsed" class="media-grid">
                <div v-for="g in grp.items" :key="g.key" class="media-card series-card" :class="{picked:selectedSeries.includes(g.key)}" @click="toggleSelect(g.key)">
                  <div class="media-poster">
                    <el-image v-if="g.poster_path" :src="'https://image.tmdb.org/t/p/w342'+g.poster_path" fit="cover" style="width:100%;height:100%"/>
                    <div v-else class="no-poster"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('image')"/></svg><span>{{ mediaLabel(g.media_type) || 'MEDIA' }}</span></div>
                    <input type="checkbox" class="card-check" :checked="selectedSeries.includes(g.key)" @click.stop="toggleSelect(g.key)"/>
                    <span v-if="g.share_status === 'active'" class="scrape-ok">✓</span>
                    <span class="media-badge" :class="'badge-'+g.share_status">{{ mediaShareLabel(g.share_status) }}</span>
                    <span v-if="g.pushed" class="push-badge" title="已推送到 Telegram">已推送</span>
                    <span v-if="g.link_state && mediaLinkLabel(g.link_state)" class="link-badge" :title="mediaLinkLabel(g.link_state)">{{ mediaLinkLabel(g.link_state) }}</span>
                    <span v-if="g.finish_status==='ended'" class="finish-badge fin-ended">完结<span v-if="g.finish_locked">·手动</span></span>
                    <span v-else-if="g.finish_status==='returning'" class="finish-badge fin-returning">追更<span v-if="g.finish_locked">·手动</span></span>
                    <span v-if="g.share_duration" class="dur-badge">{{ durLabel(g.share_duration) }}</span>
                    <span class="ep-count">{{ g.count }} 集</span>
                  </div>
                  <div class="media-body"><b :title="g.title">{{ g.title }}</b><small>{{ mediaLabel(g.media_type) }}{{ g.year ? ' · ' + g.year : '' }}{{ g.episode_label ? ' · ' + g.episode_label : '' }}</small><div class="media-meta"><span v-if="g.tmdb_id">TMDB #{{ g.tmdb_id }}</span><span v-if="g.vote_average">评分 {{ Number(g.vote_average).toFixed(1) }}</span><span v-if="g.identified_count && g.identified_count < g.count" class="identify-chip">{{ g.identified_count }}/{{ g.count }} 已识别</span><span v-else-if="g.identify_status && g.identify_status !== 'identified'" class="identify-chip">{{ identifyLabel(g.identify_status) }}</span></div></div>
                  <div class="media-actions" @click.stop>
                    <el-button size="small" type="primary" plain :loading="publishing === g.representative_id" @click="seriesRepresentative(g) && publishResource(seriesRepresentative(g))">推送</el-button>
                    <el-button v-if="g.share_url" size="small" @click="cancelShare(seriesRepresentative(g) || {id:g.representative_id, name:g.title})">取消分享</el-button>
                    <el-button v-else size="small" @click="createShare(seriesRepresentative(g) || {id:g.representative_id, name:g.title})">分享</el-button>
                    <el-button size="small" text @click="identify(seriesRepresentative(g) || {id:g.representative_id, name:g.title}, true)">识别</el-button>
                    <el-button size="small" text @click="openMatch(seriesRepresentative(g) || {id:g.representative_id, name:g.title, path_hint:''})">手动修正</el-button>
                    <el-dropdown size="small" trigger="click" @command="c => setFinish(g, c)">
                      <el-button size="small" text>完结状态 ▾</el-button>
                      <template #dropdown><el-dropdown-menu>
                        <el-dropdown-item command="ended">标记完结（锁定）</el-dropdown-item>
                        <el-dropdown-item command="returning">标记追更（锁定）</el-dropdown-item>
                        <el-dropdown-item command="">清除状态（锁定）</el-dropdown-item>
                        <el-dropdown-item command="auto">恢复 TMDB 自动判断</el-dropdown-item>
                      </el-dropdown-menu></template>
                    </el-dropdown>
                  </div>
                </div>
              </div>
            </div>
            <div v-if="!filteredSeries.length" class="empty big">没有匹配的作品，试试点「重新扫描」或清空扫描结果后重扫</div>
          </div>

          <!-- 平铺视图（不分组）：与分组视图共用同一批卡片，操作完全一致 -->
          <div v-else class="media-grid">
            <div v-for="g in filteredSeries" :key="g.key" class="media-card series-card" :class="{picked:selectedSeries.includes(g.key)}" @click="toggleSelect(g.key)">
              <div class="media-poster">
                <el-image v-if="g.poster_path" :src="'https://image.tmdb.org/t/p/w342'+g.poster_path" fit="cover" style="width:100%;height:100%"/>
                <div v-else class="no-poster"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('image')"/></svg><span>{{ mediaLabel(g.media_type) || 'MEDIA' }}</span></div>
                <input type="checkbox" class="card-check" :checked="selectedSeries.includes(g.key)" @click.stop="toggleSelect(g.key)"/>
                <span v-if="g.share_status === 'active'" class="scrape-ok">✓</span>
                <span class="media-badge" :class="'badge-'+g.share_status">{{ mediaShareLabel(g.share_status) }}</span>
                <span v-if="g.pushed" class="push-badge" title="已推送到 Telegram">已推送</span>
                <span v-if="g.link_state && mediaLinkLabel(g.link_state)" class="link-badge" :title="mediaLinkLabel(g.link_state)">{{ mediaLinkLabel(g.link_state) }}</span>
                <span v-if="g.finish_status==='ended'" class="finish-badge fin-ended">完结<span v-if="g.finish_locked">·手动</span></span>
                <span v-else-if="g.finish_status==='returning'" class="finish-badge fin-returning">追更<span v-if="g.finish_locked">·手动</span></span>
                <span v-if="g.share_duration" class="dur-badge">{{ durLabel(g.share_duration) }}</span>
                <span class="ep-count">{{ g.count }} 集</span>
              </div>
              <div class="media-body"><b :title="g.title">{{ g.title }}</b><small>{{ mediaLabel(g.media_type) }}{{ g.year ? ' · ' + g.year : '' }}{{ g.episode_label ? ' · ' + g.episode_label : '' }}</small><div class="media-meta"><span v-if="g.tmdb_id">TMDB #{{ g.tmdb_id }}</span><span v-if="g.vote_average">评分 {{ Number(g.vote_average).toFixed(1) }}</span><span v-if="g.identified_count && g.identified_count < g.count" class="identify-chip">{{ g.identified_count }}/{{ g.count }} 已识别</span><span v-else-if="g.identify_status && g.identify_status !== 'identified'" class="identify-chip">{{ identifyLabel(g.identify_status) }}</span></div></div>
              <div class="media-actions" @click.stop>
                <el-button size="small" type="primary" plain :loading="publishing === g.representative_id" @click="seriesRepresentative(g) && publishResource(seriesRepresentative(g))">推送</el-button>
                <el-button v-if="g.share_url" size="small" @click="cancelShare(seriesRepresentative(g) || {id:g.representative_id, name:g.title})">取消分享</el-button>
                <el-button v-else size="small" @click="createShare(seriesRepresentative(g) || {id:g.representative_id, name:g.title})">分享</el-button>
                <el-button size="small" text @click="identify(seriesRepresentative(g) || {id:g.representative_id, name:g.title}, true)">识别</el-button>
                <el-button size="small" text @click="openMatch(seriesRepresentative(g) || {id:g.representative_id, name:g.title, path_hint:''})">手动修正</el-button>
                <el-dropdown size="small" trigger="click" @command="c => setFinish(g, c)">
                  <el-button size="small" text>完结状态 ▾</el-button>
                  <template #dropdown><el-dropdown-menu>
                    <el-dropdown-item command="ended">标记完结（锁定）</el-dropdown-item>
                    <el-dropdown-item command="returning">标记追更（锁定）</el-dropdown-item>
                    <el-dropdown-item command="">清除状态（锁定）</el-dropdown-item>
                    <el-dropdown-item command="auto">恢复 TMDB 自动判断</el-dropdown-item>
                  </el-dropdown-menu></template>
                </el-dropdown>
              </div>
            </div>
            <div v-if="!filteredSeries.length" class="empty big">没有匹配的作品，试试点「重新扫描」或清空扫描结果后重扫</div>
          </div>

          <!-- v0.8.1：原「扫描设置」页已合并到资源库；扫描根目录只读引用镜像同步里的 B 库 ID -->
          <div v-if="showScanPanel" class="panel" style="margin-top:16px">
            <div class="panel-head"><h3>扫描计划与断点</h3><el-tag size="small">Asia/Shanghai · UTC+8</el-tag></div>
            <div class="field"><label>扫描间隔</label>
              <div class="preset-row">
                <button v-for="v in scanPresets" :key="v" class="preset-btn" :class="{on: !scanCustom && scanInterval === v}" @click="pickScanInterval(v)">{{ intervalLabel(v) }}</button>
              </div>
              <small>点选即可，无需手写 Cron。当前表达式：{{ task.cron || '未设置' }}</small>
            </div>
            <div class="field"><label class="inline-label"><input type="checkbox" v-model="scanCustom"/> 自定义 Cron 表达式</label>
              <el-input v-if="scanCustom" v-model="task.cron" placeholder="*/30 * * * *"/>
              <small v-if="scanCustom">例：*/10 * * * * 每 10 分钟；0 3 * * * 每天 03:00</small>
            </div>
            <div class="form-grid">
              <div class="field inline"><label>启用定时扫描</label><el-switch v-model="task.enabled"/></div>
            </div>
            <p class="hint">扫描根目录自动引用「镜像同步」里填的 B 库目录 ID，无需在此填写；改 B 库 ID 后下一次扫描立即生效。</p>
            <div class="panel-foot"><el-button type="primary" @click="saveTask">保存扫描计划</el-button><el-button :loading="asyncScanRunning" @click="scan">立即扫描</el-button></div>
            <div class="cursor-box"><div><small>游标状态</small><b>{{ task.cursor_status }}</b></div><div><small>已处理文件</small><b>{{ task.cursor_files }}</b></div><div><small>遍历目录</small><b>{{ task.cursor_dirs }}</b></div><div><small>上次结果</small><b>{{ task.last_status || 'never' }}</b></div></div>
            <p class="hint">{{ task.last_message || '尚未执行扫描' }}{{ task.cursor_error ? ' · 错误：' + task.cursor_error : '' }}</p>
          </div>
          <div v-if="showScanPanel" class="panel" style="margin-top:14px">
            <div class="panel-head"><h3>扫描批次保护</h3><el-tag size="small" type="info">仅完整全量批次允许对账删除</el-tag></div>
            <div class="table-scroll"><el-table :data="scanBatches" size="small">
              <el-table-column prop="id" label="#" width="60"/>
              <el-table-column prop="status" label="状态" width="100"/>
              <el-table-column prop="files" label="文件" width="80"/>
              <el-table-column prop="seen_count" label="本轮见到" width="100"/>
              <el-table-column label="完整" width="80"><template #default="s">{{ s.row.complete ? '是' : '否' }}</template></el-table-column>
              <el-table-column label="允许对账" width="100"><template #default="s">{{ s.row.reconcile_allowed ? '是' : '否' }}</template></el-table-column>
            </el-table></div>
            <p class="hint">分页不完整、断点续扫、空结果或接口异常时，一律跳过对账，不会误删资源或取消分享。</p>
          </div>
        </section>

        <section v-else-if="tab === '推送队列'" class="fade">
          <div class="sc-shell">
            <!-- 顶部 tab（发布总览 → 发布规则 → 推送任务 → 分享管理 → 阅读量） -->
            <div class="sc-tabbar">
              <button v-for="n in scSubNavs" :key="n.key" :class="['sc-tab', { active: scSubNav === n.key }]" @click="scSwitchSubNav(n.key)">
                <span class="sc-tab-ico">{{ n.ico }}</span>{{ n.key }}
              </button>
            </div>

            <!-- 工作区 -->
            <div class="sc-workspace">
              <div class="sc-topbar">
                <div><h2>{{ scSubNav }}</h2>
                  <p>{{ scSubNav === '发布总览' ? '发布生命周期与发布调度' : '清单 · 计划 · 排期 · 审核 统一控制台' }}</p></div>
                <div class="head-tools">
                  <span :class="['sc-run-badge', scOverview.paused ? 'paused' : '']"><i></i>{{ scOverview.paused ? '已暂停' : '运行中' }}</span>
                  <el-button size="small" :type="scOverview.paused ? 'success' : 'danger'" plain @click="scTogglePause">{{ scOverview.paused ? '恢复发布' : '暂停发布' }}</el-button>
                  <el-button size="small" type="primary" :loading="runningQueue" @click="scRun">手动扫描</el-button>
                </div>
              </div>

              <!-- 子页：发布总览（仪表盘，全部来自 /sc/overview 真实数据） -->
              <template v-if="scSubNav === '发布总览'">
                <div class="sc-dash">
                  <!-- 六 KPI 卡：全部来自 /sc/overview kpi 真实聚合；点击跳转筛选 -->
                  <div class="sc-kpi-grid sc-kpi-six">
                    <button class="sc-kpi sc-kpi-blue" @click="scFilterQueue('pending')">
                      <span class="sc-kpi-ico">➤</span><span class="sc-kpi-body"><small>待推送清单</small><b>{{ scOverview.kpi?.pending_list || 0 }}</b></span></button>
                    <button class="sc-kpi sc-kpi-green" @click="scFilterQueue('published')">
                      <span class="sc-kpi-ico">▤</span><span class="sc-kpi-body"><small>今日已推送</small><b>{{ scOverview.kpi?.today_published || 0 }}</b></span></button>
                    <button class="sc-kpi sc-kpi-violet" @click="scFilterQueue('published')">
                      <span class="sc-kpi-ico">↻</span><span class="sc-kpi-body"><small>自动配送</small><b>{{ scOverview.kpi?.auto_delivered || 0 }}</b></span></button>
                    <button class="sc-kpi sc-kpi-teal" @click="scSubNav = '推送任务'">
                      <span class="sc-kpi-ico">✓</span><span class="sc-kpi-body"><small>推送成功率</small><b>{{ scOverview.kpi?.success_rate || 0 }}%</b></span></button>
                    <button class="sc-kpi sc-kpi-amber" @click="scSubNav = '分享管理'">
                      <span class="sc-kpi-ico">✦</span><span class="sc-kpi-body"><small>本周AI候选</small><b>{{ scOverview.kpi?.weekly_ai_candidates || 0 }}</b></span></button>
                    <button class="sc-kpi sc-kpi-rose" @click="scFilterQueue('failed')">
                      <span class="sc-kpi-ico">!</span><span class="sc-kpi-body"><small>失败待处理</small><b>{{ scOverview.kpi?.failed_pending || 0 }}</b></span></button>
                  </div>

                  <!-- 链接健康来源披露：分享任务只写「已建链 / 待建链」两态，
                       不伪装旧核验链路的失效判定（那是另一套已下线的数据体系）。 -->
                  <div class="sc-link-source-note">
                    <span class="sc-ico">ⓘ</span>
                    <span>链接状态说明：分享任务只记录 <b>已建链 / 待建链</b> 两态；链接失效由资源库删除事件驱动，不在此展示历史核验态</span>
                    <span class="sc-link-mini">当前有效 {{ scOverview.link_state_counts?.active || 0 }} · 待建链 {{ scOverview.link_state_counts?.unknown || 0 }}</span>
                  </div>

                  <!-- 设计稿三栏：推送趋势 / 分类推送占比 / 本周热门候选 -->
                  <div class="sc-dash-three">
                    <div class="sc-card">
                      <div class="sc-card-head"><span class="sc-ico">⌁</span><div><h3>推送趋势</h3><p>按实际推送时间统计</p></div>
                        <div class="sc-seg">
                          <button :class="{on: scTrendPeriod==='day'}" @click="scSwitchTrend('day')">日</button>
                          <button :class="{on: scTrendPeriod==='week'}" @click="scSwitchTrend('week')">周</button>
                          <button :class="{on: scTrendPeriod==='month'}" @click="scSwitchTrend('month')">月</button>
                        </div></div>
                      <div class="sc-trend-wrap">
                        <svg viewBox="0 0 420 160" preserveAspectRatio="none" class="sc-trend-svg" v-if="(scOverview.trend?.series || []).length">
                          <polyline :points="scTrendPoints()" fill="none" stroke="#6d5efc" stroke-width="2"/>
                          <polyline :points="scTrendArea()" fill="rgba(109,94,252,.12)" stroke="none"/>
                          <circle v-for="(p,i) in scTrendCoords()" :key="i" :cx="p.x" :cy="p.y" r="2.5" fill="#6d5efc"/>
                        </svg>
                        <div v-else class="empty">暂无发布记录，趋势图将在有真实推送后显示</div>
                        <div class="sc-trend-labels" v-if="(scOverview.trend?.series || []).length">
                          <span v-for="s in (scOverview.trend?.series || []).slice(-8)" :key="s.label">{{ s.label }}</span>
                        </div>
                      </div>
                    </div>
                    <div class="sc-card">
                      <div class="sc-card-head"><span class="sc-ico">◔</span><div><h3>分类推送占比</h3><p>按 TMDB 分类统计已推送数量</p></div></div>
                      <div class="sc-ratio-wrap" v-if="(scOverview.category_ratio?.items || []).length">
                        <svg viewBox="0 0 120 120" class="sc-donut">
                          <circle v-for="(it,i) in scDonutSegments()" :key="i" cx="60" cy="60" r="46" fill="none"
                                  :stroke="it.color" stroke-width="16" :stroke-dasharray="it.dash" :stroke-dashoffset="it.offset"
                                  transform="rotate(-90 60 60)"/>
                        </svg>
                        <div class="sc-ratio-legend">
                          <div v-for="it in (scOverview.category_ratio?.items || []).slice(0,6)" :key="it.category_key">
                            <i :style="{background: scDonutColor(it.category_key)}"></i><span>{{ it.name }}</span><b>{{ it.ratio }}%</b>
                          </div>
                        </div>
                      </div>
                      <div v-else class="empty">暂无已发布任务，占比图将在有真实推送后显示</div>
                    </div>
                    <div class="sc-card">
                      <div class="sc-card-head"><span class="sc-ico">★</span><div><h3>本周热门候选</h3><p>按 TMDB 评分排序</p></div></div>
                      <div class="sc-hot-list" v-if="(scOverview.hot_works?.items || []).length">
                        <div class="sc-hot-row" v-for="(w,i) in scOverview.hot_works.items" :key="i">
                          <span :class="['sc-hot-rank', i<3?'top':'']">{{ i+1 }}</span>
                          <span class="sc-hot-title">{{ w.title }}</span>
                          <span class="sc-hot-score">评分 {{ w.score }}</span>
                        </div>
                      </div>
                      <div v-else class="empty">暂无已识别作品评分</div>
                      <button class="sc-link-btn sc-hot-more" @click="scSubNav = '分享管理'">查看候选详情 ›</button>
                    </div>
                  </div>

                  <!-- 设计稿底部三表：推送历史 / 推送队列 / 推送失败（真实 views 数据） -->
                  <div class="sc-dash-three sc-three-tables">
                    <div class="sc-card">
                      <div class="sc-card-head"><span class="sc-ico">▤</span><div><h3>推送历史</h3><p>已成功推送的记录</p></div></div>
                      <el-table :data="scOverview.views?.history || []" stripe size="small" max-height="300">
                        <el-table-column label="时间" min-width="130" show-overflow-tooltip><template #default="s">{{ fmtTime(s.row.published_at) }}</template></el-table-column>
                        <el-table-column prop="title" label="资源名称" min-width="130" show-overflow-tooltip/>
                        <el-table-column label="分类" width="80"><template #default="s">{{ s.row.kind === 'first' ? '首次' : '追更' }}</template></el-table-column>
                        <el-table-column label="结果" width="76"><template #default="s"><span class="sc-dot-state published">成功</span></template></el-table-column>
                      </el-table>
                      <div v-if="!(scOverview.views?.history || []).length" class="empty">暂无推送历史</div>
                    </div>
                    <div class="sc-card">
                      <div class="sc-card-head"><span class="sc-ico">◷</span><div><h3>推送队列</h3><p>正在排队或推送中的记录</p></div></div>
                      <el-table :data="scOverview.views?.queue || []" stripe size="small" max-height="300">
                        <el-table-column label="时间" min-width="130" show-overflow-tooltip><template #default="s">{{ fmtTime(s.row.created_at) }}</template></el-table-column>
                        <el-table-column prop="title" label="资源名称" min-width="130" show-overflow-tooltip/>
                        <el-table-column label="类型" width="76"><template #default="s">{{ s.row.kind === 'first' ? '首次' : '追更' }}</template></el-table-column>
                        <el-table-column label="状态" width="100"><template #default="s">{{ scTaskStatusLabel(s.row.status) }}</template></el-table-column>
                      </el-table>
                      <div v-if="!(scOverview.views?.queue || []).length" class="empty">当前无在途任务</div>
                    </div>
                    <div class="sc-card">
                      <div class="sc-card-head"><span class="sc-ico">✕</span><div><h3>推送失败</h3><p>推送失败的记录，可逐条重试</p></div></div>
                      <el-table :data="scOverview.views?.failed || []" stripe size="small" max-height="300">
                        <el-table-column label="时间" min-width="120" show-overflow-tooltip><template #default="s">{{ fmtTime(s.row.updated_at) }}</template></el-table-column>
                        <el-table-column prop="title" label="资源名称" min-width="120" show-overflow-tooltip/>
                        <el-table-column prop="error" label="失败原因" min-width="120" show-overflow-tooltip/>
                        <el-table-column label="操作" width="66"><template #default="s"><button class="sc-link-btn" @click="scTaskAction(s.row.id, 'retry')">重试</button></template></el-table-column>
                      </el-table>
                      <div v-if="!(scOverview.views?.failed || []).length" class="empty">无失败任务</div>
                    </div>
                  </div>

                  <!-- v2.1.1：按设计稿收敛——发布总览只保留「六 KPI + 趋势/占比/热门候选 + 推送历史/队列/失败三表」，
                       其余三块汇总型面板均不在参考图范围内，已移除。
                       清单与任务明细改由「推送任务」「分享管理」两页承载。 -->
                </div>
              </template>

              <!-- 其它子页卡片（按子导航显示）
                   v2.1.5 布局根治：这三页此前挂在 .sc-board（两列 Grid）里，
                   ① 满宽类 .sc-board-full 定义在 .sc-board **之前**（同特异度后者胜出）
                      → 满宽规则完全失效，仍只占半幅；
                   ② 发布规则页内部再套一层两列 .sc-rule-layout → 每列只剩 1/4 页宽，
                      卡片被压成「缩小版」，尺寸与页面严重不符。
                   修法：把三页**移出 Grid**，与总览页（.sc-dash，flex column）平级，
                   各自独占整行满宽；页内需要并排时由各页自己的内部容器负责。 -->
              <div class="sc-subpage" v-show="scSubNav !== '发布总览'">
            <!-- 设计稿分享管理：搜索 + 状态筛选 + 批量 + 统计卡 + 记录表（详情为隐藏式弹窗） -->
            <div class="sc-share-layout" v-show="scSubNav === '分享管理'">
              <div class="sc-share-main">
                <div class="sc-form-row">
                  <el-input v-model="scShareSearch" size="small" clearable placeholder="作品名称 / 分享码 / 分享链接" style="width:280px"/>
                  <el-select v-model="scShareStatus" size="small" style="width:130px">
                    <el-option v-for="st in ['全部','有效','待校验','疑似失效','已失效','已失败']" :key="st" :label="st" :value="st"/>
                  </el-select>
                  <el-button size="small" type="primary">查询</el-button>
                  <el-button size="small" :disabled="!scSelectedShares.length" @click="scBatchTask('retry')">批量分享 ({{ scSelectedShares.length }})</el-button>
                  <el-button size="small" :disabled="!scSelectedShares.length" @click="scBatchTask('cancel')">批量取消</el-button>
                  <el-button size="small" plain :loading="scCleaningLinks" @click="scCleanupLinks">清理无效链接</el-button>
                </div>
                <div class="sc-stat-row sc-share-stats">
                  <div class="sc-stat"><b>{{ scShareStats.total || 0 }}</b><small>全部记录</small></div>
                  <div class="sc-stat"><b>{{ scShareStats.active || 0 }}</b><small>当前有效</small></div>
                  <div class="sc-stat"><b>{{ scShareStats.pending || 0 }}</b><small>待校验</small></div>
                  <div class="sc-stat"><b>{{ scShareStats.invalid || 0 }}</b><small>已失效</small></div>
                  <div class="sc-stat"><b>{{ scShareStats.history || 0 }}</b><small>历史记录</small></div>
                </div>
                <div class="sc-table-scroll">
                <el-table :data="scShareViews" stripe size="small" max-height="420" @selection-change="s => scSelectedShares = s">
                  <el-table-column type="selection" width="40"/>
                  <!-- v2.1.4：首列改「作品」——展示 TMDB 海报缩略图 + 作品名 + 类型 -->
                  <el-table-column label="作品" min-width="220">
                    <template #default="s">
                      <div class="sc-media-cell">
                        <div class="sc-media-thumb">
                          <el-image v-if="s.row.cover" :src="'https://image.tmdb.org/t/p/w185' + s.row.cover" fit="cover">
                            <template #error><div class="sc-thumb-fallback">无封面</div></template>
                          </el-image>
                          <div v-else class="sc-thumb-fallback">无封面</div>
                        </div>
                        <div class="sc-media-text">
                          <b>{{ s.row.share_title || '未命名作品' }}</b>
                          <small>{{ s.row.kind==='first' ? '电影/剧' : '追更' }} · {{ s.row.category_name || '未分类' }}</small>
                        </div>
                      </div>
                    </template>
                  </el-table-column>
                  <el-table-column prop="share_code" label="分享码" min-width="120" show-overflow-tooltip/>
                  <el-table-column label="发布时间" min-width="150" show-overflow-tooltip><template #default="s">{{ fmtTime(s.row.created_at) }}</template></el-table-column>
                  <el-table-column label="最后校验时间" min-width="150" show-overflow-tooltip><template #default="s">{{ fmtTime(s.row.last_checked_at) }}</template></el-table-column>
                  <el-table-column label="最近推送时间" min-width="150" show-overflow-tooltip><template #default="s">{{ fmtTime(s.row.last_pushed_at) }}</template></el-table-column>
                  <el-table-column label="链接状态" width="96"><template #default="s"><span :class="['sc-dot-state', s.row.link_tone]">{{ s.row.link_status_label }}</span></template></el-table-column>
                  <el-table-column label="操作" width="220"><template #default="s">
                    <el-button size="small" link type="primary" @click="scOpenShareDetail(s.row)">详情</el-button>
                    <el-button size="small" link @click="scCopyShareLink(s.row)">复制链接</el-button>
                    <el-button size="small" link @click="scTaskAction(s.row.id,'retry')">复查</el-button>
                    <el-button size="small" link type="danger" @click="scTaskAction(s.row.id,'cancel')">取消分享</el-button>
                  </template></el-table-column>
                </el-table>
                </div>
                <div v-if="!scShareViews.length" class="empty">暂无分享记录</div>
              </div>
            </div>

            <!-- 分享详情隐藏式弹窗：点「详情」才弹出，不占常驻版面 -->
            <el-dialog v-model="scShareDetailVisible" title="分享详情" width="560px" append-to-body>
              <div v-if="scSelectedDetail">
                <!-- v2.1.4：详情内页展示 TMDB 海报缩略图 -->
                <div class="sc-share-poster">
                  <el-image v-if="scSelectedDetail.cover" :src="'https://image.tmdb.org/t/p/w342' + scSelectedDetail.cover" fit="cover">
                    <template #error><div class="sc-thumb-fallback">无封面</div></template>
                  </el-image>
                  <div v-else class="sc-thumb-fallback">无封面</div>
                </div>
                <div class="sc-detail-kv">
                <div><span>作品</span><b>{{ scSelectedDetail.share_title || '—' }}</b></div>
                <div><span>分享码</span><b>{{ scSelectedDetail.share_code || '—' }}</b></div>
                <div><span>任务编号</span><b>#{{ scSelectedDetail.id }}</b></div>
                <div><span>类型</span><b>{{ scSelectedDetail.kind==='first'?'首次':'追更' }}</b></div>
                <div><span>所属分类</span><b>{{ scSelectedDetail.category_name || '未分类' }}</b></div>
                <div><span>当前状态</span><b>{{ scSelectedDetail.link_status_label || '待校验' }}</b></div>
                <div><span>分享链接</span><b style="word-break:break-all">{{ scSelectedDetail.share_url || '—' }}</b></div>
                <div><span>创建时间</span><b>{{ fmtTime(scSelectedDetail.created_at, true) }}</b></div>
                <div><span>最近校验</span><b>{{ fmtTime(scSelectedDetail.last_checked_at, true) }}</b></div>
                <div><span>最近推送</span><b>{{ fmtTime(scSelectedDetail.last_pushed_at, true) }}</b></div>
                <div><span>推送渠道</span><b>{{ scSelectedDetail.target_summary || '—' }}</b></div>
                <div><span>错误</span><b style="word-break:break-all">{{ scSelectedDetail.error || '—' }}</b></div>
                </div>
              </div>
              <template #footer>
                <el-button size="small" type="primary" @click="scOpenTaskDetail(scSelectedDetail.id)">完整执行记录</el-button>
                <el-button size="small" @click="scTaskAction(scSelectedDetail.id,'retry')">重新分享</el-button>
                <el-button size="small" danger plain @click="scTaskAction(scSelectedDetail.id,'cancel')">取消分享</el-button>
              </template>
            </el-dialog>

            <!-- v2.1.1：按设计稿收敛——分享管理页只保留「搜索/筛选/批量 + 统计卡 + 记录表 + 右侧详情」。
                 原先铺在页面上的清单汇总面板（曾一次铺开 4000+ 条历史记录）已整卡删除，
                 调度内部产物改由「推送任务」页按任务查看。
                 分类字典与 AI 建议仍由后端真实消费（分类决定排期归属、AI 建议进审核队列），
                 管理入口收敛到「发布规则」页的分类时间排期。 -->

            <!-- 设计稿发布规则：基础规则 / 分类时间排期 两个子 tab -->
            <!-- v2.1.4：整页满宽（不再挤在 .sc-board 的半格子里）。
                 基础规则与周期计划两卡并列一行，分类排期表独占整行。 -->
            <div v-show="scSubNav === '发布规则'">
              <div class="sc-subtabbar">
                <button v-for="t in ['基础规则','分类时间排期']" :key="t" :class="['sc-subtab',{on: scRuleTab===t}]" @click="scRuleTab=t">{{ t }}</button>
              </div>

              <div class="sc-rule-layout">
              <!-- 基础规则：上新即时推送 -->
              <div class="sc-card" v-show="scRuleTab==='基础规则'">
                <div class="sc-card-head"><span class="sc-ico">⚡</span><div><h3>上新即时推送</h3><p>最近上架的影视有新变化立即推送；配置持久化于 app_config</p></div>
                  <span class="sc-run-badge" :class="isBoolOn(scBasicRules.quick_publish_enabled)?'':'paused'"><i></i>{{ isBoolOn(scBasicRules.quick_publish_enabled)?'已启用':'已停用' }}</span></div>
                <div class="sc-form-row">
                  <label class="hint">启用即时推送<br><el-switch v-model="scBasicRules.quick_publish_enabled" active-value="true" inactive-value="false"/></label>
                  <label class="hint">更新检测频率(分钟)<br><el-input-number v-model="scBasicRules.update_detect_minutes" :min="1" size="small"/></label>
                  <label class="hint">每小时上限(条)<br><el-input-number v-model="scBasicRules.quick_publish_hourly_limit" :min="0" size="small" title="0=不限"/></label>
                  <label class="hint">冲突处理<br><el-select v-model="scBasicRules.conflict_policy" size="small" style="width:160px"><el-option label="新上映优先" value="prefer_release"/><el-option label="新入库优先" value="prefer_new"/><el-option label="存量排期优先" value="prefer_backlog"/></el-select></label>
                  <el-button size="small" type="primary" @click="scSaveBasicRules">保存基础规则</el-button>
                </div>
                <p class="hint sc-source-note">冲突处理决定**同一批待推任务**的先后顺序，不改变任何配额上限：新上映优先 = 近期上映的作品先发；新入库优先 = 刚入库的作品先发；存量排期优先 = 已排期的存量作品先发。</p>
              </div>

              <!-- 周期计划管理：小时/日/周/月度配额 + 静默保护（真实持久化并生效）
                   v2.1.6：仅在「基础规则」子 tab 下常驻，分类时间排期 tab 不再重复露出
                   （此前无 v-show，两个 tab 都显示，排期区会冒出一张重复卡片）。 -->
              <div class="sc-card" v-show="scRuleTab==='基础规则'">
                <div class="sc-card-head"><span class="sc-ico">▦</span><div><h3>周期计划管理</h3><p>小时/日/周/月度配额与静默保护；保存后由调度引擎夹逼执行</p></div>
                  <el-button size="small" type="primary" plain @click="scSaveCyclePlan">调整周期计划</el-button></div>
                <div class="sc-subtabbar sc-subtab-sm">
                  <button v-for="t in ['小时','日度','周度','月度']" :key="t" :class="['sc-subtab',{on: scCycleTab===t}]" @click="scCycleTab=t">{{ t }}</button>
                </div>
                <div class="sc-cycle-grid">
                  <template v-if="scCycleTab==='小时'">
                    <label class="hint">全周小时最大推送条数<br><el-input-number v-model="scCyclePlan.hour_max" :min="0" size="small"/></label>
                    <label class="hint">单分类每小时上限<br><el-input-number v-model="scCyclePlan.category_hour_max" :min="0" size="small"/></label>
                    <label class="hint">静默保护<br><el-switch v-model="scCyclePlan.quiet_enabled"/></label>
                    <label class="hint">静默起(时)<br><el-input-number v-model="scCyclePlan.quiet_start" :min="0" :max="23" size="small"/></label>
                    <label class="hint">静默止(时)<br><el-input-number v-model="scCyclePlan.quiet_end" :min="1" :max="24" size="small"/></label>
                    <p class="hint sc-source-note">静默保护默认关闭；开启后 {{ scCyclePlan.quiet_start }}:00–{{ scCyclePlan.quiet_end }}:00 不推送，命中时段 plan_engine 直接空转。凌晨时段（00:00–08:00）依赖该保护关闭才能产出。</p>
                  </template>
                  <template v-else-if="scCycleTab==='日度'">
                    <label class="hint">单日推送总上限<br><el-input-number v-model="scCyclePlan.day_max" :min="0" size="small"/></label>
                    <label class="hint">单分类每日上限<br><el-input-number v-model="scCyclePlan.category_day_max" :min="0" size="small" title="0=不限"/></label>
                    <label class="hint">主推送窗口起<br><el-input-number v-model="scCyclePlan.window_start" :min="0" :max="23" size="small"/></label>
                    <label class="hint">主推送窗口止<br><el-input-number v-model="scCyclePlan.window_end" :min="1" :max="24" size="small"/></label>
                  </template>
                  <template v-else-if="scCycleTab==='周度'">
                    <label class="hint">每周推送总配额<br><el-input-number v-model="scCyclePlan.week_max" :min="0" size="small"/></label>
                    <label class="hint">周度休闲窗口<br><el-switch v-model="scCyclePlan.week_rest_enabled"/></label>
                  </template>
                  <template v-else>
                    <label class="hint">月度推送总量配额<br><el-input-number v-model="scCyclePlan.month_max" :min="0" size="small"/></label>
                    <label class="hint">月度休刊<br><el-switch v-model="scCyclePlan.month_rest_enabled"/></label>
                  </template>
                </div>
              </div>

              <!-- 分类-时间排期配置：每分类一行（TMDB 官方分类）
                   v2.1.6：表格瘦身 + 勾选批量操作
                   - 去掉「周期维度 / 每小时上限 / 日上限 / 周维度建议」四列；
                   - 「偏好档位 + AI 建议」合并为单一「推荐时间段」（2 小时颗粒度）；
                   - 批量操作（一键恢复默认 / 批量采纳建议 / 保存）按勾选生效，
                     未勾选或全勾选 = 全部。 -->
              <div class="sc-card sc-hour-card sc-rule-full" v-show="scRuleTab==='分类时间排期'">
                <div class="sc-card-head"><span class="sc-ico">◷</span><div><h3>分类-时间排期配置</h3><p>按 TMDB 官方分类配置推送时段；默认按观影习惯分配 2 小时一段（热门分类排白天、冷门分类排夜间，自动避开静默时段），共 {{ scSchedRows.length }} 类</p></div>
                  <el-button size="small" plain :loading="scSchedSaving" @click="scApplySchedDefaults">一键恢复默认</el-button>
                  <el-button size="small" plain :loading="scAdoptingAllAi" @click="scAdoptAllAi">批量采纳 AI 建议</el-button>
                  <el-button size="small" type="primary" :loading="scSchedSaving" @click="scSaveAllSchedRows">保存</el-button></div>
                <div class="sc-table-scroll sc-sched-scroll">
                <el-table :data="scPrefSortRows(scSchedRows)" stripe size="small" max-height="420"
                          ref="scSchedTableRef" @selection-change="scSchedSelection = $event">
                  <el-table-column type="selection" width="46" fixed/>
                  <el-table-column label="TMDB 分类" width="120" fixed><template #default="s"><b>{{ s.row.name }}</b></template></el-table-column>
                  <el-table-column label="推荐时间段" min-width="188">
                    <template #default="s">
                      <!-- v2.1.6：用来源标记判定「有没有建议」，不能用起点数值真伪——
                           夜间段 00:00–02:00 的起点是 0，用 v-if="s.row.suggest_start"
                           会把这类建议显示成「暂无建议」并让采纳按钮置灰。 -->
                      <div v-if="s.row.suggest_source" class="sc-suggest-cell">
                        <span class="sc-suggest-time">{{ s.row.suggest_start }}:00–{{ s.row.suggest_end }}:00</span>
                        <span :class="['sc-mini-tag', s.row.suggest_source === 'history' ? '' : 'muted']">{{ s.row.suggest_source === 'history' ? '实测' : '习惯基线' }}</span>
                      </div>
                      <span v-else class="hint">暂无建议</span>
                      <div v-if="s.row.pref_rank" class="sc-suggest-rank">偏好第 {{ s.row.pref_rank }} 名</div>
                    </template>
                  </el-table-column>
                  <el-table-column label="配置时间段" width="196">
                    <template #default="s">
                      <div class="sc-time-cell">
                        <el-select v-model="s.row.window_start" size="small" class="sc-hour-select" placeholder="起">
                          <el-option v-for="h in scStartHourOptions" :key="'s'+h" :label="h + ':00'" :value="h" :disabled="scHourDisabled(h)"/>
                        </el-select>
                        <span class="sc-time-dash">–</span>
                        <el-select v-model="s.row.window_end" size="small" class="sc-hour-select" placeholder="止">
                          <el-option v-for="h in scEndHourOptions" :key="'e'+h" :label="h + ':00'" :value="h" :disabled="scEndHourDisabled(h)"/>
                        </el-select>
                      </div>
                      <div v-if="s.row.quiet_conflict" class="sc-cell-warn">与静默保护时段重叠</div>
                    </template>
                  </el-table-column>
                  <el-table-column label="是否采纳" width="88"><template #default="s"><span :class="['sc-dot-state', s.row.ai_adopted ? 'active' : 'unknown']">{{ s.row.ai_adopted ? '已采纳' : '未采纳' }}</span></template></el-table-column>
                  <el-table-column label="采纳操作" width="88"><template #default="s"><el-button size="small" link type="primary" :disabled="!s.row.suggest_source" @click="scAdoptSchedAI(s.row)">采纳建议</el-button></template></el-table-column>
                  <el-table-column label="启用" width="72"><template #default="s"><el-switch v-model="s.row.enabled" size="small"/></template></el-table-column>
                  <el-table-column label="操作" width="118" fixed="right"><template #default="s">
                    <el-button size="small" link type="primary" @click="scSaveSchedRow(s.row)">保存</el-button>
                    <el-button size="small" link @click="scResetSchedDefault(s.row)">默认</el-button>
                  </template></el-table-column>
                </el-table>
                </div>
                <div v-if="!scSchedRows.length" class="empty">暂无分类排期，点击「一键恢复默认」按 TMDB 官方分类初始化</div>
                <p class="hint sc-source-note">分类来自 tmdb.py 官方 genre（{{ scGenreCatalog.length }} 项）；推荐时间段为**单一时间段**（2 小时颗粒度），来源标注「实测」= 该分类真实发布的小时分布，「习惯基线」= 默认时间表派生的作息基线——每个分类都有建议，采纳按钮始终可用。</p>
                <p v-if="scQuietOn" class="hint sc-source-note">静默保护已开启（{{ scCyclePlan.quiet_start }}:00–{{ scCyclePlan.quiet_end }}:00），配置时间段中落在静默区间的小时**不可选**，默认时间表也会自动避开静默时段。</p>
                <p class="hint sc-source-note">默认时间表按观影习惯排布：热门分类优先占白天段、冷门分类排夜间段，每段 2 小时（{{ (scPreference.day_segments || []).length }} 个白天段 + {{ (scPreference.night_segments || []).length }} 个夜间段），段用完的分类保持全天 {{ (scPreference.full_day_window || [0, 24])[0] }}:00–{{ (scPreference.full_day_window || [0, 24])[1] }}:00。偏好排名由分类字典的人工优先级与该分类已成功推送条数真实派生（人工优先、热度次之）。速率上限统一由「周期计划管理」约束，不在分类表内单独配置。</p>
              </div>
              </div>
            </div>

            <!-- v2.1.1：按设计稿收敛——本页不单独挂 AI 建议卡（参考图无此模块）。
                 AI 建议仍由后端每周生成并进审核队列（sc_ai_auto_apply 默认关闭，
                 只出建议不直接推送），其采纳入口并入「发布规则」的分类时间排期
                 （每行有「采纳建议」按钮，走 /sc/category-schedules/adopt-ai）。 -->

            <!-- v2.1.1：按设计稿收敛——推送任务页不单独挂节奏参数配置卡
                 （参考图无此模块）。节奏参数仍在后端生效并已内置默认
                 （channel 2s/30 条每分、group 4s/20 条每分 + 1s 抖动），
                 需要调整时走 /sc/pacing 端点，不占用本页版面。 -->

            <!-- 设计稿推送任务：搜索 + 统计条 + 任务表 + 右侧详情面板 -->
            <div class="sc-task-layout" v-show="scSubNav === '推送任务'">
              <div class="sc-task-main">
                <div class="sc-form-row">
                  <el-input v-model="scTaskSearch" size="small" clearable placeholder="按作品名称搜索" style="width:220px"/>
                  <el-select v-model="scTaskStatusFilter" size="small" clearable placeholder="状态 全部" style="width:170px">
                    <el-option label="待发布（待发布 + 待推送）" value="pending"/>
                    <el-option label="推送中（提交/领取/推送/建链）" value="inflight"/>
                    <el-option v-for="opt in scTaskStatusOptions" :key="opt.value" :label="opt.label" :value="opt.value"/>
                  </el-select>
                  <el-select v-model="scTaskChannelFilter" size="small" clearable placeholder="渠道 全部" style="width:130px">
                    <el-option label="频道" value="channel"/>
                    <el-option label="群" value="group"/>
                  </el-select>
                  <el-select v-model="scTaskTypeFilter" size="small" style="width:120px">
                    <el-option label="推送类型 全部" value="全部"/><el-option label="首次" value="first"/><el-option label="追更" value="update"/>
                  </el-select>
                  <el-button size="small" type="primary">查询</el-button>
                  <el-button size="small" @click="scClearQueueFilter">清除筛选</el-button>
                  <el-button size="small" plain @click="scExportTasks">导出结果</el-button>
                </div>
                <!-- 统计条：全部真实来自 status_counts / link_state_counts -->
                <div class="sc-task-stats">
                  <div><small>待提交</small><b>{{ scOverview.status_counts?.planned || 0 }}</b></div>
                  <div><small>发送中</small><b>{{ (scOverview.status_counts?.building||0)+(scOverview.status_counts?.submitted||0)+(scOverview.status_counts?.accepted||0)+(scOverview.status_counts?.publishing||0) }}</b></div>
                  <div><small>发送</small><b>{{ scOverview.status_counts?.ready || 0 }}</b></div>
                  <div><small>成功</small><b class="ok">{{ scOverview.status_counts?.published || 0 }}</b></div>
                  <div><small>失败</small><b class="bad">{{ scOverview.status_counts?.failed || 0 }}</b></div>
                  <div><small>待建链</small><b class="muted">{{ scOverview.link_state_counts?.unknown || 0 }}</b></div>
                </div>
                <div class="sc-table-scroll">
                <el-table :data="scTaskViewRows" stripe size="small" max-height="420" @selection-change="s => scSelectedTasks = s">
                  <el-table-column type="selection" width="40"/>
                  <el-table-column label="作品" min-width="200">
                    <template #default="s">
                      <div class="sc-media-cell">
                        <div class="sc-media-thumb">
                          <el-image v-if="s.row.cover" :src="'https://image.tmdb.org/t/p/w185' + s.row.cover" fit="cover">
                            <template #error><div class="sc-thumb-fallback">无封面</div></template>
                          </el-image>
                          <div v-else class="sc-thumb-fallback">无封面</div>
                        </div>
                        <div class="sc-media-text">
                          <b>{{ s.row.title || '未命名作品' }}</b>
                          <small>{{ s.row.kind==='first' ? '首次发布' : '追更' }} · 编号 {{ s.row.id }}</small>
                        </div>
                      </div>
                    </template>
                  </el-table-column>
                  <el-table-column label="推送渠道" min-width="240" show-overflow-tooltip>
                    <template #default="s">
                      <div v-if="scChannelTargetLines(s.row).length" class="sc-target-cell">
                        <span v-for="(line, li) in scChannelTargetLines(s.row)" :key="li" class="sc-mini-tag">{{ line }}</span>
                      </div>
                      <span v-else class="hint">—</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="类型" width="86"><template #default="s">{{ s.row.kind==='first'?'首次发布':'追更' }}</template></el-table-column>
                  <el-table-column prop="share_url" label="分享记录" min-width="160" show-overflow-tooltip/>
                  <el-table-column label="推送目标" width="96"><template #default="s">{{ s.row.link_label || '待建链' }}</template></el-table-column>
                  <el-table-column label="状态" width="90"><template #default="s">{{ scTaskStatusLabel(s.row.status) }}</template></el-table-column>
                  <el-table-column prop="attempts" label="重试次数" width="86"/>
                  <el-table-column label="最后执行时间" min-width="150" show-overflow-tooltip><template #default="s">{{ fmtTime(s.row.updated_at) }}</template></el-table-column>
                  <el-table-column label="操作" width="80"><template #default="s"><el-button size="small" link type="primary" @click="scOpenTaskDetail(s.row.id)">详情</el-button></template></el-table-column>
                </el-table>
                </div>
                <div class="sc-form-row" style="margin-top:12px">
                  <el-button size="small" :disabled="!scSelectedTasks.length" @click="scBatchTask('retry')">提交重试</el-button>
                  <el-button size="small" :disabled="!scSelectedTasks.length" @click="scBatchTask('cancel')">取消任务</el-button>
                  <el-button size="small" plain :loading="scCleaningTasks" @click="scCleanupTasks">清理无效任务</el-button>
                </div>
                <div v-if="!scTaskViewRows.length" class="empty">暂无发布任务（当前筛选条件下为空）</div>
              </div>
            </div>

            <!-- ================================================================
                 v2.1.7 #241：阅读量统计
                 口径如实表达：①只有频道帖有浏览量，群聊 Telegram 根本不提供，
                 群聊目标因此不显示阅读量列；②展示的是**期间新增**（累计值相邻
                 两次采集求差），不是累计总数；③读不到的消息标记为「暂不可采集」，
                 **不显示 0**（0 会被误读成「没人看」）。
                 ================================================================ -->
            <div class="sc-stats-layout" v-show="scSubNav === '阅读量'">
              <!-- 顶部：就绪状态 + 采集操作 -->
              <div class="sc-card">
                <div class="sc-card-head">
                  <span class="sc-ico">◉</span>
                  <div><h3>阅读量统计</h3><p>统计频道帖的浏览量、转发与表情回应</p></div>
                  <div class="head-tools">
                    <el-select :model-value="scStatsDays" size="small" style="width:110px" @change="scStatsSwitchDays">
                      <el-option label="近 7 天" :value="7"/>
                      <el-option label="近 30 天" :value="30"/>
                      <el-option label="近 90 天" :value="90"/>
                    </el-select>
                    <el-button size="small" type="primary" :loading="scStatsCollecting" :disabled="!scStatsStatus.ready || !scStatsStatus.enabled" @click="scStatsCollect">立即采集</el-button>
                    <el-button size="small" plain :loading="scStatsLoading" @click="loadScStats">刷新</el-button>
                  </div>
                </div>

                <!-- 就绪状态：如实说明为什么不能用 -->
                <div class="sc-stats-state">
                  <span class="sc-state-dot" :class="scStatsStatus.ready ? 'on' : 'off'"></span>
                  <span v-if="scStatsStatus.ready">采集通道已就绪</span>
                  <span v-else>{{ scStatsStatus.reason || '尚未配置采集通道' }}</span>
                  <span class="sc-state-hint">{{ scStatsStatus.capability?.group_note || '群聊不提供浏览量统计' }}</span>
                </div>

                <!-- 配置区：凭证 + 采集参数 + 登录 -->
                <div class="sc-stats-config">
                  <div class="form-grid">
                    <div class="field">
                      <label>应用编号（api_id）</label>
                      <el-input v-model="scStatsConfig.api_id" placeholder="在 my.telegram.org 申请的数字编号"/>
                      <small>用于连接 Telegram 的应用凭据，加密存储，不会回显。</small>
                    </div>
                    <div class="field">
                      <label>应用密钥（api_hash）</label>
                      <el-input v-model="scStatsConfig.api_hash" type="password" show-password placeholder="32 位十六进制字符串"/>
                      <small>与编号配套使用，加密存储，不会回显。</small>
                    </div>
                    <div class="field">
                      <label>登录手机号</label>
                      <el-input v-model="scStatsConfig.phone" placeholder="含国际区号，如 +86"/>
                      <small>仅用于首次登录验证，验证码由 Telegram 直接发送给你。</small>
                    </div>
                    <div class="field inline"><label>启用采集</label><el-switch v-model="scStatsConfig.enabled"/></div>
                    <div class="field">
                      <label>回溯窗口</label>
                      <el-input-number v-model="scStatsConfig.lookback_hours" :min="1" :max="720" controls-position="right" style="width:130px"/><span class="unit">小时</span>
                      <small>只回溯这段时间内推送过的消息，避免每轮全量扫描。</small>
                    </div>
                    <div class="field">
                      <label>单轮上限</label>
                      <el-input-number v-model="scStatsConfig.max_per_run" :min="1" :max="2000" controls-position="right" style="width:130px"/><span class="unit">条</span>
                      <small>接口风暴护栏：单轮最多采集多少条消息。</small>
                    </div>
                  </div>
                  <div class="sc-form-row" style="margin-top:12px">
                    <el-button size="small" type="primary" :loading="scStatsSaving" @click="saveScStatsConfig">保存配置</el-button>
                    <el-button size="small" plain :loading="scStatsSaving" :disabled="!scStatsConfig.phone && !scStatsConfig.api_id" @click="scStatsSendLoginCode">获取登录验证码</el-button>
                    <el-input v-if="scStatsLoginCodeSent" v-model="scStatsLoginCode" size="small" placeholder="填写收到的验证码" style="width:170px"/>
                    <el-button v-if="scStatsLoginCodeSent" size="small" type="primary" plain :loading="scStatsSaving" @click="scStatsSubmitLoginCode">完成登录</el-button>
                  </div>
                </div>
              </div>

              <!-- 概览四数 -->
              <div class="sc-task-stats" style="margin-top:14px">
                <div><small>期间新增阅读</small><b>{{ scStatsTrend.totals?.reads_gain || 0 }}</b></div>
                <div><small>期间新增转发</small><b>{{ scStatsTrend.totals?.shares_gain || 0 }}</b></div>
                <div><small>期间新增回应</small><b>{{ scStatsTrend.totals?.reactions_gain || 0 }}</b></div>
                <div><small>统计消息数</small><b class="muted">{{ scStatsTrend.totals?.posts || 0 }}</b></div>
              </div>

              <!-- 按日趋势 + 各目标表现 -->
              <div class="sc-dash-three" style="margin-top:14px">
                <div class="sc-card">
                  <div class="sc-card-head"><span class="sc-ico">⌁</span><div><h3>每日新增阅读</h3><p>相邻两次采集的差值</p></div></div>
                  <el-table :data="scStatsTrend.buckets || []" stripe size="small" max-height="260">
                    <el-table-column prop="day" label="日期" min-width="110"/>
                    <el-table-column label="新增阅读" width="100"><template #default="s">{{ s.row.reads_gain || 0 }}</template></el-table-column>
                    <el-table-column label="新增转发" width="100"><template #default="s">{{ s.row.shares_gain || 0 }}</template></el-table-column>
                    <el-table-column label="新增回应" width="100"><template #default="s">{{ s.row.reactions_gain || 0 }}</template></el-table-column>
                  </el-table>
                  <div v-if="!(scStatsTrend.buckets || []).length" class="empty">暂无采集数据，开启采集并运行一轮后即可看到趋势</div>
                </div>
                <div class="sc-card">
                  <div class="sc-card-head"><span class="sc-ico">▤</span><div><h3>各推送目标表现</h3><p>频道与群聊分列</p></div></div>
                  <el-table :data="scStatsTrend.targets || []" stripe size="small" max-height="260">
                    <el-table-column prop="name" label="推送目标" min-width="140" show-overflow-tooltip/>
                    <el-table-column label="类型" width="76"><template #default="s">{{ s.row.kind_label || '频道' }}</template></el-table-column>
                    <el-table-column label="阅读量" width="100"><template #default="s">
                      <span v-if="s.row.reads_shown">{{ s.row.reads_total || 0 }}</span>
                      <span v-else class="muted" :title="'群聊不提供浏览量统计'">不适用</span>
                    </template></el-table-column>
                    <el-table-column label="转发" width="80"><template #default="s">{{ s.row.shares_total || 0 }}</template></el-table-column>
                    <el-table-column label="回应" width="80"><template #default="s">{{ s.row.reactions_total || 0 }}</template></el-table-column>
                  </el-table>
                  <div v-if="!(scStatsTrend.targets || []).length" class="empty">暂无目标数据</div>
                </div>
                <div class="sc-card">
                  <div class="sc-card-head"><span class="sc-ico">✦</span><div><h3>作品新增阅读榜</h3><p>按期间新增阅读排序</p></div></div>
                  <el-table :data="scStatsTrend.works || []" stripe size="small" max-height="260">
                    <el-table-column prop="title" label="作品" min-width="150" show-overflow-tooltip/>
                    <el-table-column label="新增阅读" width="100"><template #default="s">{{ s.row.reads_gain || 0 }}</template></el-table-column>
                    <el-table-column label="转发" width="80"><template #default="s">{{ s.row.shares_gain || 0 }}</template></el-table-column>
                    <el-table-column label="回应" width="80"><template #default="s">{{ s.row.reactions_gain || 0 }}</template></el-table-column>
                  </el-table>
                  <div v-if="!(scStatsTrend.works || []).length" class="empty">暂无作品数据</div>
                </div>
              </div>

              <!-- 最近采集明细 + 采集记录 -->
              <div class="sc-dash-three" style="margin-top:14px">
                <div class="sc-card">
                  <div class="sc-card-head"><span class="sc-ico">≡</span><div><h3>最近采集明细</h3><p>每条消息的最新状态</p></div></div>
                  <el-table :data="scStatsItems || []" stripe size="small" max-height="300">
                    <el-table-column prop="title" label="作品" min-width="140" show-overflow-tooltip/>
                    <el-table-column prop="target" label="推送目标" min-width="120" show-overflow-tooltip/>
                    <el-table-column label="类型" width="70"><template #default="s">{{ s.row.kind_label || '频道' }}</template></el-table-column>
                    <el-table-column label="阅读量" width="92"><template #default="s">
                      <span v-if="s.row.reads !== null && s.row.reads !== undefined">{{ s.row.reads }}</span>
                      <span v-else class="muted" :title="'群聊不提供浏览量统计'">不适用</span>
                    </template></el-table-column>
                    <el-table-column label="转发" width="76"><template #default="s">{{ s.row.shares || 0 }}</template></el-table-column>
                    <el-table-column label="回应" width="76"><template #default="s">{{ s.row.reactions || 0 }}</template></el-table-column>
                    <el-table-column label="状态" width="110"><template #default="s">
                      <el-tag v-if="s.row.available" size="small" type="success" effect="plain">已采集</el-tag>
                      <el-tooltip v-else :content="s.row.note || '该消息暂不可采集'" placement="top">
                        <el-tag size="small" type="info" effect="plain">暂不可采集</el-tag>
                      </el-tooltip>
                    </template></el-table-column>
                  </el-table>
                  <div v-if="!(scStatsItems || []).length" class="empty">暂无采集明细</div>
                </div>
                <div class="sc-card">
                  <div class="sc-card-head"><span class="sc-ico">◷</span><div><h3>采集记录</h3><p>每轮采集的执行结果</p></div></div>
                  <el-table :data="scStatsRuns || []" stripe size="small" max-height="300">
                    <el-table-column label="状态" width="110"><template #default="s">
                      <el-tag size="small" :type="s.row.status === 'success' ? 'success' : (s.row.status === 'running' ? 'warning' : 'info')" effect="plain">{{ scStatsRunLabel(s.row.status) }}</el-tag>
                    </template></el-table-column>
                    <el-table-column label="扫描" width="76"><template #default="s">{{ s.row.scanned || 0 }}</template></el-table-column>
                    <el-table-column label="采集" width="76"><template #default="s">{{ s.row.collected || 0 }}</template></el-table-column>
                    <el-table-column label="跳过" width="76"><template #default="s">{{ s.row.skipped || 0 }}</template></el-table-column>
                    <el-table-column prop="error" label="说明" min-width="150" show-overflow-tooltip/>
                  </el-table>
                  <div v-if="!(scStatsRuns || []).length" class="empty">尚无采集记录</div>
                </div>
              </div>
            </div>
          </div>
          </div><!-- /sc-workspace -->
          </div><!-- /sc-shell -->

          <!-- 任务详情抽屉：真实端点 /sc/tasks/{id} -->
          <el-drawer v-model="scDetailVisible" size="46%" :title="'任务详情 #' + (scDetailTask?.id || '')">
            <template v-if="scDetailTask">
              <!-- v2.1.4：详情内页展示 TMDB 海报缩略图 -->
              <div class="sc-detail-poster">
                <el-image v-if="scDetailTask.cover" :src="'https://image.tmdb.org/t/p/w342' + scDetailTask.cover" fit="cover">
                  <template #error><div class="sc-thumb-fallback">无封面</div></template>
                </el-image>
                <div v-else class="sc-thumb-fallback">无封面</div>
              </div>
              <div class="sc-detail-kv">
                <div><span>作品</span><b>{{ scDetailTask.title || '未命名作品' }}</b></div>
                <div><span>状态</span><b>{{ scTaskStatusLabel(scDetailTask.status) }}</b></div>
                <div><span>类型</span><b>{{ scDetailTask.kind==='first'?'首次发布':'追更' }}</b></div>
                <div><span>推送渠道</span><b>{{ scDetailTask.target_summary || '—' }}</b></div>
                <div><span>链接状态</span><b>{{ scDetailTask.link_label || '待建链' }}</b></div>
                <div><span>尝试次数</span><b>{{ scDetailTask.attempts }}</b></div>
                <div><span>分享链接</span><b style="word-break:break-all">{{ scDetailTask.share_url || '—' }}</b></div>
                <div><span>计划执行时间</span><b>{{ fmtTime(scDetailTask.scheduled_at, true) }}</b></div>
                <div><span>发布时间</span><b>{{ fmtTime(scDetailTask.published_at, true) }}</b></div>
                <div><span>错误</span><b style="word-break:break-all">{{ scDetailTask.error || '—' }}</b></div>
              </div>
              <div class="sc-form-row" style="margin-top:12px">
                <el-button size="small" type="primary" @click="scTaskAction(scDetailTask.id,'retry')">提交重试</el-button>
                <el-button size="small" danger plain @click="scTaskAction(scDetailTask.id,'cancel')">取消任务</el-button>
              </div>
              <div class="sc-subhead">执行记录（{{ scDetailExecutions.length }}）</div>
              <el-table :data="scDetailExecutions" stripe size="small" max-height="360">
                <el-table-column prop="id" label="ID" width="60"/>
                <el-table-column label="阶段" width="110"><template #default="s">{{ scExecStageLabel(s.row.stage) }}</template></el-table-column>
                <el-table-column label="状态" width="90"><template #default="s">{{ scExecStatusLabel(s.row.status) }}</template></el-table-column>
                <el-table-column label="耗时" width="90"><template #default="s">{{ fmtDuration(s.row.duration_ms) }}</template></el-table-column>
                <el-table-column label="错误" min-width="160" show-overflow-tooltip><template #default="s">{{ s.row.error || '—' }}</template></el-table-column>
                <el-table-column label="开始" width="150" show-overflow-tooltip><template #default="s">{{ fmtTime(s.row.started_at, true) }}</template></el-table-column>
              </el-table>
              <div v-if="!scDetailExecutions.length" class="empty">该任务暂无执行记录</div>
            </template>
          </el-drawer>
        </section>

        <section v-else-if="tab === 'TMDB 设置'" class="fade">
          <div class="page-head"><div><h2>TMDB 设置</h2><p>自动识别电影/剧集，补全海报与简介；可选功能，不配置不影响扫描、建分享和推送</p></div><el-button plain @click="loadTmdb">刷新状态</el-button></div>

          <div class="panel" style="margin-bottom:16px">
            <div class="panel-head"><h3>当前状态</h3>
              <el-tag :type="tmdbStatus.configured ? 'success' : 'warning'" size="small">{{ tmdbStatus.configured ? '已配置' : '未配置' }}</el-tag>
            </div>
            <div class="auth-summary" style="margin-bottom:0">
              <div class="auth-summary-item"><small>识别方式</small><b>{{ tmdbStatus.mode === 'v4-bearer' ? 'v4 Read Access Token' : (tmdbStatus.mode === 'v3-key' ? 'v3 API Key' : '—') }}</b></div>
              <div class="auth-summary-item"><small>已保存密钥</small><b>{{ tmdbStatus.masked || '—' }}</b></div>
              <div class="auth-summary-item"><small>语言</small><b>{{ tmdbStatus.language || 'zh-CN' }}</b></div>
              <div class="auth-summary-item"><small>格式检查</small><b :class="tmdbStatus.format_warning ? 'bad' : 'good'">{{ tmdbStatus.format_warning ? '格式可疑，请重新粘贴' : '正常' }}</b></div>
            </div>
          </div>

          <div class="panel">
            <div class="panel-head"><h3>配置密钥</h3><span class="hint">密钥加密保存在本机数据库，无需改 .env、无需重启</span></div>
            <p class="hint" style="margin:0 0 14px">
              二选一即可：<b>v4 Read Access Token</b>（以 <code>eyJ</code> 开头的长串，推荐）或 <b>v3 API Key</b>（32 位十六进制）。
              系统会自动按格式判断类型，不用区分填在哪个框。
            </p>
            <div class="form-grid">
              <div><small>v4 Read Access Token（推荐）</small><el-input v-model="tmdbForm.bearer_token" type="password" show-password placeholder="eyJhbGciOi..." clearable/></div>
              <div><small>v3 API Key（32 位十六进制）</small><el-input v-model="tmdbForm.api_key" type="password" show-password placeholder="例如 1a2b3c...（32 位）" clearable/></div>
              <div><small>语言</small><el-input v-model="tmdbForm.language" placeholder="zh-CN"/></div>
            </div>
            <div class="connect-actions" style="margin-top:16px">
              <el-button type="primary" :loading="tmdbSaving" @click="saveTmdb">保存配置</el-button>
              <el-button plain :loading="tmdbTesting" @click="testTmdb">测试连接</el-button>
            </div>
            <p class="hint" style="margin-top:14px">
              没有密钥？到 <a href="https://www.themoviedb.org/settings/api" target="_blank" class="device-link">TMDB 账号设置 → API</a> 免费申请，
              复制 <b>API Read Access Token</b> 粘贴到上面第一个框。
            </p>
          </div>

          <!-- v0.9.8：按场景独立限流，Bot 默认更保守防恶意刷 -->
          <div class="panel">
            <div class="panel-head"><h3>调用限流（防止 TMDB 账号被风控）</h3><span class="hint">两个场景独立统计，留空保持默认；保存配置即刻生效</span></div>
            <p class="hint" style="margin:0 0 14px">
              <b>Bot 场景</b>用于私聊求片/投稿的候选搜索，对外暴露、默认间隔久频次低，避免被恶意刷导致 TMDB 账号异常；
              <b>资源库识别</b>用于内部扫描识别，可更宽松。超出间隔或频次时本次不请求 TMDB，自动降级。
            </p>
            <div class="form-grid">
              <div><small>识别 · 最小间隔（秒）</small><el-input v-model="tmdbForm.scan_min_interval" placeholder="默认 0.2"/></div>
              <div><small>识别 · 每窗口最大次数</small><el-input v-model="tmdbForm.scan_max_calls" placeholder="默认 60"/></div>
              <div><small>Bot · 最小间隔（秒）</small><el-input v-model="tmdbForm.bot_min_interval" placeholder="默认 3"/></div>
              <div><small>Bot · 每窗口最大次数</small><el-input v-model="tmdbForm.bot_max_calls" placeholder="默认 20"/></div>
              <div><small>统计窗口（秒）</small><el-input v-model="tmdbForm.rate_window_seconds" placeholder="默认 60"/></div>
            </div>
            <div class="connect-actions" style="margin-top:16px">
              <el-button type="primary" :loading="tmdbSaving" @click="saveTmdb">保存限流配置</el-button>
            </div>
            <p class="hint" style="margin-top:12px">
              当前生效：识别 {{ tmdbStatus.rate_limit?.scan_min_interval || '0.2' }}s / {{ tmdbStatus.rate_limit?.scan_max_calls || '60' }} 次；
              Bot {{ tmdbStatus.rate_limit?.bot_min_interval || '3' }}s / {{ tmdbStatus.rate_limit?.bot_max_calls || '20' }} 次（每 {{ tmdbStatus.rate_limit?.rate_window_seconds || '60' }}s）。
            </p>
          </div>

          <div class="panel" style="margin-top:16px">
            <div class="panel-head"><h3>说明</h3></div>
            <p class="hint" style="margin:0 0 8px">未配置或密钥无效时，资源仍会正常入库、创建分享并推送到 Telegram，只是没有海报、简介和 TMDB 链接。配置好之后，回到「资源库」对已有资源点「自动识别」即可补全。</p>
            <p class="hint" style="margin:0">若之前报 <code>HTTP 401 Invalid API key</code>，多半是把 v4 Token 当成 v3 Key 用了，或密钥已失效；重新粘贴后点「测试连接」确认。</p>
          </div>

          <div class="panel" style="margin-top:16px">
            <div class="panel-head"><h3>AI 辅助识别（可选）</h3>
              <el-tag :type="aiStatus.active ? 'success' : 'info'" size="small">{{ aiStatus.active ? '已启用' : '未启用' }}</el-tag>
            </div>
            <p class="hint" style="margin:0 0 14px">
              接入任意 <b>OpenAI 兼容</b>接口（OpenAI、DeepSeek、Ollama、各类中转均可）。AI 只用于解析文件名、
              主搜索失败时提供备用片名、以及电影/剧集候选接近时消歧；<b>不可用时自动降级</b>到本地规则 + TMDB，
              不影响扫描、建分享、推送。带季集信息的文件仍按剧集处理，AI 不会把它改成电影。
            </p>
            <div class="form-grid">
              <div><small>启用 AI 辅助识别</small><div class="mt"><el-switch v-model="aiForm.enabled" active-text="启用" inactive-text="关闭"/></div></div>
              <div><small>模型名称</small><el-input v-model="aiForm.model" placeholder="gpt-4o-mini"/></div>
              <div><small>接口地址 Base URL</small><el-input v-model="aiForm.base_url" placeholder="https://api.openai.com/v1"/></div>
              <div><small>API Key（加密保存）</small><el-input v-model="aiForm.api_key" type="password" show-password placeholder="sk-..." clearable/></div>
              <div><small>超时（秒）</small><el-input-number v-model="aiForm.timeout" :min="5" :max="120" style="width:100%"/></div>
            </div>
            <div class="field mt"><small>自定义补充要求（可选）</small><el-input v-model="aiForm.prompt" type="textarea" :rows="2" placeholder="例如：片名优先用中文译名；动漫统一按剧集处理"/></div>
            <div class="connect-actions" style="margin-top:14px">
              <el-button type="primary" :loading="aiSaving" @click="saveAi">保存 AI 配置</el-button>
              <el-button plain :loading="aiTesting" @click="testAi">测试连接</el-button>
            </div>
            <div class="ai-test">
              <el-input v-model="aiParseName" placeholder="输入一个文件名，测试 AI 解析结果" @keyup.enter="parseAi"/>
              <el-button plain @click="parseAi">识别测试</el-button>
            </div>
            <div v-if="aiParseResult" class="ai-result"><pre>{{ JSON.stringify(aiParseResult, null, 2) }}</pre></div>
            <p class="hint">当前：{{ aiStatus.masked || '未配置密钥' }} · {{ aiStatus.model || '—' }} · {{ aiStatus.base_url || '—' }}</p>
          </div>
        </section>

        <section v-else-if="tab === '消息模板'" class="fade">
          <div class="page-head"><div><h2>消息模板</h2><p>内置模板一键载入、可自定义；支持双花括号变量与可嵌套条件块；有海报时自动发送图片</p></div><el-button plain @click="restoreDefaultTpl">载入 默认模板</el-button></div>

          <!-- v0.9.10.2：三个跳转按钮并排同一行（频道/群聊/投稿·求片），渲染层统一追加到所有模板；「获取资源」单独一行 -->
          <div class="panel" style="margin-bottom:14px">
            <div class="panel-head"><h3>推送消息 · 跳转链接</h3><span>三个入口做成并排按钮（频道 / 群聊 / 投稿·求片），渲染层统一追加到所有模板，留空的不显示；「获取资源」按钮单独一行</span></div>
            <div class="three-col">
              <div class="panel narrow">
                <div class="panel-head"><h4>频道跳转</h4><el-tag size="small" :type="interactionCfg.tg_channel_link ? 'success' : 'info'">{{ interactionCfg.tg_channel_link ? '已启用' : '未启用' }}</el-tag></div>
                <el-input v-model="interactionCfg.tg_channel_link" placeholder="@频道 或 https://t.me/频道"/>
                <p class="hint">填值后推送消息追加「频道」按钮。</p>
              </div>
              <div class="panel narrow">
                <div class="panel-head"><h4>群聊跳转</h4><el-tag size="small" :type="interactionCfg.tg_group_link ? 'success' : 'info'">{{ interactionCfg.tg_group_link ? '已启用' : '未启用' }}</el-tag></div>
                <el-input v-model="interactionCfg.tg_group_link" placeholder="@群聊 或 https://t.me/群聊"/>
                <p class="hint">填值后推送消息追加「群聊」按钮。</p>
              </div>
              <div class="panel narrow">
                <div class="panel-head"><h4>投稿·求片跳转</h4><el-tag size="small" :type="interactionCfg.tg_bot_link ? 'success' : 'info'">{{ interactionCfg.tg_bot_link ? '已启用' : '未启用' }}</el-tag></div>
                <el-input v-model="interactionCfg.tg_bot_link" placeholder="@推送机器人 或 https://t.me/推送机器人"/>
                <p class="hint">填值后推送消息追加「投稿·求片」按钮（指向推送 Bot 本身）。</p>
              </div>
            </div>
            <div class="panel-foot"><el-button type="primary" plain :loading="interactionSaving" @click="saveJumpLinks">保存消息跳转链接</el-button><span class="hint" style="margin-left:10px">也可在「分发与互动中心」点顶部统一保存按钮一起保存。</span></div>
          </div>

          <!-- v0.9.6：推送渲染选项（从已删除的 TG 推送配置卡迁入，后端逻辑不变） -->
          <div class="panel narrow" style="margin-bottom:14px">
            <div class="panel-head"><h3>推送渲染选项</h3><span>分享链接样式 · 详情篇幅（影响所有模板的推送消息）</span></div>
            <div class="form-grid">
              <div class="field"><label>分享链接样式</label><el-select v-model="tgExtra.link_style" style="width:100%"><el-option label="按钮（推荐）" value="button"/><el-option label="纯文本" value="plain"/></el-select><small>按钮模式把分享链接渲染成「📥 获取资源」inline 按钮，并去掉正文末尾的纯文本链接。</small></div>
              <div class="field"><label>详情篇幅</label><el-select v-model="tgExtra.detail_level" style="width:100%"><el-option label="精简" value="compact"/><el-option label="完整" value="full"/></el-select><small>精简把简介压到 110 字；完整保留整段简介。</small></div>
            </div>
            <div class="panel-foot"><el-button type="primary" :loading="tgExtraSaving" @click="saveTgExtra">保存渲染选项</el-button></div>
          </div>

          <div class="two-col tpl-layout">
            <div class="panel">
              <div class="field"><label>内置模板（一键载入，可改后另存）</label><el-select v-model="tplPresetPick" placeholder="选择一套内置模板" style="width:100%" @change="applyTplPreset"><el-option v-for="p in tplPresets" :key="p.name" :label="p.name" :value="p.name"/></el-select><small>推送只使用「默认」模板：载入后请勾选「设为默认」再保存，或保存后在下方列表点「设为默认」；只保存不设默认不会改变推送样式。</small></div>
              <div class="field mt"><label>模板名称</label><el-input v-model="tpl.name" placeholder="例如：电影发布模板"/></div>
              <div class="field mt"><label>消息内容</label><el-input v-model="tpl.content" type="textarea" :rows="10" placeholder="在这里编写 Telegram 消息内容"/></div>
              <div class="mt"><el-switch v-model="tpl.enabled" active-text="启用"/><el-switch v-model="tpl.is_default" active-text="设为默认" class="ml"/></div>
              <el-button type="primary" class="mt" @click="saveTpl">保存模板</el-button>
              <el-button v-if="tpl.id" class="mt" plain @click="tpl = { name:'', content:'', enabled:true, is_default:false }; tplPresetPick=''">新建空模板</el-button>
              <div class="tpl-list"><div v-for="t in templates" :key="t.id" class="mini-row tpl-mini"><span class="tpl-mini-name" @click="editTpl(t)"><b>{{ t.name }}</b><el-tag v-if="t.is_default" type="success" size="small">默认</el-tag></span><el-button size="small" text type="primary" :disabled="t.is_default" @click="setDefaultTpl(t)">设为默认</el-button><el-button size="small" text type="danger" @click="deleteTpl(t)">删除</el-button></div></div>
            </div>
            <div class="panel">
              <div class="panel-head"><h3>可用变量</h3><span>点击插入</span></div>
              <div class="var-grid"><button v-for="v in templateVars" :key="v" @click="insertVar(v)">{{ v }}</button></div>
              <p class="hint" style="margin:0 0 12px">{{ tagHint }}</p>
              <p class="hint" style="margin:0 0 12px">{{ episodeHint }}</p>
              <div class="preview-box"><div class="preview-title">预览效果</div><div class="preview-body">{{ previewText }}</div></div>
              <p class="hint">条件块写法：{{ syntaxHint }}</p>
            </div>
          </div>
        </section>

        <section v-else-if="tab === '操作审计'" class="fade">
          <div class="page-head"><div><h2>操作审计</h2><p>授权、建分享、推送、审核与重试的全量记录</p></div><el-tag>{{ audit.length }} 条</el-tag></div>
          <div class="panel" style="margin-bottom:14px">
            <div class="panel-head"><h3>日志保留与清理</h3><el-tag size="small" type="info">共 {{ logOverview.total_rows }} 条 · 可清理 {{ logOverview.total_pruneable }} 条</el-tag></div>
            <div class="form-grid">
              <div class="field"><label>保留天数</label><el-input-number v-model="logCfg.retention_days" :min="1" :max="3650" controls-position="right" style="width:150px"/><small>超过天数的历史日志自动清理</small></div>
              <div class="field"><label>每类日志上限（条）</label><el-input-number v-model="logCfg.max_rows" :min="100" :max="200000" :step="500" controls-position="right" style="width:160px"/><small>超出时从最旧的记录开始删</small></div>
              <div class="field inline"><label>自动清理</label><el-switch v-model="logCfg.auto_prune"/></div>
            </div>
            <p class="hint">{{ logCfg.schedule }}；关闭自动清理后只能手动点「立即清理」或「清空全部日志」。</p>
            <div class="panel-foot"><el-button type="primary" :loading="logBusy" @click="saveLogCfg">保存保留策略</el-button><el-button :loading="logBusy" @click="pruneLogs">立即清理</el-button><el-button type="danger" plain :loading="logBusy" @click="clearLogs">清空全部日志</el-button></div>
            <div class="table-scroll"><el-table :data="logStats" size="small">
              <el-table-column prop="label" label="日志类别" width="150"/>
              <el-table-column prop="rows" label="总条数" width="100"/>
              <el-table-column prop="pruneable" label="可清理" width="100"/>
              <el-table-column prop="protected" label="绝不删除" min-width="210"/>
              <el-table-column prop="oldest" label="最早" min-width="170"/>
              <el-table-column prop="newest" label="最新" min-width="170"/>
            </el-table></div>
            <p class="hint">只清理日志与历史留痕；资源、目录映射、当前文件映射、待推送队列与待处理分发事件一律保留。{{ logOverview.container_log }}</p>
          </div>
          <div class="panel"><div class="timeline full"><div v-for="a in audit" :key="a.id" class="tl-row"><span class="tl-dot"></span><span class="tl-time">{{ a.ts }}</span><b class="tl-action">{{ a.action }}</b><span class="tl-target">{{ a.target_type }} {{ a.target_id }}</span><span class="tl-detail">{{ a.detail }}</span></div><div v-if="!audit.length" class="empty big">暂无审计记录</div></div></div>
        </section>
      </main>

      <!-- LitePan 风格「重新匹配元数据」：类型 + 片名/ID + 海报候选 + 应用匹配 / 无匹配 -->
      <el-dialog v-model="matchVisible" title="重新匹配元数据" width="760px" top="7vh" :close-on-click-modal="false">
        <div class="match-path"><small>作品文件</small><b>{{ matchRow?.name }}</b><small v-if="matchRow?.path_hint" style="margin-top:6px">目录线索：{{ matchRow.path_hint }} · 默认按中文剧名搜索</small></div>
        <div class="match-controls">
          <el-select v-model="matchType" style="width:112px"><el-option label="剧集" value="tv"/><el-option label="电影" value="movie"/></el-select>
          <el-input v-model="matchQuery" placeholder="填中文片名 / 外语片名 / TMDB ID 后点「搜索」" clearable @keyup.enter="searchMatch(false)"/>
          <el-button type="primary" :loading="matchBusy" @click="searchMatch(false)">搜索</el-button>
        </div>
        <p class="hint" style="margin:8px 0 4px">{{ matchClue }}。可凭海报和类型选择，选中后点「应用匹配」。</p>
        <p v-if="matchTranslated.length" class="hint" style="margin:0 0 10px">已自动译成外语标题：{{ matchTranslated.join(' / ') }}；依次尝试：{{ matchTried.map(t => t.query).join(' / ') }}</p>
        <p v-else class="hint" style="margin:0 0 10px">输入中文片名后会自动译成英/日/韩等外语标题一起搜索；未启用 AI 时仍按中文原名搜索，或直接填 TMDB ID。</p>
        <div v-if="matchCandidates.length" class="match-grid">
          <div v-for="c in matchCandidates" :key="c.tmdb_id + '-' + c.media_type" class="match-card" :class="{ on: matchSelected === c }" @click="matchSelected = c">
            <div class="match-poster">
              <el-image v-if="c.poster_path || c.poster_url" :src="c.poster_url || ('https://image.tmdb.org/t/p/w185' + c.poster_path)" fit="cover" style="width:100%;height:100%"/>
              <div v-else class="match-no-poster">无海报</div>
            </div>
            <div class="match-info">
              <b :title="c.title">{{ c.title }}</b>
              <small v-if="c.original_title && c.original_title !== c.title">{{ c.original_title }}</small>
              <span class="match-type" :class="'type-'+c.media_type">{{ c.media_type === 'tv' ? '剧集' : '电影' }} · {{ c.year || '—' }}</span>
              <span>TMDB #{{ c.tmdb_id }}</span>
              <span v-if="c.genres?.length" class="match-genres">{{ c.genres.slice(0,3).join('、') }}</span>
            </div>
          </div>
        </div>
        <div v-else-if="!matchBusy" class="empty">暂无候选，换个中文/外语片名、TMDB ID 或类型再试</div>
        <template #footer>
          <div class="match-footer">
            <el-button type="danger" plain @click="markNoMatch">无匹配</el-button>
            <div><el-button @click="matchVisible = false">取消</el-button><el-button type="primary" :disabled="!matchSelected" :loading="matchBusy" @click="applyMatch">应用匹配</el-button></div>
          </div>
        </template>
      </el-dialog>
    </div>
  </div>
</template>

<style>
:root{
  --yg-bg:#f5f6fb;--yg-surface:#fff;--yg-ink:#1e2745;--yg-muted:#8992ac;--yg-line:#edf0f7;
  --yg-primary:#5b5bd6;--yg-primary-2:#8271ef;--yg-teal:#19b3a6;--yg-amber:#f2a33c;--yg-rose:#e5526b;
  --yg-shadow:0 1px 2px rgba(30,39,69,.03),0 12px 32px rgba(30,39,69,.06);
  --yg-shadow-lg:0 2px 4px rgba(30,39,69,.04),0 20px 48px rgba(30,39,69,.10);
  --yg-radius:20px;--yg-radius-sm:14px;
  /* v2.1.6 #245：全局语义色 token（浅色基线）。
     此前这些色值散落成 20+ 处硬编码字面量（#eef0ff / #e7f8f0 / #fff3e2 / #fdecef …），
     深色主题下无法跟随主题 → 逐处失配（浅底 + 深字在深色背景上刺眼且对比不足）。
     现在统一成「底色 + 字色」成对 token：浅色写在这里，深色在 html.dark 里换值，
     换主题只改一处，页面样式不再出现裸色值。 */
  --yg-tone-blue-bg:#eef0ff;--yg-tone-blue-fg:#5b5bd6;
  --yg-tone-green-bg:#e7f8f0;--yg-tone-green-fg:#159a6b;
  --yg-tone-amber-bg:#fff3e2;--yg-tone-amber-fg:#cf7f1c;
  --yg-tone-red-bg:#fdecef;--yg-tone-red-fg:#d9435e;
  --yg-tone-violet-bg:#f0ebff;--yg-tone-violet-fg:#7b5bd6;
  --yg-tone-teal-bg:#e6f7f7;--yg-tone-teal-fg:#199a9a;
  --yg-tone-slate-bg:#eef1f7;--yg-tone-slate-fg:#7e88a8;
  --yg-panel:#f7f8fd;
  --yg-grad-a:#eef0ff;--yg-grad-b:#e9f8f5;
  --yg-border:#e6e8eb;--yg-panel-soft:rgba(127,127,127,.05);
  --el-color-primary:#5b5bd6;--el-border-radius-base:10px;--el-font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
}
*{box-sizing:border-box}
body{margin:0;background:radial-gradient(1200px 600px at 100% -10%,var(--yg-grad-a) 0%,transparent 55%),radial-gradient(900px 500px at -10% 100%,var(--yg-grad-b) 0%,transparent 55%),var(--yg-bg);background-attachment:fixed;color:var(--yg-ink);font-family:var(--el-font-family);font-size:14px;-webkit-font-smoothing:antialiased}
button{font-family:inherit}
.ico{width:16px;height:16px;fill:none;stroke:currentColor;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round;flex:none}
/* ============ v0.9.0 全站设计语言：embyhub 图标块 + 大圆角白卡 ============ */
/* 所有面板标题统一带渐变图标块，卡片获得柔和双层阴影与悬浮反馈 */
.panel-head h3{display:flex;align-items:center;gap:11px}
.panel-head h3::before{content:"";width:24px;height:24px;border-radius:8px;flex:none;
  background:linear-gradient(135deg,var(--yg-primary-2),var(--yg-primary));
  box-shadow:0 4px 10px rgba(91,91,214,.26),inset 0 0 0 1px rgba(255,255,255,.18);
  background-image:radial-gradient(circle at 50% 50%,#fff 0 2.6px,transparent 3px)}
/* 页面主标题：左侧双色竖条，层次更清晰 */
.page-head h2{display:flex;align-items:center;gap:11px}
.page-head h2::before{content:"";width:5px;height:23px;border-radius:4px;flex:none;
  background:linear-gradient(180deg,var(--yg-primary-2),var(--yg-primary));box-shadow:0 2px 6px rgba(91,91,214,.28)}
/* 顶栏、表格、表单控件全站统一圆角与留白 */
.topbar{box-shadow:0 1px 0 var(--yg-line),0 6px 20px rgba(30,39,69,.03)}
.el-table{--el-table-border-color:var(--yg-line);border-radius:12px;overflow:hidden}
.el-table th.el-table__cell{background:var(--yg-panel)!important;color:var(--yg-muted);font-weight:650;font-size:12px}
.el-input__wrapper,.el-select__wrapper{border-radius:10px;box-shadow:0 0 0 1px var(--yg-line) inset}
.el-button{border-radius:10px;font-weight:600}
.el-tag{border-radius:8px}
/* 二级数据卡：授权摘要 / 凭证条 / 分享会话，圆角阴影对齐主卡。
   v2.1.6 #245：原先这里还有一条独立的 ``.auth-summary-item{box-shadow:…;border-color:…}``
   与一条 ``.method-card{border-radius:var(--yg-radius-sm)}``——
   前者的属性已被下面的主定义完全覆盖（死代码），后者被 .method-card 主定义的
   var(--yg-radius) 覆盖（同特异度后者胜 → 声明已成死代码）。
   两条一并清除，避免「同选择器多处定义」这种隐性坑（v2.1.5 的层叠顺序教训）。 */
.credential-bar,.kv-grid>div{border-radius:var(--yg-radius-sm)}
/* 概览「最近操作」卡片标题图标块颜色随卡片语义微调 */
.stat-card .stat-bar i{opacity:.9}
.app-shell{display:flex;min-height:100vh}
.sidebar{width:238px;flex:none;background:linear-gradient(180deg,#252a4e 0%,#171b35 100%);color:#e9ecff;display:flex;flex-direction:column;position:sticky;top:0;height:100vh}
.brand{display:flex;align-items:center;gap:12px;padding:24px 20px 22px;border-bottom:1px solid rgba(255,255,255,.09)}
.brand-mark{width:38px;height:38px;border-radius:11px;background:linear-gradient(135deg,var(--yg-primary-2),#43c9bd);display:grid;place-items:center;font-weight:800;font-size:14px;letter-spacing:.5px;color:#fff;box-shadow:0 6px 16px rgba(91,91,214,.35)}
.brand-mark.lg{width:52px;height:52px;border-radius:15px;font-size:18px}
/* v0.9.13 #250：品牌区使用项目图标（光鸭橙 + 纸飞机） */
.brand-logo{width:38px;height:38px;border-radius:11px;object-fit:cover;box-shadow:0 6px 16px rgba(242,78,0,.28);flex:none}
.brand-logo.lg{width:52px;height:52px;border-radius:15px}
.brand-name{font-size:18px;font-weight:750;letter-spacing:.3px}
.brand-sub{font-size:11px;color:#9ea6d4;margin-top:2px}
.nav-groups{flex:1;padding:16px 12px;overflow-y:auto}
.nav-group{margin-bottom:18px}
.nav-group-label{font-size:10px;letter-spacing:1.6px;color:#7983b8;padding:0 12px 8px;text-transform:uppercase}
.nav-item{width:100%;display:flex;align-items:center;gap:11px;padding:10px 12px;border:0;background:transparent;color:#b9c0e6;border-radius:10px;cursor:pointer;font-size:13px;text-align:left;transition:.18s}
.nav-item:hover{background:rgba(255,255,255,.07);color:#fff}
.nav-item.active{background:linear-gradient(90deg,rgba(130,113,239,.28),rgba(130,113,239,.08));color:#fff;box-shadow:inset 3px 0 0 #8a7bf5}
.nav-badge{margin-left:auto;background:var(--yg-rose);color:#fff;border-radius:9px;padding:1px 7px;font-size:10px;font-weight:700}
.sidebar-foot{padding:16px 20px;border-top:1px solid rgba(255,255,255,.09);font-size:11px;color:#98a1ce}
.foot-status{display:flex;align-items:center;gap:8px;margin-bottom:6px}
.foot-version{display:flex;align-items:center;gap:10px}
.foot-gh{color:#8a9cff;text-decoration:none;font-weight:600;border-bottom:1px dashed rgba(138,156,255,.5)}
.foot-gh:hover{color:#b9c4ff;border-bottom-style:solid}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block}.dot.on{background:#39d4b0;box-shadow:0 0 0 4px rgba(57,212,176,.14)}.dot.off{background:#f0a33c;box-shadow:0 0 0 4px rgba(240,163,60,.14)}
.main-wrap{flex:1;min-width:0}
.topbar{height:66px;background:rgba(255,255,255,.88);backdrop-filter:blur(10px);border-bottom:1px solid var(--yg-line);display:flex;align-items:center;justify-content:space-between;padding:0 28px;position:sticky;top:0;z-index:20}
.crumb{display:flex;align-items:center;gap:10px;color:var(--yg-muted);font-size:13px}.crumb b{color:#c8cfe3;font-weight:400}.crumb strong{color:var(--yg-ink);font-weight:700}
.top-actions{display:flex;align-items:center;gap:10px}.clock{font-variant-numeric:tabular-nums;color:var(--yg-muted);font-size:12px;margin-right:6px}.session-warn{color:#e5526b;font-weight:700}
.top-actions .ico{margin-right:5px}
.content{max-width:1480px;margin:0 auto;padding:26px 28px 60px}
.fade{animation:fade .3s ease}@keyframes fade{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
.page-head{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:22px;gap:16px;flex-wrap:wrap;padding-bottom:16px;border-bottom:1px solid var(--yg-line)}
.page-head h2{margin:0 0 6px;font-size:23px;letter-spacing:-.4px;font-weight:750}.page-head p{margin:0 0 0 16px;color:var(--yg-muted);font-size:12px}
/* v2.1.6 #245：.head-tools 此前定义两处（此处 gap:8px 与下方 gap:6px 8px），
   同特异度后者胜 → 本条的 gap 一直失效。合并为一处，只保留最终生效的取值。 */
.head-tools{display:flex;gap:6px 8px;align-items:center;flex-wrap:wrap;justify-content:flex-end}
.hero{border-radius:20px;background:linear-gradient(120deg,#4f4fc4 0%,#6b5ce0 52%,#3aa9c9 100%);color:#fff;padding:26px 30px;display:flex;justify-content:space-between;align-items:center;gap:20px;box-shadow:0 18px 40px rgba(91,91,214,.22);position:relative;overflow:hidden}
.hero:after{content:"";position:absolute;width:280px;height:280px;border-radius:50%;border:40px solid rgba(255,255,255,.07);right:-70px;top:-120px}
.hero-tag{font-size:10px;letter-spacing:2px;opacity:.75;margin-bottom:6px}.hero h2{margin:0 0 6px;font-size:26px}.hero p{margin:0;opacity:.82;font-size:12px}
.hero-actions{display:flex;align-items:center;gap:14px;z-index:1}.hero-pill{padding:6px 13px;border-radius:20px;font-size:12px;font-weight:600}.pill-on{background:rgba(57,212,176,.22);color:#b9f7e6}.pill-off{background:rgba(255,255,255,.18)}
.stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(168px,1fr));gap:15px;margin:20px 0}
.stat-card{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);padding:20px;box-shadow:var(--yg-shadow);position:relative;overflow:hidden;transition:.22s}
.stat-card:hover{transform:translateY(-4px);box-shadow:var(--yg-shadow-lg)}
.stat-icon{width:40px;height:40px;border-radius:12px;display:grid;place-items:center;margin-bottom:14px}
.stat-icon .ico{width:20px;height:20px}
.tone-indigo .stat-icon{background:var(--yg-tone-violet-bg);color:var(--yg-tone-violet-fg)}.tone-teal .stat-icon{background:var(--yg-tone-teal-bg);color:var(--yg-tone-teal-fg)}.tone-amber .stat-icon{background:var(--yg-tone-amber-bg);color:var(--yg-tone-amber-fg)}.tone-violet .stat-icon{background:var(--yg-tone-violet-bg);color:var(--yg-tone-violet-fg)}.tone-rose .stat-icon{background:var(--yg-tone-red-bg);color:var(--yg-tone-red-fg)}.tone-slate .stat-icon{background:var(--yg-tone-slate-bg);color:var(--yg-tone-slate-fg)}
.stat-num{font-size:30px;font-weight:780;line-height:1.1;font-variant-numeric:tabular-nums;letter-spacing:-.5px}.stat-label{color:var(--yg-muted);font-size:12px;margin-top:6px}
/* v0.9.8 #194 推送效果报表 */
.stats-cards{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:16px}
.stats-cards .stat-card{padding:14px}.stats-cards .stat-num{font-size:24px}
.chart-wrap{padding:6px 2px 0}
.chart-bars{display:flex;align-items:flex-end;gap:4px;height:150px;padding-bottom:20px;position:relative;border-bottom:1px solid var(--yg-line)}
.chart-col{flex:1;min-width:14px;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%}
.chart-stack{width:70%;max-width:26px;height:100%;display:flex;flex-direction:column;justify-content:flex-end}
.chart-stack i{display:block;width:100%;min-height:0}
.bar-sent{background:var(--yg-primary);border-radius:3px 3px 0 0}
.bar-fail{background:var(--yg-tone-red-fg);border-radius:3px 3px 0 0}
.chart-col small{font-size:10px;color:var(--yg-muted);margin-top:6px;white-space:nowrap;transform:rotate(-30deg);transform-origin:center}
.chart-legend{display:flex;gap:16px;justify-content:center;margin-top:14px;font-size:12px;color:var(--yg-muted)}
.chart-legend span{display:flex;align-items:center;gap:6px}.chart-legend i{width:10px;height:10px;border-radius:2px;display:inline-block}
.lg-sent{background:var(--yg-primary)}.lg-fail{background:var(--yg-tone-red-fg)}
/* v2.1.6 #245：.empty-hint 的旧定义已并入下方「空态统一」段，避免同选择器两处定义。 */
.unavailable-box{margin-top:16px;background:var(--yg-surface);border:1px dashed var(--yg-line);border-radius:var(--yg-radius-sm);padding:12px 14px}
.unavailable-box>b{font-size:12px;color:var(--yg-muted)}
.unavailable-row{display:flex;gap:12px;padding:6px 0;font-size:12px;border-top:1px solid var(--yg-line);margin-top:6px}
.unavailable-row span{font-weight:600;min-width:60px}.unavailable-row small{color:var(--yg-muted)}
/* v0.9.8 #195 镜像流程节点 + 进度 */
.sync-progress{display:flex;align-items:center;gap:12px;margin-bottom:16px;font-size:12px;color:var(--yg-muted)}
.sync-progress-track{flex:1;height:8px;background:var(--yg-line);border-radius:6px;overflow:hidden}
.sync-progress-track i{display:block;height:100%;background:var(--yg-primary);border-radius:6px;transition:width .3s}
.pipeline-flow{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}
.pipe-node{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);padding:12px;position:relative;overflow:hidden}
.pipe-node.is-done{border-color:#19b3a6}.pipe-node.is-active{border-color:#f2a33c}.pipe-node.is-idle{opacity:.6}
.pipe-node:before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--yg-line)}
.pipe-node.is-done:before{background:#19b3a6}.pipe-node.is-active:before{background:#f2a33c}
.pipe-label{font-size:12px;font-weight:600}
.pipe-value{font-size:22px;font-weight:780;margin-top:4px;font-variant-numeric:tabular-nums}
.pipe-value span{font-size:13px;color:var(--yg-muted);font-weight:400}
.pipe-detail{font-size:11px;color:var(--yg-muted);margin-top:6px;line-height:1.4}
.home-flow .pipe-node{padding:10px}.home-flow .pipe-value{font-size:18px}
/* 阶段六：任务中心分类聚合视图 */
.task-notice{background:var(--yg-panel-soft);border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);padding:10px 14px;font-size:12px;color:var(--yg-muted);margin-bottom:18px}
.task-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px}
.task-card{margin-bottom:0;padding:18px 18px 14px;border-left:3px solid var(--yg-line)}
.task-card.task-active{border-left-color:#f2a33c}.task-card.task-warn{border-left-color:#e2564f}.task-card.task-done{border-left-color:#19b3a6}
.task-card-head{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:14px}
.task-title{display:flex;align-items:center;gap:8px;font-size:14px}.task-title .ico{width:17px;height:17px;color:var(--yg-primary)}
.task-state{font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px;background:var(--yg-panel-soft);color:var(--yg-muted);white-space:nowrap}
.task-state.st-active{color:#c47a12;background:rgba(242,163,60,.14)}.task-state.st-warn{color:#d9463d;background:rgba(226,86,79,.14)}
.task-state.st-done{color:#0f9c90;background:rgba(25,179,166,.14)}
.task-metrics{display:flex;flex-wrap:wrap;gap:14px 18px;min-height:44px}
.task-metric{display:flex;flex-direction:column;gap:2px}.tm-value{font-size:21px;font-weight:780;font-variant-numeric:tabular-nums;line-height:1.1}
.tm-value.tm-good{color:#c47a12}.tm-value.tm-warn{color:#d9463d}.tm-value.tm-bad{color:#d9463d}
.tm-label{font-size:11px;color:var(--yg-muted)}
.task-card-foot{margin-top:14px;padding-top:10px;border-top:1px dashed var(--yg-line);display:flex;justify-content:space-between;align-items:flex-end;gap:8px}
.task-card-foot small{color:var(--yg-muted);font-size:10px;line-height:1.4;flex:1}.task-card-foot .link-btn{white-space:nowrap}
.stat-bar{height:4px;background:var(--yg-line);border-radius:4px;margin-top:16px;overflow:hidden}.stat-bar i{display:block;height:100%;width:42%;background:linear-gradient(90deg,var(--yg-primary),var(--yg-primary-2));border-radius:4px}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:18px}.three-col{display:grid;grid-template-columns:repeat(3,1fr);gap:18px}
/* v2.1.6 #245：主体容器统一到分享中心基准（.sc-card 口径）。
   此前全站有 5 套主体容器（.panel / .bot-card / .media-card / .method-card / 裸 div），
   圆角（20/16/14/11/10px）、内边距、阴影各写各的 → 同一屏里卡片体量参差。
   统一后：主体面板一律 var(--yg-radius) 圆角 + var(--yg-surface) 底 + var(--yg-shadow) 阴影，
   内边距统一 20px 22px（与 .sc-card 一致）。 */
.panel{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius);padding:20px 22px;box-shadow:var(--yg-shadow);margin-bottom:18px}
.panel.narrow{max-width:780px}.mirror-two-col{margin-bottom:18px}.mirror-col{max-width:none;min-width:0}.mirror-col .form-grid{grid-template-columns:1fr}
/* c/d 联动模块并排：每列内部表单单列、panel 不限宽；窄屏(<1100px)自动堆叠 */
.link-modules{display:grid;grid-template-columns:1fr 1fr;gap:18px;align-items:start;margin-top:22px}.link-module{min-width:0;display:flex;flex-direction:column}.link-module .sec-band{margin-top:0}.link-module .panel{max-width:none;flex:1}.link-module .form-grid{grid-template-columns:1fr;gap:14px}.link-module .panel-head{flex-wrap:wrap}.panel-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:18px;gap:12px}.panel-head h3{margin:0;font-size:16px;font-weight:720}.panel-head>span{font-size:11px;color:var(--yg-muted)}
.link-btn{border:0;background:transparent;color:var(--yg-primary);cursor:pointer;font-size:12px}
.check-row{display:flex;align-items:center;gap:12px;padding:11px 0;border-bottom:1px dashed var(--yg-line)}.check-row:last-child{border:0}
.check-dot{width:24px;height:24px;border-radius:8px;display:grid;place-items:center}.check-dot.ok{background:var(--yg-tone-teal-bg);color:var(--yg-tone-teal-fg)}.check-dot.no{background:var(--yg-tone-amber-bg);color:var(--yg-tone-amber-fg)}.check-dot .ico{width:14px;height:14px}
.check-copy{flex:1}.check-copy b{display:block;font-size:13px}.check-copy small{color:var(--yg-muted);font-size:11px}
.check-state{font-size:11px;font-weight:600}.check-state.ok{color:#19b3a6}.check-state.no{color:#e29328}
.dist-row{display:flex;align-items:center;gap:10px;padding:8px 0}.dist-label{width:56px;font-size:12px;color:var(--yg-muted)}.dist-track{flex:1;height:8px;background:var(--yg-line);border-radius:8px;overflow:hidden}.dist-track i{display:block;height:100%;border-radius:8px;transition:width .3s}.dist-row b{width:24px;text-align:right;font-variant-numeric:tabular-nums}
.timeline .tl-row{display:flex;align-items:center;gap:12px;padding:9px 0;border-bottom:1px dashed var(--yg-line);font-size:12px}.timeline .tl-row:last-child{border:0}
.tl-dot{width:7px;height:7px;border-radius:50%;background:var(--yg-primary-2);flex:none}.tl-time{color:var(--yg-muted);font-variant-numeric:tabular-nums;min-width:70px}.tl-action{color:var(--yg-primary);min-width:150px}.tl-detail{color:var(--yg-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.tl-target{color:#8a93aa}
/* v2.1.6 #245：空态统一。此前全站有 4 种写法（.empty / .empty.big / .empty-hint / el-empty），
   字号与留白各不相同（padding 24 / 28 / 60px）→ 同一屏里空态体量参差。
   现在统一为一套：.empty 基础态 + .empty.big 强调态（仅放大留白，字号同基础态），
   .empty-hint 保留类名但复用基础态口径（历史上用于图表区提示）。 */
.empty{text-align:center;color:var(--yg-muted);padding:24px;font-size:13px}
.empty.big{padding:60px}
.empty-hint{padding:28px;text-align:center;color:var(--yg-muted);font-size:13px}
.sub-head{margin:4px 0 12px;padding-left:10px;border-left:3px solid var(--yg-primary);color:var(--yg-muted);font-size:12px}
.connect-hero{background:linear-gradient(135deg,var(--yg-tone-blue-bg),var(--yg-surface));border:1px solid var(--yg-line);margin-bottom:18px}
.connect-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
.connect-head h3{margin:0 0 4px;font-size:17px;color:#1b2b4b}
.connect-head p{margin:0;color:var(--yg-muted);font-size:12px}
.connect-steps{display:flex;gap:8px;margin:18px 0}
.cstep{flex:1;display:flex;align-items:center;gap:9px;padding:10px 12px;border-radius:var(--yg-radius-sm);background:var(--yg-surface);border:1px solid var(--yg-line);color:var(--yg-muted);font-size:12px}
.cstep b{width:22px;height:22px;border-radius:50%;background:var(--yg-tone-slate-bg);color:var(--yg-tone-slate-fg);display:flex;align-items:center;justify-content:center;font-size:12px;flex:none}
.cstep.active{border-color:#4f7cff;color:#2a5bd7}.cstep.active b{background:#4f7cff;color:#fff}
.cstep.done{border-color:#19b3a6;color:#12897f}.cstep.done b{background:#19b3a6;color:#fff}
.connect-form{display:flex;flex-direction:column;gap:10px;max-width:560px}
.connect-actions{display:flex;gap:10px}
.connect-done .el-result{padding:6px 0}
.connect-foot{display:flex;gap:20px;margin-top:14px;font-size:12px;color:var(--yg-muted);flex-wrap:wrap;align-items:center}
.credential-bar{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px;padding:16px 18px;border-radius:var(--yg-radius-sm);background:linear-gradient(135deg,var(--yg-tone-teal-bg),var(--yg-surface));border:1px solid var(--yg-line)}
.cred-cell small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:7px}
.cred-cell b{font-size:13px;word-break:break-all}
.credential-bar .good{color:#128f84}.credential-bar .bad{color:var(--yg-rose)}.credential-bar .warn{color:#d8861a}
.dot.live{background:#19b3a6;box-shadow:0 0 0 4px rgba(25,179,166,.15);margin-right:6px;animation:pulseDot 1.8s ease-in-out infinite}
@keyframes pulseDot{0%,100%{opacity:1}50%{opacity:.45}}
.auth-summary{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}.auth-summary-item{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);padding:15px 18px;box-shadow:var(--yg-shadow)}.auth-summary-item small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:6px}.auth-summary-item b{font-size:14px;word-break:break-all}.auth-summary-item .good{color:var(--yg-tone-green-fg)}.auth-summary-item .bad{color:var(--yg-rose)}
.method-card{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius);padding:20px 22px;box-shadow:var(--yg-shadow);margin-bottom:18px}
.method-top{display:flex;gap:14px;align-items:center;margin-bottom:16px}.method-top h3{margin:0 0 4px;font-size:16px}.method-top p{margin:0;color:var(--yg-muted);font-size:12px}
.method-icon{width:42px;height:42px;border-radius:12px;display:grid;place-items:center;flex:none}.method-icon.indigo{background:var(--yg-tone-violet-bg);color:var(--yg-tone-violet-fg)}.method-icon.teal{background:var(--yg-tone-teal-bg);color:var(--yg-tone-teal-fg)}.method-icon .ico{width:21px;height:21px}
.steps{margin:0 0 18px;padding-left:20px;color:#5b6484;font-size:12px;line-height:2}.steps li::marker{color:var(--yg-primary)}
.method-foot{display:flex;justify-content:space-between;align-items:center;border-top:1px dashed var(--yg-line);padding-top:16px}
.device-panel{border-color:var(--yg-line);background:linear-gradient(180deg,var(--yg-tone-violet-bg),var(--yg-surface))}
.pulse{color:var(--yg-primary);animation:blink 1.4s infinite}@keyframes blink{50%{opacity:.35}}
.device-code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:30px;letter-spacing:6px;text-align:center;background:var(--yg-tone-violet-bg);border:1px dashed var(--yg-line);border-radius:12px;padding:18px;margin-bottom:16px;color:var(--yg-tone-violet-fg)}
.webqr-box{display:flex;justify-content:center;padding:14px;background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:12px;margin-bottom:14px}.webqr-box img{width:240px;height:240px;image-rendering:pixelated}
.webqr-code{text-align:center;font-size:14px;color:var(--yg-muted);margin-bottom:14px}.webqr-code b{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:18px;letter-spacing:2px;color:var(--yg-tone-violet-fg)}
.device-actions{display:flex;gap:12px;justify-content:center;align-items:center}.device-link{color:var(--yg-primary);text-decoration:none;font-size:13px;display:flex;align-items:center;gap:5px}
.kv-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.kv-grid small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:5px}.kv-grid b{font-size:13px;word-break:break-all}
.hint{color:var(--yg-muted);font-size:11px;line-height:1.8;margin:12px 0 0}
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.field label{display:block;font-size:12px;font-weight:600;margin-bottom:7px}.field small{display:block;color:var(--yg-muted);font-size:11px;margin-top:6px}.field.inline{display:flex;align-items:center;gap:14px}.field.inline label{margin:0}
.panel-foot{display:flex;gap:10px;margin-top:22px;padding-top:18px;border-top:1px dashed var(--yg-line);flex-wrap:wrap}
/* v0.9.6：镜像锁占用提示——后台改名复查静默持锁时，让用户知道为何「立即同步」会 409 */
.mirror-lock-tip{display:flex;align-items:center;gap:10px;margin-top:16px;padding:10px 14px;background:var(--yg-tone-amber-bg);border:1px solid var(--yg-line);border-radius:10px;color:var(--yg-tone-amber-fg);font-size:12px;line-height:1.7}
.batch-control-row{display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin-top:14px;padding:10px 14px;background:var(--yg-tone-blue-bg);border:1px solid var(--yg-line);border-radius:10px;font-size:12px}
.batch-control-row .hint{color:var(--yg-muted)}
/* v0.9.0：镜像同步页分区治理——状态数字按语义分组，配置/接口改成结构化 chip */
.mirror-fold{margin-top:16px;border-top:1px solid var(--yg-line);border-bottom:0}
.mirror-fold .el-collapse-item__header{font-size:13px;font-weight:650;color:#3b4361;min-height:42px;height:auto;padding:10px 4px}
.mirror-fold .el-collapse-item__content{padding-bottom:10px}
.fold-title{font-size:13px;font-weight:650;color:#3b4361;display:inline-flex;align-items:center;gap:8px}
.fold-title::before{content:"";width:18px;height:18px;border-radius:6px;flex:none;background:linear-gradient(135deg,#8271ef,#5b5bd6);background-image:radial-gradient(circle at 50% 50%,#fff 0 2px,transparent 2.4px)}
.fold-sub{margin-left:8px;font-size:11px;color:var(--yg-muted);font-weight:400}
/* 状态分组：标题 + 网格，一组一类，不再一屏 15 个数字 */
.stat-group{margin-bottom:16px}.stat-group:last-child{margin-bottom:0}
.stat-group-title{display:flex;align-items:center;gap:8px;font-size:12px;font-weight:700;color:var(--yg-muted);margin:0 0 10px}
.stat-group-title .grp-ico{width:22px;height:22px;border-radius:7px;display:grid;place-items:center;font-size:11px;background:var(--yg-tone-blue-bg);color:var(--yg-tone-blue-fg);flex:none}
.stat-group-title .grp-ico.g-teal{background:var(--yg-tone-teal-bg);color:var(--yg-tone-teal-fg)}
.stat-group-title .grp-ico.g-amber{background:var(--yg-tone-amber-bg);color:var(--yg-tone-amber-fg)}
.stat-group-title .grp-ico.g-violet{background:var(--yg-tone-violet-bg);color:var(--yg-tone-violet-fg)}
.mini-stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:10px}
.mini-stat{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);padding:11px 13px;box-shadow:var(--yg-shadow);transition:.16s}
.mini-stat:hover{border-color:var(--yg-primary-2);box-shadow:var(--yg-shadow-lg)}
.mini-stat small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:6px;line-height:1.3}
.mini-stat b{font-size:18px;font-variant-numeric:tabular-nums;font-weight:750}
/* 生效配置 / 接口清单：结构化 chip 行，替代长串小字 */
.info-chips{display:flex;flex-wrap:wrap;gap:8px 10px;margin:2px 0 0}
.info-chip{display:inline-flex;align-items:center;gap:6px;background:var(--yg-bg);border:1px solid var(--yg-line);border-radius:9px;padding:6px 11px;font-size:12px;color:var(--yg-muted)}
.info-chip b{font-weight:700;color:var(--yg-ink)}
.info-chip.on b{color:var(--yg-tone-green-fg)}.info-chip.off b{color:var(--yg-rose)}
.endpoint-list{display:flex;flex-wrap:wrap;gap:6px;margin:6px 0 0}
.endpoint-list code{font-size:11px;background:var(--yg-tone-blue-bg);color:var(--yg-tone-blue-fg);border:1px solid var(--yg-line);border-radius:6px;padding:3px 8px;word-break:break-all}
/* v0.9.3：分发与互动中心三板块标题（a / b / c） */
.sec-band{display:flex;align-items:center;gap:12px;margin:22px 0 14px}
.sec-band .sec-key{display:inline-grid;place-items:center;width:30px;height:30px;border-radius:9px;background:linear-gradient(135deg,#5b5bd6,#3b3b8f);color:#fff;font-weight:800;font-size:15px;flex:none}
.sec-band h3{margin:0;font-size:16px;font-weight:750;color:var(--yg-ink)}
.sec-band p{margin:2px 0 0;font-size:11px;color:var(--yg-muted)}
.sec-band .sec-line{flex:1;height:1px;background:var(--yg-line)}
/* v0.9.0：Bot 交互中心卡片（参考 embyhub）——v2.1.6 #245 已统一到 .panel 口径 */
.bot-card{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius);padding:20px 22px;margin-bottom:16px;box-shadow:var(--yg-shadow)}
.bot-card-head{display:flex;align-items:center;gap:12px;margin-bottom:18px}
.bot-card-head h3{font-size:16px;margin:0;font-weight:700}
.bot-ico{display:inline-flex;align-items:center;justify-content:center;width:38px;height:38px;border-radius:10px;background:var(--yg-tone-blue-bg);color:var(--yg-tone-blue-fg);font-size:16px;font-weight:700}
.restrict-sub{font-size:12px;color:#929bb5}
.cmd-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.cmd-item{display:flex;align-items:center;gap:12px;background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);padding:12px 14px;box-shadow:var(--yg-shadow)}
.cmd-chip{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:13px;color:var(--yg-tone-blue-fg);background:var(--yg-tone-blue-bg);border:1px solid var(--yg-line);border-radius:7px;padding:4px 10px;white-space:nowrap}
.cmd-desc{font-size:12px;color:#5a6178;flex:1}
.cmd-add{display:flex;gap:10px;flex-wrap:wrap;margin-top:16px;padding-top:16px;border-top:1px dashed var(--yg-line)}
.restrict-card{background:linear-gradient(180deg,var(--yg-tone-green-bg),var(--yg-surface));border-color:var(--yg-line)}
.restrict-card .bot-ico{background:var(--yg-tone-green-bg);color:var(--yg-tone-green-fg)}
.restrict-row{display:flex;align-items:center;gap:14px;padding:12px 0;border-bottom:1px dashed var(--yg-line)}
.restrict-row.col{flex-direction:column;align-items:stretch;gap:8px}
.restrict-row:last-of-type{border-bottom:0}
.restrict-label b{display:block;font-size:12px;color:#2f6b45;margin-bottom:4px}
.restrict-label small{display:block;font-size:11px;color:#6f9279;line-height:1.7}
.restrict-row .unit{font-size:12px;color:#2f6b45}
.hint.warn{color:#b8860b}
.cursor-box{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}.cursor-box small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:5px}.cursor-box b{font-size:18px}
.view-switch{display:flex;border:1px solid var(--yg-line);border-radius:9px;overflow:hidden}.view-switch button{border:0;background:var(--yg-surface);padding:7px 10px;cursor:pointer;color:var(--yg-muted)}.view-switch button.on{background:var(--yg-tone-violet-bg);color:var(--yg-primary)}
.media-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(172px,1fr));gap:14px}
/* v0.6.4：参考 STRM 刮削海报墙——紧凑竖版卡片、统一 2:3 海报、状态角标 */
.series-card{border-radius:11px}.series-card .media-poster{height:auto;aspect-ratio:2/3;background:var(--yg-bg)}.series-card .media-body{padding:9px 10px}.series-card .media-body b{font-size:12px}.series-card .media-body small{font-size:10px;color:var(--yg-muted);margin-top:3px}
.series-card .media-meta{flex-wrap:wrap;gap:4px 8px;margin-top:6px}.series-card .media-actions{padding:0 9px 10px}.series-card .media-actions .el-button{margin:0 2px 4px 0;padding:5px 8px;font-size:11px}
.series-card .media-badge{border-radius:6px;padding:3px 7px;font-size:9px}
/* v2.1.6 #245：原先这里还有一条独立的 .scrape-ok 与 .ep-count 定义，
   被下方 .series-card .media-badge 那一行里的同名定义完全覆盖（同特异度后者胜）
   → 死代码。清除，避免「同选择器多处定义」的隐性坑。 */
.schedule-controls{align-items:center;gap:8px}.schedule-label{color:var(--yg-muted);font-size:11px;white-space:nowrap}
.media-card{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);overflow:hidden;box-shadow:var(--yg-shadow);transition:.2s}.media-card:hover{transform:translateY(-3px);box-shadow:var(--yg-shadow-lg)}
.media-poster{height:270px;position:relative;background:var(--yg-bg)}.no-poster{height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:8px;color:var(--yg-muted)}.no-poster .ico{width:30px;height:30px}.no-poster span{font-size:11px;letter-spacing:1px}
.media-badge{position:absolute;top:10px;left:10px;padding:4px 9px;border-radius:20px;font-size:10px;font-weight:700;color:#fff;background:#98a1b8}.badge-active{background:#19b3a6}.badge-not_created{background:#e29328}.badge-failed{background:#e5526b}.badge-cancelled{background:#98a1b8}
.series-card .media-badge{left:auto;right:10px}.scrape-ok{position:absolute;top:10px;left:10px;width:22px;height:22px;border-radius:50%;background:#19b3a6;color:#fff;font-size:13px;font-weight:800;display:grid;place-items:center;box-shadow:0 2px 8px rgba(0,0,0,.2)}.ep-count{position:absolute;bottom:8px;right:8px;padding:3px 8px;border-radius:12px;background:rgba(20,24,40,.78);color:#fff;font-size:10px;font-weight:700}
/* 阶段六资源卡角标：推送 / 链接有效性（本地推断） */
.push-badge{position:absolute;top:34px;right:10px;padding:2px 7px;border-radius:9px;font-size:9px;font-weight:700;color:#fff;background:#4b7bec;z-index:4}
.link-badge{position:absolute;bottom:26px;right:8px;padding:2px 7px;border-radius:9px;font-size:9px;font-weight:700;color:#fff;background:#e5526b;z-index:4}
/* 分享中心：左侧子导航 + 仪表盘工作台 */
.sc-shell{display:flex;flex-direction:column;gap:16px}
.sc-tabbar{display:flex;gap:8px;background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);padding:6px;box-shadow:var(--yg-shadow);overflow-x:auto}
.sc-tab{border:0;background:transparent;white-space:nowrap;padding:11px 20px;border-radius:10px;display:flex;align-items:center;gap:9px;color:var(--yg-muted);font-size:13px;font-weight:700;cursor:pointer;transition:.18s}
.sc-tab:hover{background:var(--yg-bg);color:var(--yg-ink)}
.sc-tab.active{background:linear-gradient(135deg,var(--yg-tone-blue-bg),var(--yg-tone-violet-bg));color:var(--yg-primary);box-shadow:0 2px 8px rgba(91,91,214,.12)}
.sc-tab-ico{font-size:15px}
.sc-workspace{min-width:0}
/* v2.1.5：分享中心「非总览」子页的满宽容器。
   总览页用 .sc-dash（flex column）直接挂在 .sc-workspace 下 → 天然铺满；
   其余三页（发布规则 / 推送任务 / 分享管理）此前挂在 .sc-board（两列 Grid）里，
   叠加 .sc-board-full 的层叠顺序 bug，只占半幅 → 真机呈「缩小版」。
   现在三页与总览页平级，由本容器独占整行满宽。 */
.sc-subpage{display:block;width:100%;min-width:0}
/* v2.1.1：已按设计稿整块删除的汇总面板，其死样式一并清除（避免留下无人引用的 CSS）。 */
/* 小时分类排期 */
.sc-hour-list{display:flex;flex-direction:column;gap:7px}
.sc-hour-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:7px 10px;border:1px solid var(--yg-line);border-radius:10px;background:var(--yg-bg)}
.sc-hour-time{width:104px;flex:none;font-size:11px;font-weight:750;color:var(--yg-ink);font-variant-numeric:tabular-nums}
.sc-topbar{display:flex;justify-content:space-between;align-items:flex-end;gap:16px;flex-wrap:wrap;margin-bottom:18px;padding-bottom:16px;border-bottom:1px solid var(--yg-line)}
.sc-topbar h2{margin:0 0 5px;font-size:23px;letter-spacing:-.4px;font-weight:750}
.sc-topbar p{margin:0;color:var(--yg-muted);font-size:12px}
.sc-run-badge{display:inline-flex;align-items:center;gap:7px;padding:6px 12px;border-radius:20px;background:var(--yg-tone-green-bg);color:var(--yg-tone-green-fg);font-size:12px;font-weight:700}
.sc-run-badge i{width:7px;height:7px;border-radius:50%;background:#19b98a}
.sc-run-badge.paused{background:var(--yg-tone-red-bg);color:var(--yg-tone-red-fg)}
.sc-run-badge.paused i{background:#e5526b}
.sc-dash{display:flex;flex-direction:column;gap:18px}
.sc-kpi-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}
.sc-kpi{border:1px solid var(--yg-line);background:var(--yg-surface);border-radius:var(--yg-radius-sm);padding:18px;display:flex;align-items:center;gap:14px;cursor:pointer;text-align:left;box-shadow:var(--yg-shadow);transition:.2s}
.sc-kpi:hover{transform:translateY(-3px);box-shadow:var(--yg-shadow-lg)}
.sc-kpi-ico{width:44px;height:44px;border-radius:13px;display:grid;place-items:center;font-size:20px;font-weight:800;flex:none}
.sc-kpi-blue .sc-kpi-ico{background:var(--yg-tone-blue-bg);color:var(--yg-tone-blue-fg)}
.sc-kpi-green .sc-kpi-ico{background:var(--yg-tone-green-bg);color:var(--yg-tone-green-fg)}
.sc-kpi-amber .sc-kpi-ico{background:var(--yg-tone-amber-bg);color:var(--yg-tone-amber-fg)}
.sc-kpi-rose .sc-kpi-ico{background:var(--yg-tone-red-bg);color:var(--yg-tone-red-fg)}
.sc-kpi-body{flex:1;min-width:0}
.sc-kpi-body small{display:block;color:var(--yg-muted);font-size:12px;margin-bottom:5px}
.sc-kpi-body b{display:block;font-size:27px;font-weight:790;line-height:1;font-variant-numeric:tabular-nums}
.sc-kpi-arrow{color:var(--yg-muted);font-size:22px}
/* v2.1.1：已删面板的 sc-sched-* 样式随之清除（sc-progress 仍被其它进度条复用，保留）。 */
.sc-progress{height:7px;border-radius:6px;background:var(--yg-line);overflow:hidden}
.sc-progress i{display:block;height:100%;border-radius:6px;background:linear-gradient(90deg,var(--yg-primary-2),var(--yg-primary))}
.sc-dash-two{display:grid;grid-template-columns:minmax(0,1.7fr) minmax(0,1fr);gap:18px;align-items:start}
.sc-link-btn{border:0;background:transparent;color:var(--yg-primary);font-size:12px;font-weight:700;cursor:pointer;padding:4px 6px}
.sc-link-btn:hover{text-decoration:underline}
.sc-mini-tag{background:var(--yg-tone-blue-bg);color:var(--yg-primary);border-radius:7px;padding:2px 7px;font-size:10px;font-weight:700}
.sc-dot-state{font-size:10px;font-weight:700;padding:2px 6px;border-radius:7px;background:var(--yg-bg);color:var(--yg-muted)}
.sc-dot-state.published{background:var(--yg-tone-green-bg);color:var(--yg-tone-green-fg)}.sc-dot-state.failed{background:var(--yg-tone-red-bg);color:var(--yg-tone-red-fg)}
/* v2.1.4：链接状态视觉态四档（后端 link_tone 下发：ok / muted / warn / bad）。
   此前只有 published / failed 两档，链接状态全部落回灰底灰字，
   「有效」和「疑似失效」在页面上看不出区别。 */
.sc-dot-state.ok{background:var(--yg-tone-green-bg);color:var(--yg-tone-green-fg)}
.sc-dot-state.muted{background:var(--yg-bg);color:var(--yg-muted)}
.sc-dot-state.warn{background:var(--yg-tone-amber-bg);color:var(--yg-tone-amber-fg)}
.sc-dot-state.bad{background:var(--yg-tone-red-bg);color:var(--yg-tone-red-fg)}
.sc-health-grid{display:grid;grid-template-columns:1fr 1fr;gap:11px}
.sc-health{border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);padding:14px;text-align:left;cursor:pointer;position:relative;transition:.2s;background:var(--yg-surface)}
.sc-health:hover{transform:translateY(-2px);box-shadow:var(--yg-shadow-lg)}
.sc-health span{position:absolute;top:12px;right:13px;width:22px;height:22px;border-radius:7px;display:grid;place-items:center;font-size:12px;font-weight:800;color:#fff}
.sc-health small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:7px}
.sc-health b{display:block;font-size:24px;font-weight:780;font-variant-numeric:tabular-nums}
.sc-health-green span{background:#19a97c}.sc-health-amber span{background:#e08a1e}.sc-health-rose span{background:#e5526b}.sc-health-gray span{background:#8992ac}
.sc-source-note{margin:12px 0 0;font-size:10px;line-height:1.6}
.sc-detail-kv{display:grid;grid-template-columns:1fr 1fr;gap:12px 20px;margin-bottom:18px}
.sc-detail-kv>div{background:var(--yg-bg);border-radius:10px;padding:10px 12px}
.sc-detail-kv span{display:block;color:var(--yg-muted);font-size:10px;margin-bottom:5px}
.sc-detail-kv b{display:block;font-size:12px;font-weight:700}
/* v2.0.5 设计稿：发布总览 */
.sc-kpi-six{grid-template-columns:repeat(6,minmax(0,1fr))}
/* v2.1.6：KPI 卡与基础 .sc-kpi 对齐（此前六卡被压到 padding:14px / ico 38px / 数字 21px，
   与页面其它卡片的视觉体量不一致 → 「尺寸与页面不符」）。
   保留略紧的排版是为了六列并排时不换行，但不再做「缩小版」处理。 */
.sc-kpi-six .sc-kpi{padding:16px;gap:11px;min-height:76px}
.sc-kpi-six .sc-kpi-ico{width:40px;height:40px;font-size:18px}
.sc-kpi-six .sc-kpi-body b{font-size:24px}
.sc-kpi-violet .sc-kpi-ico{background:var(--yg-tone-violet-bg);color:var(--yg-tone-violet-fg)}.sc-kpi-teal .sc-kpi-ico{background:var(--yg-tone-teal-bg);color:var(--yg-tone-teal-fg)}
.sc-link-source-note{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:-4px 0 16px;padding:9px 13px;background:var(--yg-surface);border:1px dashed var(--yg-line);border-radius:10px;font-size:11px;color:var(--yg-muted)}
.sc-link-source-note b{color:var(--yg-primary)}
.sc-link-source-note .sc-ico{color:var(--yg-primary);font-style:normal}
.sc-link-mini{margin-left:auto;font-weight:600;color:var(--yg-primary)}
/* v2.1.6：发布总览卡片尺寸对齐。
   此前 .sc-dash-three 显式 align-items:start → 同一行三张卡各自按内容高度收缩，
   上下边缘参差不齐（趋势图 160px / 占比图 130px / 热门候选随行数变）。
   修法：① 同行卡片改 stretch 等高；② 卡片改纵向 flex、内容区撑满并对齐底部。 */
.sc-dash-three{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:18px;align-items:stretch;margin-bottom:18px}
.sc-dash-three>.sc-card{display:flex;flex-direction:column}
.sc-dash-three>.sc-card>.sc-card-head{flex:none}
/* 趋势图：撑满卡片剩余高度，绘图区随卡片一起等高 */
.sc-dash-three .sc-trend-wrap{flex:1;display:flex;flex-direction:column;justify-content:center}
.sc-dash-three .sc-trend-svg{flex:1;height:auto;min-height:150px}
.sc-dash-three .sc-trend-labels{flex:none}
/* 占比图：撑满并垂直居中 */
.sc-dash-three .sc-ratio-wrap{flex:1;min-height:150px}
/* 热门候选：列表撑满，「查看候选详情」贴卡片底部（与三表行的底部对齐） */
.sc-dash-three .sc-hot-list{flex:1;justify-content:center}
.sc-dash-three .sc-hot-more{margin-top:auto;padding-top:12px}
/* 三表行：卡片等高（stretch 已保证），表格保持自身 max-height，
   不给 el-table 加 flex:1 —— el-table 依赖显式 height 才撑内部滚动区，
   强行 flex:1 会让短表格塌陷。空态提示占满剩余高度，与表格底部对齐。 */
.sc-three-tables>.sc-card>.empty{flex:1;display:grid;place-items:center}
.sc-seg{display:flex;gap:3px;background:var(--yg-bg);border-radius:8px;padding:3px}
.sc-seg button{border:0;background:transparent;color:var(--yg-muted);font-size:11px;padding:4px 10px;border-radius:6px;cursor:pointer}
.sc-seg button.on{background:var(--yg-primary);color:#fff}
.sc-trend-wrap{padding:6px 2px 2px}
.sc-trend-svg{width:100%;height:160px;display:block}
.sc-trend-labels{display:flex;justify-content:space-between;color:var(--yg-muted);font-size:9px;margin-top:4px}
.sc-ratio-wrap{display:flex;align-items:center;gap:14px;min-height:160px}
.sc-donut{width:130px;height:130px;flex:none}
.sc-ratio-legend{flex:1;display:flex;flex-direction:column;gap:6px;font-size:11px}
.sc-ratio-legend>div{display:flex;align-items:center;gap:6px}
.sc-ratio-legend i{width:9px;height:9px;border-radius:3px;flex:none}
.sc-ratio-legend span{flex:1;color:var(--yg-muted)}
.sc-ratio-legend b{font-variant-numeric:tabular-nums}
.sc-hot-list{display:flex;flex-direction:column;gap:7px}
.sc-hot-row{display:flex;align-items:center;gap:9px;font-size:12px}
.sc-hot-rank{width:20px;height:20px;border-radius:6px;background:var(--yg-bg);color:var(--yg-muted);display:grid;place-items:center;font-size:10px;font-weight:800;flex:none}
.sc-hot-rank.top{background:#6d5efc;color:#fff}
.sc-hot-title{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600}
.sc-hot-score{color:var(--yg-primary);font-variant-numeric:tabular-nums;font-weight:700}
/* v2.1.6：底部按钮改为撑满卡片底部（margin-top:auto 由 .sc-dash-three 段提供） */
.sc-hot-more{display:block;text-align:center;width:100%}
.sc-three-tables .sc-card-head p{white-space:normal}
/* v2.0.5 设计稿：发布规则 */
.sc-subtabbar{display:flex;gap:6px;margin-bottom:14px;border-bottom:1px solid var(--yg-line)}
.sc-subtab{border:0;background:transparent;padding:9px 18px;font-size:13px;color:var(--yg-muted);cursor:pointer;border-bottom:2px solid transparent;font-weight:600}
.sc-subtab.on{color:var(--yg-primary);border-bottom-color:var(--yg-primary)}
.sc-subtab-sm .sc-subtab{padding:6px 14px;font-size:11px}
.sc-cycle-grid{display:flex;flex-wrap:wrap;gap:16px 22px;align-items:flex-end;margin-top:12px}
.sc-cycle-grid .hint{font-size:11px}
.sc-time-cell{display:flex;align-items:center;gap:3px}
.sc-time-cell .el-input-number{width:64px}
/* v2.1.5：发布规则页满宽容器 + 配置时段下拉 + 周维度建议网格。
   真机问题（v2.1.4 回归）：PC 端三页（发布规则 / 推送任务 / 分享管理）
   均呈「缩小版」，尺寸与页面不符、比例失调。
   根因有两层：
   ① 满宽类 .sc-board-full 定义在 .sc-board **之前**——两者同特异度（单类选择器），
      后者胜出 → 满宽规则完全失效，页面仍被塞进两列 Grid 的半格子里；
   ② 发布规则页在半幅容器内又套一层两列 .sc-rule-layout → 每列只剩 1/4 页宽，
      卡片被二次压扁。
   v2.1.5 修法：三页**移出 Grid**，由 .sc-subpage 独占整行满宽（与总览页 .sc-dash 平级）；
   页内需要并排时由各页自己的内部容器负责。 */
/* 发布规则页内部：v2.1.6 改为**竖向上下对齐**——
   「上新即时推送」与「周期计划管理」两张卡片上下依次排布（单列），
   分类排期表独占整行。此前是 2 列 Grid 并排，两卡宽度被压到半格、
   高度不齐，视觉上「不对齐」。 */
.sc-rule-layout{display:grid;grid-template-columns:minmax(0,1fr);gap:18px;align-items:stretch}
.sc-rule-layout .sc-rule-full{grid-column:1/-1}
/* 配置时段下拉：小时级选择，静默区间内的小时不可选 */
.sc-hour-select{width:82px}
.sc-time-dash{color:var(--yg-muted);font-size:11px}
/* 与静默保护时段重叠的提示 */
.sc-cell-warn{margin-top:3px;font-size:10px;color:#e5526b;line-height:1.3}
/* v2.1.6：推送渠道单元格（频道/群 各一行，格式「群:名称（群id或用户名）」） */
.sc-target-cell{display:flex;flex-direction:column;gap:3px;align-items:flex-start}
/* v2.1.6：推荐时间段单元格（单一时间段 + 来源标签 + 偏好排名） */
.sc-suggest-cell{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.sc-suggest-time{font-size:12px;font-weight:700;font-variant-numeric:tabular-nums;white-space:nowrap}
.sc-suggest-rank{margin-top:3px;font-size:10px;color:var(--yg-muted)}
.sc-mini-tag.muted{background:var(--yg-bg);color:var(--yg-muted)}
/* v2.1.4：推送任务页 TMDB 海报缩略图 */
.sc-media-cell{display:flex;align-items:center;gap:9px;min-width:0}
.sc-media-thumb{width:38px;height:54px;border-radius:6px;overflow:hidden;flex:none;background:var(--yg-bg);display:grid;place-items:center}
.sc-media-thumb .el-image{width:100%;height:100%}
.sc-media-text{min-width:0}
.sc-media-text b{display:block;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:650}
.sc-media-text small{display:block;color:var(--yg-muted);font-size:10px;margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sc-thumb-fallback{width:100%;height:100%;display:grid;place-items:center;font-size:9px;color:var(--yg-muted);text-align:center;line-height:1.2}
/* 任务详情抽屉顶部的海报 */
.sc-detail-poster{width:120px;height:172px;border-radius:12px;overflow:hidden;margin-bottom:14px;background:var(--yg-bg);display:grid;place-items:center;box-shadow:var(--yg-shadow)}
.sc-detail-poster .el-image{width:100%;height:100%}
/* v2.1.4：分享详情弹窗顶部的海报（比抽屉版略窄，适配 560px 弹窗） */
.sc-share-poster{width:96px;height:138px;border-radius:10px;overflow:hidden;margin-bottom:14px;background:var(--yg-bg);display:grid;place-items:center;box-shadow:var(--yg-shadow)}
.sc-share-poster .el-image{width:100%;height:100%}
/* v2.1.1：分享管理 / 推送任务 左右布局。
   真机问题：侧栏固定 300px 时，表格列总宽超出左栏可用宽度，
   「分享记录」列被详情面板遮挡、时间列被截断。
   修法：① 侧栏改弹性宽度（clamp 260–360px），左侧留 minmax(0,1fr)；
        ② 表格容器允许横向滚动（列不再被挤扁），窄屏时侧栏落到下方。 */
/* v2.1.2：详情改为隐藏式弹窗后，两页不再需要常驻侧栏——
   单栏满宽，表格列不再被挤扁（配合 .sc-table-scroll 横向滚动）。 */
.sc-share-layout,.sc-task-layout{display:block}
/* v2.1.7 #241：阅读量统计页同样单栏满宽。
   **独立成条，不并入上面那条 v2.1.2 规则**——那条选择器串是 v2.1.2/v2.1.5
   三版回归断言的字面量锚点，往里加类名会让历史守卫整体失配。
   拆开写两边语义各自独立，断言锚点也保持原样。 */
.sc-stats-layout{display:block}
.sc-share-main,.sc-task-main{min-width:0}
/* ---------- v2.1.7 #241：阅读量统计页 ---------- */
.sc-stats-layout .sc-card{margin-bottom:0}
.sc-stats-state{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:12px 0;padding:10px 14px;border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);background:var(--yg-panel-soft)}
.sc-state-dot{width:8px;height:8px;border-radius:50%;flex:none}
.sc-state-dot.on{background:#19a97c;box-shadow:0 0 0 3px rgba(25,169,124,.16)}
.sc-state-dot.off{background:#8992ac;box-shadow:0 0 0 3px rgba(137,146,172,.16)}
.sc-state-hint{color:var(--yg-muted);font-size:12px;margin-left:auto}
.sc-stats-config{border-top:1px solid var(--yg-line);padding-top:14px;margin-top:4px}
.sc-stats-config .unit{color:var(--yg-muted);font-size:12px;margin-left:6px}
.sc-stats-config small{display:block;color:var(--yg-muted);font-size:11px;margin-top:4px;line-height:1.5}
/* 表格横向滚动容器：列保持自身宽度，超出时整体滚动而不是把列压扁 */
.sc-table-scroll{overflow-x:auto;max-width:100%}
.sc-table-scroll .el-table{min-width:100%}
/* 分类排期表列较多，容器留足横向滚动空间并固定首尾列 */
.sc-sched-scroll .el-table{min-width:1080px}
.sc-share-stats{grid-template-columns:repeat(5,1fr);margin-bottom:14px}
.sc-task-stats{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin:12px 0;background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);padding:14px}
.sc-task-stats>div{text-align:center}
.sc-task-stats small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:6px}
.sc-task-stats b{font-size:22px;font-weight:790;font-variant-numeric:tabular-nums}
.sc-task-stats b.ok{color:#19a97c}.sc-task-stats b.bad{color:#e5526b}.sc-task-stats b.muted{color:#8992ac}
.el-table .sc-row-active{background:var(--yg-primary-2)!important;opacity:.6}
@media(max-width:1200px){.sc-kpi-grid{grid-template-columns:repeat(2,1fr)}.sc-dash-two{grid-template-columns:1fr}.sc-kpi-six{grid-template-columns:repeat(3,1fr)}.sc-dash-three{grid-template-columns:1fr}}
@media(max-width:900px){.sc-tab{padding:10px 12px;font-size:12px}}
/* 分享中心：全卡片化工作台 */
/* v2.1.5：.sc-board 不再是两列 Grid。
   它此前是 grid-template-columns:repeat(2,minmax(0,1fr))，而满宽修饰类
   .sc-board-full 又定义在它**之前**（同特异度 → 后者胜出）→ 满宽永远失效，
   子页被塞进半格子里，真机呈「缩小版」。
   现在：默认单列满宽；三页已移出 Grid，由 .sc-subpage 承载。 */
.sc-board{display:block;width:100%;min-width:0}
.sc-card{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius);padding:20px 22px;box-shadow:var(--yg-shadow);min-width:0;overflow:hidden;transition:.22s}
.sc-card:hover{box-shadow:var(--yg-shadow-lg)}
.sc-hero-card{background:linear-gradient(135deg,#4f4fc4 0%,#6b5ce0 50%,#3aa9c9 100%);color:#fff;border:0}
.sc-hero-card .sc-card-head h3,.sc-hero-card .sc-card-head p{color:#fff}
.sc-hero-card .sc-card-head p{opacity:.82}
.sc-hero-card .sc-ico{background:rgba(255,255,255,.2);color:#fff}
.sc-card-head{display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap}
.sc-card-head>div{flex:1;min-width:180px}
.sc-card-head h3{margin:0;font-size:15px;font-weight:750}
.sc-card-head p{margin:3px 0 0;color:var(--yg-muted);font-size:11px;line-height:1.5}
.sc-ico{width:34px;height:34px;border-radius:11px;background:var(--yg-tone-violet-bg);color:var(--yg-primary);display:grid;place-items:center;font-size:18px;font-weight:700;flex:none}
.sc-stat-row{display:grid;grid-template-columns:repeat(6,1fr);gap:10px}
.sc-stat{background:rgba(255,255,255,.14);border-radius:12px;padding:14px 8px;text-align:center}
.sc-stat b{display:block;font-size:23px;font-weight:790;line-height:1.1;font-variant-numeric:tabular-nums}
.sc-stat small{display:block;margin-top:5px;font-size:10px;opacity:.85}
.sc-status-line{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:14px;padding-top:12px;border-top:1px solid rgba(255,255,255,.22)}
.sc-status-tag{margin:0}
.sc-hero-card .hint{color:rgba(255,255,255,.9)}
.sc-subhead{margin:16px 0 6px;font-size:12px;font-weight:700;color:var(--yg-primary)}
.sc-inline-detail{margin-top:14px;padding:14px;border:1px dashed var(--yg-line);border-radius:12px;background:var(--yg-panel)}
.sc-inline-head{display:flex;gap:12px;align-items:center;margin-bottom:10px}
.sc-inline-head b{font-size:12px}
.sc-form-row{display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;margin-bottom:12px}
.sc-form-row .hint{margin:0;line-height:1.6}
.sc-form-row .hint input,.sc-form-row .hint .el-select{display:block}
/* v2.1.1：已删面板的 sc-pacing-now 样式随之清除。 */
@media(max-width:1100px){.sc-stat-row{grid-template-columns:repeat(3,1fr)}}
.media-body{padding:12px 13px}.media-body b{display:block;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.media-body small{display:block;color:var(--yg-muted);font-size:10px;margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.media-meta{display:flex;gap:10px;margin-top:8px;color:var(--yg-primary);font-size:10px}
.media-actions{display:flex;gap:5px;padding:0 12px 12px;flex-wrap:wrap}
.step-panel .step-head{display:flex;align-items:center;gap:10px;margin-bottom:16px}.step-head span{width:28px;height:28px;border-radius:9px;background:var(--yg-tone-violet-bg);color:var(--yg-primary);display:grid;place-items:center;font-size:12px;font-weight:800}.step-head h3{margin:0;font-size:14px}
/* v0.6.2：分发规则三卡片管理布局 */
.manage-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;align-items:start}.manage-card{margin-bottom:0;min-width:0}.manage-card .panel-head h3{font-size:15px}.manage-card .panel-head .hint{display:block;font-size:11px;color:var(--yg-muted);margin-top:3px}.manage-card .el-table{font-size:12px}.manage-card .el-table .el-button+.el-button{margin-left:2px}
.mini-row{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:9px 0;border-bottom:1px dashed var(--yg-line);font-size:11px}.mini-row:last-child{border:0}.mini-row b{font-size:12px;flex:none}.mini-row>span:not(.row-actions){color:var(--yg-muted);max-width:45%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.row-actions{display:flex;gap:4px;flex:none;max-width:none!important;overflow:visible!important}
.row-btn{border:1px solid var(--yg-line);background:var(--yg-surface);border-radius:6px;padding:3px 7px;font-size:10px;color:var(--yg-primary);cursor:pointer;white-space:nowrap}
.row-btn:hover{background:var(--yg-tone-violet-bg)}.row-btn.danger{color:var(--yg-rose)}.row-btn.danger:hover{background:var(--yg-tone-red-bg)}.row-btn:disabled{opacity:.5;cursor:wait}
.status-tag{padding:3px 9px;border-radius:14px;font-size:11px;font-weight:600;background:var(--yg-tone-slate-bg);color:var(--yg-tone-slate-fg)}.st-sent{background:var(--yg-tone-teal-bg);color:var(--yg-tone-teal-fg)}.st-pending{background:var(--yg-tone-amber-bg);color:var(--yg-tone-amber-fg)}.st-review{background:var(--yg-tone-violet-bg);color:var(--yg-tone-violet-fg)}.st-retry{background:var(--yg-tone-red-bg);color:var(--yg-tone-red-fg)}.st-failed{background:var(--yg-tone-slate-bg);color:var(--yg-tone-slate-fg)}
.tpl-layout{align-items:start}.var-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:18px}.var-grid button{border:1px solid var(--yg-line);background:var(--yg-surface);border-radius:8px;padding:8px 4px;font-size:11px;color:var(--yg-primary);cursor:pointer}.var-grid button:hover{background:var(--yg-tone-violet-bg)}
/* v0.9.6：模板列表项可点编辑 + 删除；名称区域可点 */
.tpl-mini{display:flex;align-items:center;justify-content:space-between;gap:8px}.tpl-mini-name{display:inline-flex;align-items:center;gap:8px;cursor:pointer;flex:1;min-width:0}.tpl-mini-name:hover b{color:var(--yg-primary)}
.preview-box{border:1px dashed var(--yg-line);border-radius:12px;padding:14px;background:var(--yg-surface)}.preview-title{font-size:11px;color:var(--yg-muted);margin-bottom:8px}.preview-body{font-size:13px;white-space:pre-wrap;line-height:1.7;min-height:60px}
.mt{margin-top:12px}.ml{margin-left:18px}
.login-screen{min-height:100vh;display:grid;place-items:center;background:linear-gradient(135deg,var(--yg-bg) 0%,var(--yg-surface) 60%,var(--yg-tone-teal-bg) 100%);position:relative;overflow:hidden}
.login-deco{position:absolute;border-radius:50%;filter:blur(2px)}.login-deco.d1{width:420px;height:420px;background:rgba(91,91,214,.10);top:-140px;left:-100px}.login-deco.d2{width:360px;height:360px;background:rgba(25,179,166,.11);bottom:-120px;right:-80px}
.login-card{width:400px;max-width:90vw;background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:22px;padding:34px;box-shadow:var(--yg-shadow-lg);z-index:1}
.login-brand{display:flex;align-items:center;gap:14px}.login-brand h1{margin:0;font-size:24px;letter-spacing:-.5px}.login-brand p{margin:3px 0 0;color:var(--yg-muted);font-size:12px}
.login-divider{height:1px;background:var(--yg-line);margin:26px 0}.login-card h3{margin:0 0 5px;font-size:18px}.login-hint{color:var(--yg-muted);font-size:12px;margin:0 0 18px}
.login-btn{width:100%;margin-top:16px}.login-foot{display:flex;align-items:center;gap:7px;justify-content:center;color:#9aa3bd;font-size:11px;margin-top:20px}
.type-chip{padding:2px 7px;border-radius:10px;font-size:10px;font-weight:700;color:#fff;background:#98a1b8}
.type-tv{background:#8b6cf0}.type-movie{background:#5b5bd6}
.identify-chip{color:var(--yg-tone-amber-fg);background:var(--yg-tone-amber-bg);padding:2px 7px;border-radius:10px;font-size:10px}
.match-path{background:var(--yg-tone-blue-bg);border:1px solid var(--yg-line);border-radius:10px;padding:12px 14px;margin-bottom:14px}
.match-path small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:5px}
.match-path b{font-size:13px;word-break:break-all}
.dir-clue{display:block;margin-top:3px;color:#6b74a8;font-size:11px;word-break:break-all}
.match-controls{display:flex;gap:10px}
.match-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(215px,1fr));gap:12px;max-height:46vh;overflow-y:auto;margin-top:12px}
.match-card{display:flex;gap:10px;border:1px solid var(--yg-line);border-radius:12px;padding:10px;cursor:pointer;background:var(--yg-surface);transition:.18s}
.match-card:hover{border-color:var(--yg-primary-2);transform:translateY(-2px)}
.match-card.on{border-color:var(--yg-primary);box-shadow:0 0 0 2px rgba(91,91,214,.18);background:var(--yg-tone-blue-bg)}
.match-poster{width:68px;height:96px;flex:none;border-radius:8px;overflow:hidden;background:var(--yg-bg)}
.match-no-poster{height:100%;display:grid;place-items:center;color:#aab2c9;font-size:11px}
.match-info{display:flex;flex-direction:column;gap:4px;min-width:0;flex:1}
.match-info b{font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.match-info small{font-size:10px;color:var(--yg-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.match-info span{font-size:10px;color:#8a93aa}
.match-type{font-weight:700;align-self:flex-start;padding:2px 7px;border-radius:9px;color:#fff;background:#98a1b8}
.match-type.type-tv{background:#8b6cf0}.match-type.type-movie{background:#5b5bd6}
.match-genres{color:var(--yg-primary)!important}
.match-footer{display:flex;justify-content:space-between;align-items:center}
.ai-test{display:flex;gap:10px;margin-top:14px}
.ai-result{margin-top:10px;background:var(--yg-bg);border:1px solid var(--yg-line);border-radius:10px;padding:10px}
.ai-result pre{margin:0;font-size:11px;white-space:pre-wrap;word-break:break-all;color:#4a5578}
/* ============ v0.7.0：状态栏 / 批量管理 / 角标 ============ */
.status-tabs{display:flex;flex-wrap:wrap;gap:12px 20px;align-items:center;margin-bottom:14px;padding:10px 14px;background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:12px}
.status-tab-group{display:flex;align-items:center;gap:5px;flex-wrap:wrap}
.stab-label{font-size:11px;color:var(--yg-muted);margin-right:2px}
.status-tab-group button{border:1px solid transparent;background:var(--yg-bg);color:var(--yg-muted);border-radius:16px;padding:5px 12px;font-size:12px;cursor:pointer;transition:.16s}
.status-tab-group button:hover{background:var(--yg-tone-violet-bg);color:var(--yg-primary)}
.status-tab-group button.on{background:var(--yg-primary);color:#fff;box-shadow:0 4px 10px rgba(91,91,214,.22)}
.batch-bar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:14px;padding:10px 14px;background:linear-gradient(135deg,var(--yg-tone-violet-bg),var(--yg-surface));border:1px solid var(--yg-line);border-radius:12px;font-size:12px}
.batch-bar b{color:var(--yg-primary)}
.grid-toolbar{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin:0 0 8px 2px;font-size:12px;color:var(--yg-muted)}
.grid-toolbar-right{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.select-all{display:flex;align-items:center;gap:6px;cursor:pointer;user-select:none}
.select-all input,.card-check{width:15px;height:15px;accent-color:var(--yg-primary);cursor:pointer}
/* v2.1.6：资源库按 TMDB 分类分组 + 折叠 */
.lib-groups{display:flex;flex-direction:column;gap:16px}
.lib-group{border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);background:var(--yg-surface);padding:12px 14px 14px;box-shadow:var(--yg-shadow)}
.lib-group-head{display:flex;align-items:center;gap:10px;width:100%;border:0;background:transparent;padding:2px 0 10px;cursor:pointer;text-align:left;color:var(--yg-ink)}
.lib-group-head:hover b{color:var(--yg-primary)}
.lib-group-caret{font-size:13px;color:var(--yg-muted);width:14px;flex:none}
.lib-group-head b{font-size:14px;font-weight:750}
.lib-group-count{font-size:11px;color:var(--yg-muted);background:var(--yg-bg);border-radius:8px;padding:2px 8px;font-weight:700}
.lib-group-hint{margin-left:auto;font-size:11px;color:var(--yg-muted)}
/* v2.1.6 #245：transition 与 .media-card 基础档统一（此前 .2s / .18s 两处取值
   同特异度后者胜，基础档的 .2s 一直失效）——统一为 .2s。 */
.media-card.series-card{cursor:pointer;position:relative;transition:.2s}
.media-card.series-card.picked{border-color:var(--yg-primary);box-shadow:0 0 0 2px rgba(91,91,214,.25)}
.card-check{position:absolute;top:8px;left:8px;z-index:5;width:18px;height:18px;background:rgba(255,255,255,.9);border-radius:4px}
.finish-badge{position:absolute;bottom:8px;left:8px;padding:2px 7px;border-radius:9px;font-size:9px;font-weight:700;color:#fff;z-index:4}
.fin-ended{background:rgba(25,179,166,.92)}.fin-returning{background:rgba(242,163,60,.95)}
.dur-badge{position:absolute;top:8px;right:8px;padding:2px 7px;border-radius:9px;background:rgba(20,24,40,.72);color:#fff;font-size:9px;z-index:4}
.tg-extra-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:14px}
.title-link{color:var(--yg-primary);text-decoration:none;cursor:pointer}.title-link:hover{text-decoration:underline}
/* ============ v0.8.0：间隔按钮 / 镜像 / 互动 ============ */
.preset-row{display:flex;flex-wrap:wrap;gap:8px;margin:6px 0 4px}
.preset-btn{border:1px solid var(--yg-line);background:var(--yg-surface);color:var(--yg-ink);border-radius:8px;padding:6px 12px;font-size:12px;cursor:pointer;transition:.15s}
.preset-btn:hover{border-color:var(--yg-primary)}
.preset-btn.on{background:var(--yg-primary);border-color:var(--yg-primary);color:#fff;font-weight:600}
.inline-label{display:flex;align-items:center;gap:7px;cursor:pointer;font-weight:400}
.inline-form{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:14px}
@media(max-width:1100px){.sidebar{width:72px}.brand>div:last-child,.nav-group-label,.nav-text,.nav-badge,.sidebar-foot{display:none}.nav-item{justify-content:center;padding:12px}.content{padding:20px}.two-col,.three-col,.form-grid,.auth-summary,.kv-grid,.credential-bar,.manage-grid{grid-template-columns:1fr}.stats-cards,.pipeline-flow{grid-template-columns:repeat(3,1fr)}.task-grid{grid-template-columns:repeat(2,1fr)}.link-modules{grid-template-columns:1fr;gap:0}.link-module .sec-band:not(:first-child){margin-top:22px}}

/* ============ v0.6.5 手机 / 触屏适配 ============ */
/* ≤900px：侧栏从左侧竖排改为顶部横向导航，主区占满宽度 */
@media(max-width:900px){
  .app-shell{flex-direction:column}
  .sidebar{width:100%;height:auto;position:sticky;top:0;z-index:30;flex-direction:column;border-radius:0;box-shadow:0 4px 16px rgba(23,27,53,.18)}
  .brand{padding:10px 14px;border-bottom:1px solid rgba(255,255,255,.08)}
  .brand-mark{width:30px;height:30px;border-radius:9px;font-size:12px}
  .brand-logo{width:30px;height:30px;border-radius:9px}
  .brand-name{font-size:15px}
  .nav-groups{display:flex;flex-direction:row;gap:6px;padding:8px 10px;overflow-x:auto;overflow-y:hidden;white-space:nowrap;-webkit-overflow-scrolling:touch}
  .nav-group{display:flex;flex-direction:row;gap:6px;margin:0;flex:none}
  .nav-group-label,.sidebar-foot{display:none}
  .nav-text,.nav-badge{display:inline}
  .nav-item{width:auto;flex:none;justify-content:flex-start;gap:7px;padding:8px 12px;font-size:12px}
  .nav-item .ico{width:15px;height:15px}
  .nav-badge{margin-left:2px}
  .main-wrap{width:100%}
  .topbar{height:56px;padding:0 14px}
  .content{padding:16px 14px 48px}
  .hero{padding:20px;flex-wrap:wrap}
  .hero h2{font-size:22px}
  .top-actions .clock{display:none}
  /* 顶部操作区在窄屏允许按钮收缩 */
  .top-actions{gap:6px}
}
/* 表格横向滚动：包一层即可，避免列多撑破布局 */
.table-scroll{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}
.table-scroll .el-table{min-width:560px}
/* 固定宽度内联控件的通用兜底：窄屏不要溢出 */
.head-tools .el-input,.head-tools .el-select{max-width:100%}
/* v0.9.13 #251：手机端卡片防溢出。grid 子项默认 min-width:auto 会被内容撑破，
   统一改为 minmax(0,1fr) 语义；面板/统计卡强制 min-width:0 并允许长文本换行。 */
.stats-cards,.pipeline-flow,.manage-grid,.stat-grid,.media-grid,.var-grid,.tg-extra-grid,
.credential-bar,.auth-summary,.kv-grid,.form-grid,.two-col,.three-col{min-width:0}
.stats-cards>*,.pipeline-flow>*,.manage-grid>*,.stat-grid>*,.media-grid>*,
.two-col>*,.three-col>*,.form-grid>*{min-width:0}
.panel,.stat-card,.manage-card{width:100%;min-width:0;overflow:hidden}
.panel p,.panel .hint,.stat-card,.el-table .cell{overflow-wrap:anywhere;word-break:break-word}
/* 超宽表格统一横向滚动，不撑破卡片 */
.panel .el-table{width:100%}
/* ≤680px：单列 + 海报墙两列 + 表单堆叠 */
@media(max-width:680px){
  .content{padding:14px 10px 44px}
  .topbar{padding:0 10px}
  .crumb{font-size:12px}
  .page-head{align-items:flex-start}
  .page-head h2{font-size:19px}
  .head-tools{width:100%;justify-content:flex-start}
  .head-tools .el-input,.head-tools .el-select{width:100%!important;flex:1 1 100%}
  .stat-grid{grid-template-columns:repeat(2,1fr);gap:10px}
  /* 5 列进度/统计条在手机上折行，避免卡片超出屏幕 */
  .stats-cards{grid-template-columns:repeat(2,1fr);gap:8px}
  .pipeline-flow{grid-template-columns:repeat(2,1fr);gap:8px}
  .task-grid{grid-template-columns:1fr;gap:10px}
  .stat-num{font-size:22px}
  .hero{padding:18px;gap:14px}
  .hero h2{font-size:20px}
  .hero p{font-size:11px}
  .hero-actions{width:100%;justify-content:space-between}
  .media-grid{grid-template-columns:repeat(2,1fr);gap:10px}
  .media-poster{height:auto;aspect-ratio:2/3}
  .media-actions .el-button{margin:0 3px 4px 0;padding:5px 8px;font-size:11px}
  .credential-bar,.auth-summary,.kv-grid,.var-grid,.tg-extra-grid{grid-template-columns:repeat(2,1fr);gap:10px}
  .status-tabs{gap:8px 12px;padding:8px 10px}
  .batch-bar{gap:6px}
  .cursor-box{grid-template-columns:repeat(2,1fr);gap:10px}
  .connect-steps{flex-wrap:wrap}
  .cstep{flex:1 1 45%}
  .match-controls{flex-wrap:wrap}
  .match-controls .el-input,.match-controls .el-select{min-width:140px;flex:1 1 100%}
  .schedule-controls{flex-wrap:wrap;gap:6px}
  .schedule-controls .schedule-label{width:auto}
  .match-grid{grid-template-columns:1fr;max-height:50vh}
  .field.inline{flex-wrap:wrap}
  .panel{padding:15px}
  .panel-foot{flex-wrap:wrap}
  .el-dialog{width:92%!important;margin-top:6vh!important}
  .el-dialog__body{padding:16px}
  .tl-action{min-width:0}
  .tl-time{min-width:60px}
  .device-code{font-size:22px;letter-spacing:4px}
}
/* ≤400px：极窄屏海报墙保留两列但进一步压缩 */
@media(max-width:400px){
  .media-grid{gap:8px}
  .series-card .media-actions .el-button{font-size:10px;padding:4px 6px}
  .stat-card{padding:14px}
}
/* v0.9.14 #258：自动发现群组 / 多目标路由 */
.discover-bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.disc-name{font-weight:600;overflow-wrap:anywhere;word-break:break-word}
.disc-sub{color:var(--yg-muted);font-size:12px;overflow-wrap:anywhere;word-break:break-word}
.route-targets{margin:2px 0}
.route-target-row{display:flex;gap:6px;align-items:center;margin-bottom:6px;min-width:0}
.route-target-row .el-select{min-width:0}
/* v0.9.15 #276：加群/频道引导 + 授权链接 */
.disc-addtarget{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.disc-addtarget-col{border:1px solid var(--yg-border,#e6e8eb);border-radius:10px;padding:12px;min-width:0;background:var(--yg-panel-soft,rgba(127,127,127,.04))}
.disc-addtarget-title{display:flex;align-items:center;gap:8px;font-weight:600}
.disc-invite-box{margin-top:8px;padding:8px 10px;border-radius:8px;background:var(--yg-panel-soft,rgba(127,127,127,.06));font-size:12px;display:flex;flex-direction:column;gap:4px;min-width:0}
.disc-url{font-family:monospace;overflow-wrap:anywhere;word-break:break-all}
.disc-guide-list{margin:0 0 12px;padding-left:20px;line-height:1.9}
@media(max-width:820px){.disc-addtarget{grid-template-columns:1fr}}

/* ============ 阶段六：深色主题（附加层，浅色默认不变，删掉 html.dark 即回到原样） ============ */
/* v2.1.6 #245：浅色兜底变量（--yg-border / --yg-panel-soft）已上移到主 :root 块，
   深色段只保留 html.dark 的覆盖值——避免「删掉 html.dark 即回到原样」时
   浅色兜底还留在深色段里造成语义混淆。 */
html.dark{
  --yg-bg:#0f1422;--yg-surface:#1a2133;--yg-ink:#e6eaf5;--yg-muted:#96a0bd;--yg-line:#2b3450;
  --yg-primary:#8b8bf0;--yg-primary-2:#a99bf7;--yg-teal:#3fd0c2;--yg-amber:#f5b65c;--yg-rose:#f07a90;
  --yg-shadow:0 1px 2px rgba(0,0,0,.25),0 12px 32px rgba(0,0,0,.35);
  --yg-shadow-lg:0 2px 4px rgba(0,0,0,.3),0 20px 48px rgba(0,0,0,.45);
  --yg-border:#2b3450;--yg-panel-soft:rgba(255,255,255,.04);
  /* v2.1.6 #245：语义色 token 的深色值——底色转深色、字色转亮色，
     保证「浅色底 + 深字」的成对关系在深色下依然成立（对比度不塌）。 */
  --yg-tone-blue-bg:#232a52;--yg-tone-blue-fg:#a9b0ff;
  --yg-tone-green-bg:#13352e;--yg-tone-green-fg:#5fd3b6;
  --yg-tone-amber-bg:#3a2d16;--yg-tone-amber-fg:#f0b464;
  --yg-tone-red-bg:#3a1f28;--yg-tone-red-fg:#f08a9c;
  --yg-tone-violet-bg:#2a2450;--yg-tone-violet-fg:#b5a6ff;
  --yg-tone-teal-bg:#123338;--yg-tone-teal-fg:#5ec9c9;
  --yg-tone-slate-bg:#242c42;--yg-tone-slate-fg:#9aa3bd;
  --yg-panel:#1b2233;
  --yg-grad-a:#1b2140;--yg-grad-b:#12262b;
  --el-color-primary:#8b8bf0;
  color-scheme:dark;
}
html.dark body{background:radial-gradient(1200px 600px at 100% -10%,var(--yg-grad-a) 0%,transparent 55%),radial-gradient(900px 500px at -10% 100%,var(--yg-grad-b) 0%,transparent 55%),var(--yg-bg)}
/* 面板/卡片/表格：把硬编码白底换成 surface 变量 */
html.dark .panel,html.dark .card,html.dark .stat-card,html.dark .section-card,
html.dark .method-card,html.dark .bot-card,html.dark .media-card,html.dark .login-card,
html.dark .auth-summary-item,html.dark .credential-bar,html.dark .kv-grid>div,
html.dark .cstep,html.dark .webqr-box,html.dark .preview-box,html.dark .mini-stat,
html.dark .info-chip,html.dark .cmd-item,html.dark .row-btn,html.dark .series-card{
  background:var(--yg-surface);color:var(--yg-ink);border-color:var(--yg-line)
}
html.dark .el-table th.el-table__cell{background:#202940!important;color:#b8c2dd}
html.dark .el-table,html.dark .el-table tr,html.dark .el-table td.el-table__cell{background:var(--yg-surface);color:var(--yg-ink)}
html.dark .el-table--striped .el-table__body tr.el-table__row--striped td.el-table__cell{background:#1e2638}
html.dark .el-table__inner-wrapper::before,html.dark .el-table td.el-table__cell::before{background:var(--yg-line)}
html.dark .topbar{background:var(--yg-surface)}
html.dark .content{color:var(--yg-ink)}
html.dark .crumb,html.dark .page-head h2,html.dark .panel-head h3{color:var(--yg-ink)}
html.dark .stat-bar,html.dark .media-poster{background:#232c42}
html.dark .status-tag,html.dark .tone-indigo .stat-icon,html.dark .tone-teal .stat-icon,
html.dark .grp-ico.g-amber,html.dark .grp-ico.g-violet{color:var(--yg-ink)}
html.dark .hero{background:linear-gradient(120deg,#1b2340,#172a2f);color:var(--yg-ink)}
html.dark .unavailable-box,html.dark .mirror-lock-tip,html.dark .batch-control-row{background:#242c44}
/* v2.1.6 #245：其余页面的「带底色容器」深色覆盖。
   这些容器此前写死浅色渐变/浅底（.connect-hero 浅蓝渐变、.credential-bar 浅青渐变、
   .restrict-card 浅绿渐变、.device-panel 浅紫渐变、.match-path 浅蓝底、.device-code 浅紫底），
   深色下是一块块刺眼浅斑。 */
html.dark .connect-hero{background:linear-gradient(135deg,#1a2340,#16283a);border-color:var(--yg-line)}
html.dark .connect-head h3{color:var(--yg-ink)}
html.dark .credential-bar{background:linear-gradient(135deg,#162a2b,#1b2338);border-color:var(--yg-line)}
html.dark .restrict-card{background:linear-gradient(180deg,#16281f,#1a2133);border-color:var(--yg-line)}
html.dark .restrict-card .bot-ico{background:var(--yg-tone-green-bg);color:var(--yg-tone-green-fg)}
html.dark .restrict-label b,html.dark .restrict-row .unit{color:var(--yg-tone-green-fg)}
html.dark .restrict-label small{color:var(--yg-muted)}
html.dark .restrict-row{border-bottom-color:var(--yg-line)}
html.dark .device-panel{border-color:var(--yg-line);background:linear-gradient(180deg,#1b2136,#1a2133)}
html.dark .device-code{background:#1b2140;border-color:var(--yg-line);color:var(--yg-tone-violet-fg)}
html.dark .webqr-code b{color:var(--yg-tone-violet-fg)}
html.dark .match-path{background:var(--yg-tone-blue-bg);border-color:var(--yg-line)}
html.dark .match-card{background:var(--yg-surface);border-color:var(--yg-line)}
html.dark .match-card.on{background:var(--yg-tone-blue-bg)}
html.dark .match-poster{background:var(--yg-bg)}
html.dark .media-poster{background:#232c42}
html.dark .series-card .media-poster{background:#232c42}
html.dark .no-poster,html.dark .match-no-poster{color:var(--yg-muted)}
html.dark .view-switch button{background:var(--yg-surface)}
html.dark .view-switch button.on{background:var(--yg-tone-violet-bg);color:var(--yg-primary)}
html.dark .stat-bar{background:var(--yg-line)}
html.dark .dist-track{background:var(--yg-line)}
html.dark .el-input__wrapper,html.dark .el-select__wrapper,html.dark .el-textarea__inner{background:var(--yg-surface);box-shadow:0 0 0 1px var(--yg-line) inset}
html.dark .el-dialog,html.dark .el-message-box,html.dark .el-popper{background:var(--yg-surface);color:var(--yg-ink)}
html.dark .nav-item:hover{background:rgba(255,255,255,.06)}
html.dark .task-notice{background:#242c44;color:var(--yg-muted)}
html.dark .task-card-foot small,html.dark .tm-label{color:var(--yg-muted)}
html.dark .task-state{background:rgba(255,255,255,.06)}
/* v2.1.6 #245：分享中心（.sc-*）深色覆盖。
   此前 .sc-* 体系**零条** html.dark 覆盖——所有卡片/表格/徽标都写死浅色底或浅色字，
   切到深色主题后分享中心整页失配（浅底白字不可读、深底黑字不可见）。
   语义色已 token 化（--yg-tone-*），此处只需补齐「结构性」覆盖：
   ①卡片容器底色/描边；②表格与横向滚动条；③弹窗内文字；④深色下的分隔线与占位底。 */
html.dark .sc-card,html.dark .sc-tabbar,html.dark .sc-health,html.dark .sc-kpi,
html.dark .sc-source-note,html.dark .sc-inline-detail,html.dark .sc-task-stats{
  background:var(--yg-surface);border-color:var(--yg-line);color:var(--yg-ink)
}
html.dark .sc-tab{color:var(--yg-muted)}
html.dark .sc-tab:hover{background:var(--yg-bg);color:var(--yg-ink)}
html.dark .sc-subtab{color:var(--yg-muted)}
html.dark .sc-detail-kv>div,html.dark .sc-media-thumb,html.dark .sc-detail-poster,
html.dark .sc-share-poster,html.dark .sc-hot-rank,html.dark .sc-seg,
html.dark .sc-hour-row{background:var(--yg-bg);color:var(--yg-ink)}
html.dark .sc-week-cell{background:var(--yg-panel);border-color:var(--yg-line)}
html.dark .sc-week-cell.has{background:var(--yg-tone-violet-bg)}
html.dark .sc-card-head h3,html.dark .sc-card-head p{color:var(--yg-ink)}
html.dark .sc-card-head p{color:var(--yg-muted)}
html.dark .sc-table-scroll .el-table{color:var(--yg-ink)}
html.dark .sc-task-stats b.ok{color:var(--yg-tone-green-fg)}
html.dark .sc-task-stats b.bad{color:var(--yg-tone-red-fg)}
html.dark .sc-task-stats b.muted{color:var(--yg-muted)}
html.dark .sc-health span{box-shadow:0 2px 6px rgba(0,0,0,.35)}
html.dark .sc-hero-card{background:linear-gradient(120deg,#1b2340 0%,#1b2a3a 52%,#16303a 100%)}
html.dark .sc-link-source-note{background:var(--yg-surface);border-color:var(--yg-line)}
html.dark .sc-seg button.on{background:var(--yg-primary);color:#fff}
html.dark .sc-cell-warn{color:var(--yg-rose)}
/* v2.1.6 #245：登录页 / 步骤条 / 状态标签的深色收口。 */
html.dark .login-screen{background:linear-gradient(135deg,var(--yg-bg) 0%,var(--yg-surface) 60%,var(--yg-tone-teal-bg) 100%)}
html.dark .login-foot{color:var(--yg-muted)}
html.dark .step-head span{background:var(--yg-tone-violet-bg)}
html.dark .cstep b{background:var(--yg-tone-slate-bg);color:var(--yg-tone-slate-fg)}
html.dark .status-tag{color:var(--yg-tone-slate-fg)}
html.dark .st-sent{color:var(--yg-tone-teal-fg)}
/* v2.1.7 #241：阅读量统计页的深色收口。 */
html.dark .sc-stats-state{background:var(--yg-surface);border-color:var(--yg-line)}
html.dark .sc-state-dot.on{background:var(--yg-tone-green-fg);box-shadow:0 0 0 3px rgba(95,211,182,.16)}
html.dark .sc-state-dot.off{background:var(--yg-muted);box-shadow:0 0 0 3px rgba(154,163,189,.16)}
html.dark .sc-state-hint{color:var(--yg-muted)}
html.dark .sc-stats-config{border-color:var(--yg-line)}
html.dark .sc-stats-config .unit,html.dark .sc-stats-config small{color:var(--yg-muted)}
html.dark .st-pending{color:var(--yg-tone-amber-fg)}
html.dark .st-review{color:var(--yg-tone-violet-fg)}
html.dark .st-retry{color:var(--yg-tone-red-fg)}
html.dark .st-failed{color:var(--yg-tone-slate-fg)}
html.dark .card-check{background:rgba(20,24,40,.9)}
</style>
