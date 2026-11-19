<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import QRCode from 'qrcode'

const password = ref(localStorage.getItem('ya_tg_password') || '')
const authed = ref(false)
const tab = ref('概览')
const clock = ref('')
const viewMode = ref('series')  // v0.6.4：作品视图为默认，单集按剧聚合，不再散落
const keyword = ref('')
const statusFilter = ref('')
const seriesGroups = ref([])   // v0.6.4：/api/library/series 聚合结果

const overview = ref({}), task = ref({ cron: '*/30 * * * *', enabled: true })
const resources = ref([])
const reviews = ref([]), queue = ref([]), templates = ref([]), audit = ref([])
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
const mirrorCfg = ref({ name: '默认镜像', source_root_id: '', target_root_id: '', interval_minutes: 60, enabled: false, star_naming: true, auto_identify: true, file_idle_seconds: 1.0, min_idle_seconds: 0.5, auto_create_dirs: true, rename_files: true, max_works_per_run: 0, interval_presets: [], unverified_endpoints: [], note: '' })
const mirrorStatus = ref({ counts: {}, media: [], dirs: [], missing_dirs: [], tasks: [], rule: {}, config: {}, unverified_endpoints: [], rename_errors: [], untranslated_works: [], lock: {}, pipeline: [], progress: {} })
// v0.8.9：诊断表格与长说明默认收起，镜像同步页不再把所有信息堆在一屏
const mirrorDiagOpen = ref([])
const diagCount = computed(() => {
  const s = mirrorStatus.value
  return (s.missing_dirs?.length || 0) + (s.file_errors?.length || 0) +
    (s.untranslated_works?.length || 0) + (s.rename_errors?.length || 0)
})
const scanBatches = ref([]), mirrorBusy = ref(false), mirrorSaving = ref(false)
// v0.8.1：扫描设置不再单独成页，折叠进「资源库」
// v0.8.3：定时配置里不再出现任何根目录 ID 输入框，统一引用镜像同步里的 B 库 ID
const scanning = ref(false), showScanPanel = ref(false)
// v0.9.4：Bot 交互配置——交互 Bot 复用 bots 表 token、白名单复用推送目标；内置四指令固定
const interactionCfg = ref({ contribution_enabled: false, donation_enabled: false, tgto_base_url: '', tgto_scene: 'ENV_FILTER_GUANGYA', tgto_api_key: '', interaction_bot_id: '', donation_image_url: '', donation_text: '', donation_button_text: '打赏支持',
  bot_push_channels: [], bot_push_groups: [], bot_restrict_enabled: false, bot_restrict_cache_seconds: 120, bot_required_channels: [], bot_required_groups: [], bot_commands: [], bot_gate_mode: 'whitelist', bot_options: [],
  // v0.9.5：使用限制跳转链接 + 推送消息频道/群跳转（模板变量 channel_link/group_link）
  bot_required_channel_link: '', bot_required_group_link: '', tg_channel_link: '', tg_group_link: '' })
// v0.9.5：推送白名单 = 目录路由表 + 全局单 Bot
const pushCfg = ref({ bot: null, max_routes: 10 })
// v0.9.8：一条路由的频道 ID 与群 ID 分别填写，可只填其一或两个都填
const pushRoutes = ref([{ dir_id: '', channel_id: '', group_id: '', note: '' }])
const pushSaving = ref(false), pushBotToken = ref('')
const broadcastText = ref(''), broadcastSending = ref(false)
function addPushRoute() {
  if (pushRoutes.value.length >= (pushCfg.value.max_routes || 10)) { ElMessage.warning('推送白名单最多 10 条'); return }
  pushRoutes.value.push({ dir_id: '', channel_id: '', group_id: '', note: '' })
}
function removePushRoute(i) { pushRoutes.value.splice(i, 1) }
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

let clockTimer = null, webqrTimer = null

const navGroups = [
  { label: '工作台', items: [{ key: '概览', icon: 'grid', label: '概览' }] },
  { label: '光鸭资源', items: [
    { key: '光鸭授权', icon: 'key', label: '光鸭授权' },
    { key: '镜像同步', icon: 'copy', label: '镜像同步' },
    { key: '资源库', icon: 'folder', label: '资源库' }
  ] },
  { label: '分发与互动', items: [
    { key: '分发与互动中心', icon: 'link', label: '分发与互动中心' },
    { key: '审核中心', icon: 'check', label: '审核中心' },
    { key: '推送队列', icon: 'send', label: '推送队列' }
  ] },
  { label: '系统', items: [
    { key: 'TMDB 设置', icon: 'image', label: 'TMDB 设置' },
    { key: '消息模板', icon: 'file', label: '消息模板' },
    { key: '操作审计', icon: 'clock', label: '操作审计' }
  ] }
]

const paths = {
  grid: 'M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z',
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
  logout: 'M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9'
}
const icon = name => paths[name] || paths.grid

const syntaxHint = computed(() => '{{#if year}}年份 {{year}}{{/if}} · 条件块支持嵌套')
// 说明文字含 {{...}} 字面量，必须由 computed 返回字符串，不能直接写进 Vue 模板
const tagHint = computed(() => 'TMDB 分类打标：{{type}}=movie/tv，{{type_label}}=电影/剧集，{{origin_country}}=制片地区，{{genres}}=类型，{{tags}}=组合标签（示例 #保镖恋人 #电影 #爱情 #惊悚）。按示例打标可写：type={{type}} · origin_country={{origin_country}} · genres={{genres}}')
// v0.8.9：剧集状态 / 集数进度变量，说明含 {{}} 字面量必须由 computed 返回
const episodeHint = computed(() => '剧集状态：{{episode_status}}=追更「更新至 S01E07 · 连载中」/ 收齐「S01E24 · 完结」，{{episode_progress}}=N/M（如 12/24），{{updated_to_episode}}=已更新集号，{{total_episodes}}=TMDB 官方总集数，{{current_episode}}=当前集，{{episode_count}}=本地已收录集数。电影时这些变量为空，配合 {{#if}} 条件块使用')
const templateVars = ['title', 'overview', 'year', 'resolution', 'version', 'season_label', 'episode_count', 'reward_code', 'size', 'share_url',
  'tmdb_id', 'tmdb_url', 'type', 'type_label', 'origin_country', 'origin_country_code',
  'genres', 'tags', 'vote_average', 'original_title', 'season', 'episode', 'episode_tag', 'media_type', 'filename',
  'episode_status', 'episode_progress', 'updated_to_episode', 'total_episodes', 'current_episode']
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
  total_episodes: '', current_episode: ''
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

const headers = () => ({ 'X-Admin-Password': password.value, 'Content-Type': 'application/json' })
// FastAPI 的 detail 可能是字符串、dict 或 422 校验数组；直接 Error(obj) 会显示 [object Object]
function errText(value, fallback = '请求失败') {
  if (value == null || value === '') return fallback
  if (typeof value === 'string') return value
  if (Array.isArray(value)) return value.map(x => x?.msg || x?.message || JSON.stringify(x)).join('；')
  if (typeof value === 'object') return value.message || value.msg || value.detail || JSON.stringify(value)
  return String(value)
}
async function api(path, opt = {}, timeoutMs = 0) {
  // v0.7.1：全量重扫可能耗时数分钟，允许单独放宽超时（AbortSignal 不影响其它请求）
  const ctl = timeoutMs ? new AbortController() : null
  const timer = ctl ? setTimeout(() => ctl.abort(), timeoutMs) : null
  try {
    const r = await fetch('/api' + path, { ...opt, ...(ctl ? { signal: ctl.signal } : {}),
      headers: { ...headers(), ...(opt.headers || {}) } })
    const d = await r.json().catch(() => ({}))
    if (!r.ok) throw Error(errText(d.detail ?? d.message, r.statusText))
    return d
  } catch (e) {
    if (ctl && ctl.signal.aborted) throw Error('扫描仍在进行中，请稍后刷新资源库查看结果')
    throw e
  } finally { if (timer) clearTimeout(timer) }
}
async function load() {
  overview.value = await api('/overview')
  task.value = await api('/scan-task')
  resources.value = await api('/resources')
  try { seriesGroups.value = await api('/library/series') } catch (e) { seriesGroups.value = [] }
  reviews.value = await api('/review')
  queue.value = await api('/queue')
  templates.value = await api('/templates')
  try { tplPresets.value = await api('/templates/presets') } catch (e) { tplPresets.value = [] }
  audit.value = await api('/audit')
  try { diagnostics.value = await api('/diagnostics') } catch (e) { diagnostics.value = { blockers: [] } }
  try { await loadTgExtra() } catch (e) {}
  try { const t = await api('/telegram/config'); telegramBase.value = t.base_url || t.default || '' } catch (e) {}
  try { queueSchedule.value = await api('/queue/schedule') } catch (e) {}
  // v0.8.0：每个新模块独立加载，任一接口异常不影响主界面（沿用既有约定）
  try { scanInterval.value = task.value.interval_minutes || 0; scanCustom.value = !task.value.interval_minutes } catch (e) {}
  try { mirrorCfg.value = await api('/mirror/config') } catch (e) {}
  try { mirrorStatus.value = await api('/mirror/status') } catch (e) {}
  try { scanBatches.value = await api('/scan/batches?limit=5') } catch (e) {}
  try { interactionCfg.value = await api('/interaction/config') } catch (e) {}
  try {
    const pr = await api('/push/routes')
    pushCfg.value = { bot: pr.bot || null, max_routes: pr.max_routes || 10 }
    pushRoutes.value = (pr.routes && pr.routes.length)
      ? pr.routes.map(r => ({ dir_id: r.dir_id || '', channel_id: r.channel_id || '', group_id: r.group_id || '', note: r.note || '' }))
      : [{ dir_id: '', channel_id: '', group_id: '', note: '' }]
  } catch (e) {}
  try { interactionStatus.value = await api('/interaction/status') } catch (e) {}
  try { contributions.value = await api('/contributions?limit=50') } catch (e) {}
  try { mediaRequests.value = await api('/media-requests?limit=50') } catch (e) {}
  try { await loadPushStats() } catch (e) {}
  // v0.8.3：日志概览独立加载，任一接口异常不影响主界面
  try { await loadLogs() } catch (e) {}
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
  try { await load(); authed.value = true; localStorage.setItem('ya_tg_password', password.value) }
  catch (e) { ElMessage.error(e.message) }
}
function logout() { authed.value = false; password.value = ''; localStorage.removeItem('ya_tg_password') }
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
const refreshing = ref(false)
function reconcileTip(d) {
  const rc = d.reconcile || {}
  if (rc.skipped) return `对账已跳过（${rc.skipped}）`
  const empty = rc.empty_seen_allowed ? '（B 库为空，面板已按实际内容清空）' : ''
  return `移除 ${rc.removed || 0} 项、取消分享 ${rc.shares_cancelled || 0} 个${empty}`
}
async function refreshAll() {
  refreshing.value = true
  try {
    const d = await api('/library/refresh', { method: 'POST' }, 180000)
    ElMessage.success(`已按 B 库最新内容刷新：${d.files || 0} 个文件 · ${reconcileTip(d)}`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { refreshing.value = false }
}
async function scan() {
  scanning.value = true
  try {
    const d = await api('/scan-now', { method: 'POST' }, 180000)
    ElMessage.success(`扫描完成：${d.files || 0} 个文件 · ${reconcileTip(d)}`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { scanning.value = false }
}
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
    const q = mode === 'auto' ? '' : `?post_identify=${mode === 'force' ? 'true' : 'false'}`
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
// v0.9.6：清空「英文改名待处理」清单——把英文名为空的作品标记 ignored，
// 从待处理清单消失；不改文件、不删映射，点「重新解析英文名并改名」可恢复。
async function clearUntranslated() {
  const n = mirrorStatus.value.untranslated_works?.length || 0
  if (!n) { ElMessage.info('当前没有待处理项'); return }
  const ok = await confirmBox(
    `将把 ${n} 部「英文名为空」的作品标记为已忽略，从待处理清单移除。\n\n` +
    '不删映射、不动文件、不影响入库与分享。若这些作品仍在 A 库且需要改名，' +
    '日后点「重新解析英文名并改名」会清掉标记重新解析。')
  if (!ok) return
  mirrorBusy.value = true
  try {
    const d = await api('/mirror/rename/untranslated/clear', { method: 'POST' })
    ElMessage.success(`已清空 ${d.cleared || 0} 条待处理项`)
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
// v0.9.8：清理历史无效影视映射（failed/blocked 且无 synced 文件的僵尸行）。
async function clearInvalidMedia() {
  const n = mirrorStatus.value.counts?.media_invalid || 0
  if (!n) { ElMessage.info('当前没有可清理的无效影视映射'); return }
  const ok = await confirmBox(
    `将物理删除 ${n} 条历史无效影视映射（状态失败/阻断且没有任何已同步文件，属历史遗留僵尸行）。\n\n` +
    '只删 A→B 映射登记及其文件映射，不删资源 / 分享 / 推送队列 / B 库文件。' +
    'A 库仍有源文件时，下一轮「立即同步」会自动重建映射。')
  if (!ok) return
  mirrorBusy.value = true
  try {
    const d = await api('/mirror/media/clear-invalid', { method: 'POST' })
    ElMessage.success(`已清理 ${d.removed_media || 0} 条无效影视映射（${d.removed_files || 0} 个文件映射）`)
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
  const routes = (pushRoutes.value || []).map(r => ({
    dir_id: String(r.dir_id || '').trim(),
    channel_id: String(r.channel_id || '').trim(),
    group_id: String(r.group_id || '').trim(),
    note: String(r.note || '').trim()
  })).filter(r => r.channel_id || r.group_id)
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
    donation_image_url: c.donation_image_url || '', donation_text: c.donation_text || '',
    donation_button_text: c.donation_button_text || '打赏支持',
    // v0.9.4：白名单由「推送目标」启用记录决定，不再单独提交 ID 列表
    interaction_bot_id: String(c.interaction_bot_id || ''),
    bot_restrict_enabled: !!c.bot_restrict_enabled, bot_restrict_cache_seconds: Number(c.bot_restrict_cache_seconds) || 0,
    bot_gate_mode: c.bot_gate_mode || 'whitelist',
    bot_required_channels: c.bot_required_channels || [], bot_required_groups: c.bot_required_groups || [],
    // v0.9.5：使用限制跳转 + 推送消息频道/群跳转
    bot_required_channel_link: c.bot_required_channel_link || '', bot_required_group_link: c.bot_required_group_link || '',
    tg_channel_link: c.tg_channel_link || '', tg_group_link: c.tg_group_link || '',
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
    ElMessage.success('消息跳转链接已保存；留空的一侧不会出现在推送消息里')
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
  try { await api(`/contributions/${id}/handle`, { method: 'POST', body: JSON.stringify({ status, note }) }); await load() }
  catch (e) { ElMessage.error(e.message) }
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
    await api(`/media-requests/${row.id}/handle`, { method: 'POST', body: JSON.stringify({ review_status: review, note }) })
    ElMessage.success(review === 'adopted' ? '已采纳，已通知用户' : review === 'rejected' ? '已拒绝，已通知用户' : '已重置为待处理')
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function deleteMediaRequest(row) {
  const ok = await confirmBox(`确认删除求片「${row.title}」？只删登记记录，不影响 TgtoDrive 关键词规则。`)
  if (!ok) return
  try { await api(`/media-requests/${row.id}`, { method: 'DELETE' }); ElMessage.success('已删除'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
function wantReviewLabel(s) { return ({ pending: '待处理', adopted: '已采纳', rejected: '已拒绝' })[s] || '待处理' }
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
    const reasons = [...(d.skipped || []), ...(d.detail || []).map(x => x.error)]
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
const rescanning = ref(false)
async function rescanLibrary() {
  rescanning.value = true
  try {
    const d = await api('/library/rescan', { method: 'POST' }, 180000)
    const idf = d.identify || {}
    const parts = [`扫描 ${d.files || 0} 个文件`, reconcileTip(d)]
    parts.push(`识别 ${idf.identified || 0} 项、完结回填 ${idf.finish_backfilled || 0} 项`)
    ElMessage.success(parts.join(' · '))
    await load()
  } catch (e) { ElMessage.error(e.message) }
  finally { rescanning.value = false }
}
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
        tab.value = 'TMDB 设置'
        await loadTmdb()
      } catch (_2) {}
    } else ElMessage.error(msg)
  }
}
async function approve(id) { await api(`/review/${id}/approve`, { method: 'POST' }); ElMessage.success('已通过'); await load() }
async function reject(id) { await api(`/review/${id}/reject`, { method: 'POST' }); ElMessage.success('已拒绝'); await load() }
async function retry(id) { await api(`/queue/${id}/retry`, { method: 'POST' }); ElMessage.success('已重新入队'); await load() }
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

onMounted(() => { tick(); clockTimer = setInterval(tick, 1000); if (password.value) login() })
onUnmounted(() => { clearInterval(clockTimer); clearInterval(webqrTimer) })
</script>

<template>
  <div v-if="!authed" class="login-screen">
    <div class="login-deco d1"></div><div class="login-deco d2"></div>
    <div class="login-card">
      <div class="login-brand"><div class="brand-mark lg">YT</div><div><h1>Ya-TG</h1><p>光鸭资源分发工作台</p></div></div>
      <div class="login-divider"></div>
      <h3>欢迎回来</h3>
      <p class="login-hint">请输入管理员密码以访问控制台</p>
      <el-input v-model="password" type="password" show-password placeholder="ADMIN_PASSWORD" @keyup.enter="login" size="large"/>
      <el-button type="primary" size="large" class="login-btn" @click="login">进入工作台</el-button>
      <div class="login-foot"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('shield')"/></svg> 凭证仅保存在本机浏览器 · 北京时间调度</div>
    </div>
  </div>

  <div v-else class="app-shell">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark">YT</div>
        <div><div class="brand-name">Ya-TG</div><div class="brand-sub">资源分发工作台</div></div>
      </div>
      <div class="nav-groups">
        <div v-for="g in navGroups" :key="g.label" class="nav-group">
          <div class="nav-group-label">{{ g.label }}</div>
          <button v-for="it in g.items" :key="it.key" class="nav-item" :class="{ active: tab === it.key }" @click="tab = it.key">
            <svg class="ico" viewBox="0 0 24 24"><path :d="icon(it.icon)"/></svg>
            <span class="nav-text">{{ it.label }}</span>
            <span v-if="it.key === '审核中心' && overview.review" class="nav-badge">{{ overview.review }}</span>
          </button>
        </div>
      </div>
      <div class="sidebar-foot">
        <div class="foot-status"><span class="dot" :class="overview.auth === 'ok' ? 'on' : 'off'"></span>{{ overview.auth === 'ok' ? '光鸭已授权' : '光鸭未授权' }}</div>
        <div class="foot-version">v0.9.8 · Asia/Shanghai</div>
      </div>
    </aside>

    <div class="main-wrap">
      <header class="topbar">
        <div class="crumb"><span>Ya-TG</span><b>/</b><strong>{{ tab }}</strong></div>
        <div class="top-actions">
          <span class="clock">{{ clock }}</span>
          <el-button size="small" plain @click="verify">验证连接</el-button>
          <el-button size="small" type="primary" plain :loading="refreshing" @click="refreshAll">
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
            <div class="panel-head"><h3>处理流程与进度</h3><span class="hint">{{ mirrorStatus.progress?.running ? '任务运行中' : '实时状态' }}</span></div>
            <div class="sync-progress">
              <div class="sync-progress-track"><i :style="{ width: (mirrorStatus.progress?.percent || 0) + '%' }"></i></div>
              <span>{{ mirrorStatus.progress?.percent || 0 }}%（{{ mirrorStatus.progress?.synced || 0 }}/{{ mirrorStatus.progress?.total || 0 }} 文件已同步）</span>
            </div>
            <div class="pipeline-flow">
              <div v-for="node in (mirrorStatus.pipeline || [])" :key="node.key" class="pipe-node" :class="'is-' + (node.status || 'idle')">
                <div class="pipe-label">{{ node.label }}</div>
                <div class="pipe-value">{{ node.value }}<span v-if="node.total">/{{ node.total }}</span></div>
                <div class="pipe-detail">{{ node.detail }}</div>
              </div>
            </div>
            <p class="hint" style="margin-top:12px">推送由定时调度消费队列：识别成功且有海报的影视入队后，等待队列检查（默认 30 秒）创建目录分享并推送；此处「推送入队」显示的是累积队列数，不代表本轮已全部发出。</p>
          </div>
          <div class="panel narrow">
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
            <div class="panel-foot"><el-button type="primary" :loading="mirrorSaving" @click="saveMirror">保存设置</el-button><el-button type="success" :loading="mirrorBusy" :disabled="mirrorBusy" @click="runMirror('auto')">立即同步</el-button><el-button :loading="mirrorBusy" :disabled="mirrorBusy" @click="runMirror('force')">强制入库识别</el-button><el-button :loading="mirrorBusy" :disabled="mirrorBusy" @click="runMirror('skip')">仅同步</el-button><el-button :loading="mirrorBusy" :disabled="mirrorBusy" @click="renameMirrorFiles(false)">按英文规范改名</el-button><el-button :loading="mirrorBusy" :disabled="mirrorBusy" @click="pollMirrorTasks">继续轮询任务</el-button></div>
          </div>
          <div class="panel narrow">
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
          <el-collapse v-model="mirrorDiagOpen" class="mirror-fold">
            <el-collapse-item name="diagnostics">
              <template #title>
                <span class="fold-title">问题诊断</span>
                <el-tag v-if="diagCount" type="warning" size="small" style="margin-left:8px">{{ diagCount }} 项待处理</el-tag>
                <span v-else class="fold-sub">无待处理项</span>
              </template>
          <div v-if="mirrorStatus.missing_dirs?.length" class="panel">
            <div class="panel-head"><h3>待创建目录</h3><el-tag type="warning" size="small">{{ mirrorStatus.missing_dirs.length }} 个</el-tag></div>
            <div class="table-scroll"><el-table :data="mirrorStatus.missing_dirs" size="small">
              <el-table-column prop="name" label="B 库应建目录名" min-width="200"/>
              <el-table-column prop="kind" label="类别" width="100"/>
              <el-table-column prop="parent_id" label="父目录 ID" min-width="180"/>
              <el-table-column label="失败原因 / 处理建议" min-width="320"><template #default="s"><span style="color:#c0392b">{{ s.row.error || s.row.tip }}</span></template></el-table-column>
            </el-table></div>
            <p class="hint">这里显示的是 create_dir 接口的真实报错（或「自动创建已关闭」），不再用通用文案掩盖。v0.8.3 起主目录直接用 ·（网盘拒绝 * 返回 code=141）；只有连 · 也拒绝时，才会降级为无标记标准名重试一次。相关作品标记 blocked 不是失败，修正后重新同步即可。</p>
          </div>
          <div v-if="mirrorStatus.counts?.dirs_pruned_zombie" class="panel">
            <div class="panel-head"><h3>已清理待重建目录映射</h3><el-tag type="warning" size="small">{{ mirrorStatus.counts.dirs_pruned_zombie }} 条</el-tag><el-button size="small" type="danger" plain :loading="mirrorBusy" :disabled="mirrorBusy" @click="clearPrunedMappings">清空已清理映射</el-button></div>
            <p class="hint">这些是历史「清理无效映射」留下的僵尸行：B 库目录已不存在、映射已清，下轮同步会从 A 库自动重建，<b>不需要也不应该手工建目录</b>。旧版本无标记的行现在同样识别；确认后点「清空已清理映射」即可从清单彻底移除，不影响资源 / 分享 / 队列。</p>
          </div>
          <div v-if="mirrorStatus.file_errors?.length" class="panel">
            <div class="panel-head"><h3>文件复制失败 / 待重试</h3><el-tag type="danger" size="small">{{ mirrorStatus.file_errors.length }} 个</el-tag></div>
            <div class="table-scroll"><el-table :data="mirrorStatus.file_errors" size="small">
              <el-table-column prop="work" label="作品" min-width="160"/>
              <el-table-column prop="file" label="文件名" min-width="200"/>
              <el-table-column prop="task_id" label="任务 ID" min-width="140"/>
              <el-table-column label="失败原因" min-width="320"><template #default="s"><span style="color:#c0392b">{{ s.row.error || '未记录原因' }}</span></template></el-table-column>
            </el-table></div>
            <p class="hint">这里显示 copy_file / get_task_status 的真实报错，已从数据库持久化，刷新页面仍在。v0.8.4 起失败任务会标 status=3 或 detail.code≠0 为明确失败（旧版误记为「待确认」）；<b>下一轮「立即同步」会自动重试这些文件</b>，重试前会先核对 B 库，确认已存在的不会重复复制。若全部报同一错误，请按「待验证接口」提示补抓一次网页端「复制到」的 HAR。</p>
          </div>
          <div v-if="mirrorStatus.untranslated_works?.length || mirrorStatus.rename_errors?.length" class="panel">
            <div class="panel-head"><h3>英文改名待处理 / 失败</h3><el-tag type="warning" size="small">{{ mirrorStatus.untranslated_works?.length || 0 }} 部待翻译 · {{ mirrorStatus.rename_errors?.length || 0 }} 个失败</el-tag><el-tag v-if="mirrorStatus.counts?.files_rename_skipped_legacy" type="info" size="small">历史不回刷 {{ mirrorStatus.counts.files_rename_skipped_legacy }} 个</el-tag></div>
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
            <div class="panel-foot"><el-button :loading="mirrorBusy" :disabled="mirrorBusy" @click="renameMirrorFiles(true)">重新解析英文名并改名</el-button><el-button v-if="mirrorStatus.untranslated_works?.length" :loading="mirrorBusy" :disabled="mirrorBusy" plain @click="clearUntranslated">清空待处理清单</el-button></div>
            <p class="hint">英文剧名按 TMDB en-US 官方片名优先、<b>查不到就直接直译</b>（AI 直译 → 本地拼音兜底），<b>每部作品只解析一次并缓存</b>；点「重新解析英文名并改名」会清缓存重查。v0.9.6.1 按你的口径改为：<b>旧版只改文件夹、不改单集的历史条目不再回刷</b>，升级前未完成改名的行标记「历史不回刷」，只有升级后新同步的文件自动改英文名；确需处理历史行时点「重新解析英文名并改名」显式解除。改名失败通常是网盘拒绝了文件名字符或 B 库 fileId 失效，修正后点「按英文规范改名」重试即可；<b>不会重新复制文件、不会动 A 库，也不影响入库与分享</b>。</p>
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
            <div class="panel-head"><h3>影视映射清单</h3><el-tag v-if="mirrorStatus.counts?.media_invalid" size="small" type="warning">{{ mirrorStatus.counts.media_invalid }} 条无效</el-tag><el-button size="small" type="danger" plain :loading="mirrorBusy" :disabled="mirrorBusy || !mirrorStatus.counts?.media_invalid" @click="clearInvalidMedia">清理无效影视映射</el-button></div>
            <div class="table-scroll"><el-table :data="mirrorStatus.media || []" size="small">
              <el-table-column prop="category_path" label="分类目录" min-width="140"/>
              <el-table-column prop="title" label="标准影视名" min-width="180"/>
              <el-table-column prop="mirror_name" label="B 库期望目录名" min-width="180"/>
              <el-table-column prop="media_type" label="类型" width="90"/>
              <el-table-column prop="files" label="文件" width="80"/>
              <el-table-column label="状态" width="120"><template #default="s"><el-tag v-if="s.row.invalid" type="warning" size="small">无效待清理</el-tag><span v-else>{{ s.row.status }}</span></template></el-table-column>
            </el-table></div>
            <p class="hint">B 库目录名里的 · 只存在于网盘（v0.8.3 起网盘拒绝 *）；资源库、分享标题与 Telegram 推送一律使用标准影视名。<b>状态为失败/阻断且没有任何已同步文件的历史遗留映射</b>可点「清理无效影视映射」移除；只删 A→B 映射登记，不删资源 / 分享 / 队列，A 库仍有文件时下轮同步会自动重建。</p>
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

          <!-- ===== 推送白名单 = 目录路由表（默认1条，+添加最多10条） ===== -->
          <div class="bot-card">
            <div class="bot-card-head"><span class="bot-ico">📺</span><h3>推送白名单（目录路由）</h3><span class="restrict-sub">不同 B 库目录 → 同一 Bot → 不同频道 / 群聊；默认 1 条，最多 10 条</span><el-tag size="small" style="margin-left:auto">{{ pushRoutes.length }} / {{ pushCfg.max_routes || 10 }}</el-tag></div>
            <div v-for="(r, i) in pushRoutes" :key="i" class="route-row">
              <div class="field"><label>目录 ID（影视库 B 的下属目录，留空=根目录全部）</label><el-input v-model="r.dir_id" placeholder="留空表示整个 B 库；填目录 fileId 则只推该目录及其子目录"/></div>
              <div class="field"><label>备注（选填，便于肉眼区分目录）</label><el-input v-model="r.note" maxlength="120" placeholder="如：国产剧 / 动画电影 / 欧美剧集"/></div>
              <div class="field"><label>频道 ID（可留空）</label><el-input v-model="r.channel_id" placeholder="频道 chat_id，如 -1001234567890；频道需 Bot 为管理员"/><el-button size="small" text :disabled="!r.channel_id || !pushCfg.bot" @click="testPushRoute(r, r.channel_id)">测试频道</el-button></div>
              <div class="field"><label>群 ID（可留空）</label><el-input v-model="r.group_id" placeholder="群 chat_id，如 -1001234567890；群只需 Bot 已加入"/><el-button size="small" text :disabled="!r.group_id || !pushCfg.bot" @click="testPushRoute(r, r.group_id)">测试群</el-button></div>
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
              <div class="panel-head"><h3 style="font-size:14px">打赏（内置指令 /reward）</h3><span>收款码与按钮文案；与上方 Bot / 白名单 / 使用限制共用顶部统一保存按钮</span><el-switch v-model="interactionCfg.donation_enabled" style="margin-left:auto"/></div>
              <div class="form-grid">
                <div class="field"><label>打赏二维码 / 图片地址</label><el-input v-model="interactionCfg.donation_image_url" placeholder="https://..."/></div>
                <div class="field"><label>打赏按钮文字</label><el-input v-model="interactionCfg.donation_button_text" placeholder="打赏支持"/></div>
              </div>
              <div class="field"><label>打赏说明</label><el-input v-model="interactionCfg.donation_text" type="textarea" :rows="2"/></div>
              <div class="field"><label>赞赏码（图片 URL 或文字说明）</label><el-input v-model="rewardCfg.code" placeholder="如 https://.../reward.png 或文字说明"/></div>
              <p class="hint">赞赏码可插入模板的 <code>reward_code</code> 变量；关闭开关后 /reward 不再展示打赏入口。也可点下方按钮单独保存打赏。</p>
              <div class="panel-foot"><el-button size="small" type="primary" plain :loading="rewardSaving" @click="saveReward">单独保存打赏</el-button></div>
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
          <div class="sec-band"><span class="sec-key">b</span><div><h3>用户投稿 · 用户求片</h3><p>私聊 Bot 提交投稿与求片；求片关键词可一键同步到 TgtoDrive 监控规则</p></div><span class="sec-line"></span></div>
          <div class="two-col">
            <div class="panel">
              <div class="panel-head"><h3>用户求片（我想看）</h3></div>
              <div class="inline-form"><el-input v-model="wantTitle" placeholder="影视名称" style="width:220px"/><el-button type="primary" @click="addWant">加入监控</el-button></div>
              <div class="table-scroll"><el-table :data="mediaRequests" size="small">
                <el-table-column label="影视名称（TMDB 候选）" min-width="180"><template #default="s">
                  {{ s.row.title }}<span v-if="s.row.year" class="muted"> · {{ s.row.year }}</span>
                  <el-tag v-if="s.row.tmdb_id" size="small" type="info" effect="plain">#{{ s.row.tmdb_id }}</el-tag>
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
                <el-table-column label="标题（TMDB 候选）" min-width="170"><template #default="s">
                  {{ s.row.title }}<span v-if="s.row.year" class="muted"> · {{ s.row.year }}</span>
                  <el-tag v-if="s.row.tmdb_id" size="small" type="info" effect="plain">#{{ s.row.tmdb_id }}</el-tag>
                </template></el-table-column>
                <el-table-column label="用户" width="120"><template #default="s">{{ s.row.tg_username || s.row.tg_user_id || '—' }}</template></el-table-column>
                <el-table-column prop="status" label="状态" width="96"/>
                <el-table-column label="操作" width="160"><template #default="s">
                  <el-button size="small" type="primary" plain @click="handleContribution(s.row.id, 'approved')">通过</el-button>
                  <el-button size="small" @click="handleContribution(s.row.id, 'rejected')">拒绝</el-button>
                </template></el-table-column>
              </el-table></div>
            </div>
          </div>

          <!-- ===== c 板块：TgtoDrive 项目联动调用配置 ===== -->
          <div class="sec-band"><span class="sec-key">c</span><div><h3>TgtoDrive 项目联动</h3><p>求片 / 投稿关键词同步到 TgtoDrive 监控规则；仅配置联动地址与凭证，与 Bot Token、打赏无关</p></div><span class="sec-line"></span></div>
          <div class="panel narrow">
            <div class="panel-head"><h3>TgtoDrive 联动配置</h3><el-button :loading="tgtoTesting" size="small" @click="testTgto">测试连接（只读）</el-button></div>
            <div class="form-grid">
              <div class="field"><label>TgtoDrive 地址</label><el-input v-model="interactionCfg.tgto_base_url" placeholder="http://127.0.0.1:端口"/></div>
              <div class="field"><label>场景名</label><el-input v-model="interactionCfg.tgto_scene" placeholder="ENV_FILTER_GUANGYA"/></div>
              <div class="field"><label>OpenAPI Token（留空保持原值）</label><el-input v-model="interactionCfg.tgto_api_key" placeholder="已配置则留空" show-password/></div>
            </div>
            <p class="hint">关键词规则采用「读取全量 → 合并 → 回写 → 回读校验」，只新增/更新本项目幂等 id 规则，不覆盖原有关键词。投稿转存接口待抓包验证，当前只登记与审核。</p>
          </div>

          <p class="hint" style="text-align:right;margin:0">上述互动配置已并入页面顶部「保存分发与互动配置」统一保存，无需重复点击。</p>

        </section>

        <section v-else-if="tab === '资源库'" class="fade">
          <div class="page-head"><div><h2>资源库</h2><p>按作品聚合 · 整剧目录分享 · TMDB 识别 · 批量管理</p></div><div class="head-tools"><el-button size="small" type="primary" :loading="scanning" @click="scan">立即扫描</el-button><el-button size="small" :loading="rescanning" @click="rescanLibrary">重新扫描库</el-button><el-button size="small" @click="showScanPanel=!showScanPanel">{{ showScanPanel ? '收起扫描计划' : '扫描计划' }}</el-button><el-button size="small" :loading="finishChecking" @click="loadFinishCheck">完结校验</el-button><el-button size="small" :loading="reidentifying" @click="reidentifyAll">按目录重识别</el-button><el-button size="small" type="danger" plain @click="clearLibrary">清空扫描结果</el-button><el-input v-model="keyword" placeholder="搜索作品名" clearable style="width:200px"><template #prefix><svg class="ico" viewBox="0 0 24 24"><path :d="icon('search')"/></svg></template></el-input><el-select v-model="statusFilter" placeholder="分享状态" clearable style="width:120px"><el-option label="已创建" value="active"/><el-option label="未创建" value="not_created"/><el-option label="失败" value="failed"/><el-option label="已取消" value="cancelled"/></el-select><el-select v-model="identifyFilter" placeholder="识别状态" clearable style="width:120px"><el-option label="已识别" value="identified"/><el-option label="匹配存疑" value="ambiguous"/><el-option label="识别失败" value="failed"/><el-option label="无匹配" value="none"/><el-option label="未识别" value="unidentified"/></el-select></div></div>

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

          <!-- 作品视图：整部剧/电影一张海报卡，支持勾选批量管理 -->
          <div class="grid-toolbar"><label class="select-all"><input type="checkbox" :checked="allSelected" @change="toggleSelectAll"/> 全选本页（{{ filteredSeries.length }}）</label></div>
          <div class="media-grid">
            <div v-for="g in filteredSeries" :key="g.key" class="media-card series-card" :class="{picked:selectedSeries.includes(g.key)}" @click="toggleSelect(g.key)">
              <div class="media-poster">
                <el-image v-if="g.poster_path" :src="'https://image.tmdb.org/t/p/w342'+g.poster_path" fit="cover" style="width:100%;height:100%"/>
                <div v-else class="no-poster"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('image')"/></svg><span>{{ mediaLabel(g.media_type) || 'MEDIA' }}</span></div>
                <input type="checkbox" class="card-check" :checked="selectedSeries.includes(g.key)" @click.stop="toggleSelect(g.key)"/>
                <span v-if="g.share_status === 'active'" class="scrape-ok">✓</span>
                <span class="media-badge" :class="'badge-'+g.share_status">{{ g.share_status === 'active' ? '已分享' : (g.share_status === 'not_created' ? '未分享' : g.share_status) }}</span>
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
            <div class="panel-foot"><el-button type="primary" @click="saveTask">保存扫描计划</el-button><el-button :loading="scanning" @click="scan">立即扫描</el-button></div>
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

        <section v-else-if="tab === '审核中心'" class="fade">
          <div class="page-head"><div><h2>审核中心</h2><p>通过后的条目将自动建分享并进入推送队列</p></div><el-tag type="warning">{{ reviews.length }} 条待审</el-tag></div>
          <div class="panel">
            <el-table :data="reviews"><el-table-column prop="resource" label="资源名称" min-width="260"/><el-table-column prop="channel" label="目标频道"/><el-table-column prop="identified" label="识别状态" width="120"/><el-table-column label="操作" width="180"><template #default="s"><el-button type="success" size="small" @click="approve(s.row.id)">通过</el-button><el-button size="small" @click="reject(s.row.id)">拒绝</el-button></template></el-table-column></el-table>
            <div v-if="!reviews.length" class="empty big">没有待审核内容</div>
          </div>
        </section>

        <section v-else-if="tab === '推送队列'" class="fade">
          <div class="page-head"><div><h2>推送队列</h2><p>失败项按指数退避自动重试，可手动重置</p></div><div class="head-tools"><el-tag type="success">{{ overview.sent || 0 }} 已推送</el-tag><el-tag type="danger">{{ overview.failed || 0 }} 失败</el-tag></div></div>
          <div class="panel">
            <el-table :data="queue" stripe max-height="640"><el-table-column prop="id" label="ID" width="70"/><el-table-column prop="resource" label="资源" min-width="200"/><el-table-column prop="channel" label="频道"/><el-table-column prop="status" label="状态" width="100"><template #default="s"><span class="status-tag" :class="'st-'+s.row.status">{{ s.row.status }}</span></template></el-table-column><el-table-column prop="attempts" label="尝试" width="70"/><el-table-column prop="next_retry_at" label="下次重试"/><el-table-column prop="error" label="错误" min-width="220" show-overflow-tooltip/><el-table-column label="操作" width="90"><template #default="s"><el-button size="small" @click="retry(s.row.id)">重试</el-button></template></el-table-column></el-table>
            <div v-if="!queue.length" class="empty big">队列为空</div>
          </div>
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

          <!-- v0.9.8：跳转卡片与保存按钮合并为同一张 panel，按钮紧贴卡片，增强整体性 -->
          <div class="panel" style="margin-bottom:14px">
            <div class="panel-head"><h3>推送消息 · 跳转按钮</h3><span>留空则不启用对应的按钮与模板变量</span></div>
            <div class="two-col">
              <div class="panel narrow">
                <div class="panel-head"><h4>官方频道跳转</h4><el-tag size="small" :type="interactionCfg.tg_channel_link ? 'success' : 'info'">{{ interactionCfg.tg_channel_link ? '已启用' : '未启用' }}</el-tag></div>
                <el-input v-model="interactionCfg.tg_channel_link" placeholder="@官方频道 或 https://t.me/官方频道"/>
                <p class="hint">填值后推送消息追加「📢 官方频道」按钮，模板可用 <code>&#123;&#123;channel_link&#125;&#125;</code>。</p>
              </div>
              <div class="panel narrow">
                <div class="panel-head"><h4>交流群跳转</h4><el-tag size="small" :type="interactionCfg.tg_group_link ? 'success' : 'info'">{{ interactionCfg.tg_group_link ? '已启用' : '未启用' }}</el-tag></div>
                <el-input v-model="interactionCfg.tg_group_link" placeholder="@交流群 或 https://t.me/交流群"/>
                <p class="hint">填值后推送消息追加「👥 交流群」按钮，模板可用 <code>&#123;&#123;group_link&#125;&#125;</code>。</p>
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
  --el-color-primary:#5b5bd6;--el-border-radius-base:10px;--el-font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
}
*{box-sizing:border-box}
body{margin:0;background:radial-gradient(1200px 600px at 100% -10%,#eef0ff 0%,transparent 55%),radial-gradient(900px 500px at -10% 100%,#e9f8f5 0%,transparent 55%),var(--yg-bg);background-attachment:fixed;color:var(--yg-ink);font-family:var(--el-font-family);font-size:14px;-webkit-font-smoothing:antialiased}
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
.el-table th.el-table__cell{background:#fafbfe!important;color:#5b6484;font-weight:650;font-size:12px}
.el-input__wrapper,.el-select__wrapper{border-radius:10px;box-shadow:0 0 0 1px var(--yg-line) inset}
.el-button{border-radius:10px;font-weight:600}
.el-tag{border-radius:8px}
/* 二级数据卡：授权摘要 / 凭证条 / 分享会话，圆角阴影对齐主卡 */
.auth-summary-item,.credential-bar,.method-card,.kv-grid>div{border-radius:var(--yg-radius-sm)}
.auth-summary-item{box-shadow:var(--yg-shadow);border-color:var(--yg-line)}
/* 概览「最近操作」卡片标题图标块颜色随卡片语义微调 */
.stat-card .stat-bar i{opacity:.9}
.app-shell{display:flex;min-height:100vh}
.sidebar{width:238px;flex:none;background:linear-gradient(180deg,#252a4e 0%,#171b35 100%);color:#e9ecff;display:flex;flex-direction:column;position:sticky;top:0;height:100vh}
.brand{display:flex;align-items:center;gap:12px;padding:24px 20px 22px;border-bottom:1px solid rgba(255,255,255,.09)}
.brand-mark{width:38px;height:38px;border-radius:11px;background:linear-gradient(135deg,var(--yg-primary-2),#43c9bd);display:grid;place-items:center;font-weight:800;font-size:14px;letter-spacing:.5px;color:#fff;box-shadow:0 6px 16px rgba(91,91,214,.35)}
.brand-mark.lg{width:52px;height:52px;border-radius:15px;font-size:18px}
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
.dot{width:8px;height:8px;border-radius:50%;display:inline-block}.dot.on{background:#39d4b0;box-shadow:0 0 0 4px rgba(57,212,176,.14)}.dot.off{background:#f0a33c;box-shadow:0 0 0 4px rgba(240,163,60,.14)}
.main-wrap{flex:1;min-width:0}
.topbar{height:66px;background:rgba(255,255,255,.88);backdrop-filter:blur(10px);border-bottom:1px solid var(--yg-line);display:flex;align-items:center;justify-content:space-between;padding:0 28px;position:sticky;top:0;z-index:20}
.crumb{display:flex;align-items:center;gap:10px;color:var(--yg-muted);font-size:13px}.crumb b{color:#c8cfe3;font-weight:400}.crumb strong{color:var(--yg-ink);font-weight:700}
.top-actions{display:flex;align-items:center;gap:10px}.clock{font-variant-numeric:tabular-nums;color:var(--yg-muted);font-size:12px;margin-right:6px}
.top-actions .ico{margin-right:5px}
.content{max-width:1480px;margin:0 auto;padding:26px 28px 60px}
.fade{animation:fade .3s ease}@keyframes fade{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
.page-head{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:22px;gap:16px;flex-wrap:wrap;padding-bottom:16px;border-bottom:1px solid var(--yg-line)}
.page-head h2{margin:0 0 6px;font-size:23px;letter-spacing:-.4px;font-weight:750}.page-head p{margin:0 0 0 16px;color:var(--yg-muted);font-size:12px}
.head-tools{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.hero{border-radius:20px;background:linear-gradient(120deg,#4f4fc4 0%,#6b5ce0 52%,#3aa9c9 100%);color:#fff;padding:26px 30px;display:flex;justify-content:space-between;align-items:center;gap:20px;box-shadow:0 18px 40px rgba(91,91,214,.22);position:relative;overflow:hidden}
.hero:after{content:"";position:absolute;width:280px;height:280px;border-radius:50%;border:40px solid rgba(255,255,255,.07);right:-70px;top:-120px}
.hero-tag{font-size:10px;letter-spacing:2px;opacity:.75;margin-bottom:6px}.hero h2{margin:0 0 6px;font-size:26px}.hero p{margin:0;opacity:.82;font-size:12px}
.hero-actions{display:flex;align-items:center;gap:14px;z-index:1}.hero-pill{padding:6px 13px;border-radius:20px;font-size:12px;font-weight:600}.pill-on{background:rgba(57,212,176,.22);color:#b9f7e6}.pill-off{background:rgba(255,255,255,.18)}
.stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(168px,1fr));gap:15px;margin:20px 0}
.stat-card{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius-sm);padding:20px;box-shadow:var(--yg-shadow);position:relative;overflow:hidden;transition:.22s}
.stat-card:hover{transform:translateY(-4px);box-shadow:var(--yg-shadow-lg)}
.stat-icon{width:40px;height:40px;border-radius:12px;display:grid;place-items:center;margin-bottom:14px}
.stat-icon .ico{width:20px;height:20px}
.tone-indigo .stat-icon{background:#eeedff;color:#5b5bd6}.tone-teal .stat-icon{background:#e5f8f5;color:#19b3a6}.tone-amber .stat-icon{background:#fff3e2;color:#e29328}.tone-violet .stat-icon{background:#f2edff;color:#8b6cf0}.tone-rose .stat-icon{background:#ffedf0;color:#e5526b}.tone-slate .stat-icon{background:#eef1f7;color:#8a93aa}
.stat-num{font-size:30px;font-weight:780;line-height:1.1;font-variant-numeric:tabular-nums;letter-spacing:-.5px}.stat-label{color:var(--yg-muted);font-size:12px;margin-top:6px}
/* v0.9.8 #194 推送效果报表 */
.stats-cards{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:16px}
.stats-cards .stat-card{padding:14px}.stats-cards .stat-num{font-size:24px}
.chart-wrap{padding:6px 2px 0}
.chart-bars{display:flex;align-items:flex-end;gap:4px;height:150px;padding-bottom:20px;position:relative;border-bottom:1px solid var(--yg-line)}
.chart-col{flex:1;min-width:14px;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%}
.chart-stack{width:70%;max-width:26px;height:100%;display:flex;flex-direction:column;justify-content:flex-end}
.chart-stack i{display:block;width:100%;min-height:0}
.bar-sent{background:var(--yg-accent,#4f7cff);border-radius:3px 3px 0 0}
.bar-fail{background:#e05a5a;border-radius:3px 3px 0 0}
.chart-col small{font-size:10px;color:var(--yg-muted);margin-top:6px;white-space:nowrap;transform:rotate(-30deg);transform-origin:center}
.chart-legend{display:flex;gap:16px;justify-content:center;margin-top:14px;font-size:12px;color:var(--yg-muted)}
.chart-legend span{display:flex;align-items:center;gap:6px}.chart-legend i{width:10px;height:10px;border-radius:2px;display:inline-block}
.lg-sent{background:var(--yg-accent,#4f7cff)}.lg-fail{background:#e05a5a}
.empty-hint{padding:28px;text-align:center;color:var(--yg-muted);font-size:13px}
.unavailable-box{margin-top:16px;background:var(--yg-surface);border:1px dashed var(--yg-line);border-radius:var(--yg-radius-sm);padding:12px 14px}
.unavailable-box>b{font-size:12px;color:var(--yg-muted)}
.unavailable-row{display:flex;gap:12px;padding:6px 0;font-size:12px;border-top:1px solid var(--yg-line);margin-top:6px}
.unavailable-row span{font-weight:600;min-width:60px}.unavailable-row small{color:var(--yg-muted)}
/* v0.9.8 #195 镜像流程节点 + 进度 */
.sync-progress{display:flex;align-items:center;gap:12px;margin-bottom:16px;font-size:12px;color:var(--yg-muted)}
.sync-progress-track{flex:1;height:8px;background:var(--yg-line);border-radius:6px;overflow:hidden}
.sync-progress-track i{display:block;height:100%;background:var(--yg-accent,#4f7cff);border-radius:6px;transition:width .3s}
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
.stat-bar{height:4px;background:#f0f2f8;border-radius:4px;margin-top:16px;overflow:hidden}.stat-bar i{display:block;height:100%;width:42%;background:linear-gradient(90deg,var(--yg-primary),var(--yg-primary-2));border-radius:4px}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:18px}.three-col{display:grid;grid-template-columns:repeat(3,1fr);gap:18px}
.panel{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius);padding:22px 24px;box-shadow:var(--yg-shadow);margin-bottom:18px}
.panel.narrow{max-width:780px}.panel-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:18px;gap:12px}.panel-head h3{margin:0;font-size:16px;font-weight:720}.panel-head>span{font-size:11px;color:var(--yg-muted)}
.link-btn{border:0;background:transparent;color:var(--yg-primary);cursor:pointer;font-size:12px}
.check-row{display:flex;align-items:center;gap:12px;padding:11px 0;border-bottom:1px dashed var(--yg-line)}.check-row:last-child{border:0}
.check-dot{width:24px;height:24px;border-radius:8px;display:grid;place-items:center}.check-dot.ok{background:#e5f8f5;color:#19b3a6}.check-dot.no{background:#fff3e2;color:#e29328}.check-dot .ico{width:14px;height:14px}
.check-copy{flex:1}.check-copy b{display:block;font-size:13px}.check-copy small{color:var(--yg-muted);font-size:11px}
.check-state{font-size:11px;font-weight:600}.check-state.ok{color:#19b3a6}.check-state.no{color:#e29328}
.dist-row{display:flex;align-items:center;gap:10px;padding:8px 0}.dist-label{width:56px;font-size:12px;color:var(--yg-muted)}.dist-track{flex:1;height:8px;background:#f1f3f9;border-radius:8px;overflow:hidden}.dist-track i{display:block;height:100%;border-radius:8px;transition:width .3s}.dist-row b{width:24px;text-align:right;font-variant-numeric:tabular-nums}
.timeline .tl-row{display:flex;align-items:center;gap:12px;padding:9px 0;border-bottom:1px dashed var(--yg-line);font-size:12px}.timeline .tl-row:last-child{border:0}
.tl-dot{width:7px;height:7px;border-radius:50%;background:var(--yg-primary-2);flex:none}.tl-time{color:var(--yg-muted);font-variant-numeric:tabular-nums;min-width:70px}.tl-action{color:var(--yg-primary);min-width:150px}.tl-detail{color:var(--yg-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.tl-target{color:#8a93aa}
.empty{text-align:center;color:var(--yg-muted);padding:24px}.empty.big{padding:60px;font-size:13px}
.sub-head{margin:4px 0 12px;padding-left:10px;border-left:3px solid #4f7cff;color:var(--yg-muted);font-size:12px}
.connect-hero{background:linear-gradient(135deg,#f0f7ff,#f7fbff);border:1px solid #d5e6ff;margin-bottom:18px}
.connect-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
.connect-head h3{margin:0 0 4px;font-size:17px;color:#1b2b4b}
.connect-head p{margin:0;color:var(--yg-muted);font-size:12px}
.connect-steps{display:flex;gap:8px;margin:18px 0}
.cstep{flex:1;display:flex;align-items:center;gap:9px;padding:10px 12px;border-radius:10px;background:#fff;border:1px solid var(--yg-line);color:var(--yg-muted);font-size:12px}
.cstep b{width:22px;height:22px;border-radius:50%;background:#e8edf5;color:#8a97ad;display:flex;align-items:center;justify-content:center;font-size:12px;flex:none}
.cstep.active{border-color:#4f7cff;color:#2a5bd7}.cstep.active b{background:#4f7cff;color:#fff}
.cstep.done{border-color:#19b3a6;color:#12897f}.cstep.done b{background:#19b3a6;color:#fff}
.connect-form{display:flex;flex-direction:column;gap:10px;max-width:560px}
.connect-actions{display:flex;gap:10px}
.connect-done .el-result{padding:6px 0}
.connect-foot{display:flex;gap:20px;margin-top:14px;font-size:12px;color:var(--yg-muted);flex-wrap:wrap;align-items:center}
.credential-bar{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px;padding:16px 18px;border-radius:14px;background:linear-gradient(135deg,#f2fbf9,#f7f9fe);border:1px solid #d8efe9}
.cred-cell small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:7px}
.cred-cell b{font-size:13px;word-break:break-all}
.credential-bar .good{color:#128f84}.credential-bar .bad{color:var(--yg-rose)}.credential-bar .warn{color:#d8861a}
.dot.live{background:#19b3a6;box-shadow:0 0 0 4px rgba(25,179,166,.15);margin-right:6px;animation:pulseDot 1.8s ease-in-out infinite}
@keyframes pulseDot{0%,100%{opacity:1}50%{opacity:.45}}
.auth-summary{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}.auth-summary-item{background:#fff;border:1px solid var(--yg-line);border-radius:14px;padding:15px 18px}.auth-summary-item small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:6px}.auth-summary-item b{font-size:14px;word-break:break-all}.auth-summary-item .good{color:#19b3a6}.auth-summary-item .bad{color:var(--yg-rose)}
.method-card{background:#fff;border:1px solid var(--yg-line);border-radius:var(--yg-radius);padding:22px;box-shadow:var(--yg-shadow);margin-bottom:18px}
.method-top{display:flex;gap:14px;align-items:center;margin-bottom:16px}.method-top h3{margin:0 0 4px;font-size:16px}.method-top p{margin:0;color:var(--yg-muted);font-size:12px}
.method-icon{width:42px;height:42px;border-radius:12px;display:grid;place-items:center;flex:none}.method-icon.indigo{background:#eeedff;color:#5b5bd6}.method-icon.teal{background:#e5f8f5;color:#19b3a6}.method-icon .ico{width:21px;height:21px}
.steps{margin:0 0 18px;padding-left:20px;color:#5b6484;font-size:12px;line-height:2}.steps li::marker{color:var(--yg-primary)}
.method-foot{display:flex;justify-content:space-between;align-items:center;border-top:1px dashed var(--yg-line);padding-top:16px}
.device-panel{border-color:#cfd0f8;background:linear-gradient(180deg,#fafaff,#fff)}
.pulse{color:var(--yg-primary);animation:blink 1.4s infinite}@keyframes blink{50%{opacity:.35}}
.device-code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:30px;letter-spacing:6px;text-align:center;background:#f5f5ff;border:1px dashed #c9c9f5;border-radius:12px;padding:18px;margin-bottom:16px;color:#3b3b8f}
.webqr-box{display:flex;justify-content:center;padding:14px;background:#fff;border:1px solid var(--yg-line);border-radius:12px;margin-bottom:14px}.webqr-box img{width:240px;height:240px;image-rendering:pixelated}
.webqr-code{text-align:center;font-size:14px;color:var(--yg-muted);margin-bottom:14px}.webqr-code b{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:18px;letter-spacing:2px;color:#3b3b8f}
.device-actions{display:flex;gap:12px;justify-content:center;align-items:center}.device-link{color:var(--yg-primary);text-decoration:none;font-size:13px;display:flex;align-items:center;gap:5px}
.kv-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.kv-grid small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:5px}.kv-grid b{font-size:13px;word-break:break-all}
.hint{color:#929bb5;font-size:11px;line-height:1.8;margin:12px 0 0}
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.field label{display:block;font-size:12px;font-weight:600;margin-bottom:7px}.field small{display:block;color:var(--yg-muted);font-size:11px;margin-top:6px}.field.inline{display:flex;align-items:center;gap:14px}.field.inline label{margin:0}
.panel-foot{display:flex;gap:10px;margin-top:22px;padding-top:18px;border-top:1px dashed var(--yg-line);flex-wrap:wrap}
/* v0.9.6：镜像锁占用提示——后台改名复查静默持锁时，让用户知道为何「立即同步」会 409 */
.mirror-lock-tip{display:flex;align-items:center;gap:10px;margin-top:16px;padding:10px 14px;background:#fff8e6;border:1px solid #f5d98a;border-radius:10px;color:#8a6d1f;font-size:12px;line-height:1.7}
/* v0.9.0：镜像同步页分区治理——状态数字按语义分组，配置/接口改成结构化 chip */
.mirror-fold{margin-top:16px;border-top:1px solid var(--yg-line);border-bottom:0}
.mirror-fold .el-collapse-item__header{font-size:13px;font-weight:650;color:#3b4361;min-height:42px;height:auto;padding:10px 4px}
.mirror-fold .el-collapse-item__content{padding-bottom:10px}
.fold-title{font-size:13px;font-weight:650;color:#3b4361;display:inline-flex;align-items:center;gap:8px}
.fold-title::before{content:"";width:18px;height:18px;border-radius:6px;flex:none;background:linear-gradient(135deg,#8271ef,#5b5bd6);background-image:radial-gradient(circle at 50% 50%,#fff 0 2px,transparent 2.4px)}
.fold-sub{margin-left:8px;font-size:11px;color:#929bb5;font-weight:400}
/* 状态分组：标题 + 网格，一组一类，不再一屏 15 个数字 */
.stat-group{margin-bottom:16px}.stat-group:last-child{margin-bottom:0}
.stat-group-title{display:flex;align-items:center;gap:8px;font-size:12px;font-weight:700;color:#4b5270;margin:0 0 10px}
.stat-group-title .grp-ico{width:22px;height:22px;border-radius:7px;display:grid;place-items:center;font-size:11px;background:#eef1fb;color:#5b5bd6;flex:none}
.stat-group-title .grp-ico.g-teal{background:#e5f8f5;color:#12897f}
.stat-group-title .grp-ico.g-amber{background:#fff3e2;color:#c47b12}
.stat-group-title .grp-ico.g-violet{background:#f2edff;color:#8b6cf0}
.mini-stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:10px}
.mini-stat{background:#fafbfe;border:1px solid var(--yg-line);border-radius:11px;padding:11px 13px;transition:.16s}
.mini-stat:hover{background:#fff;border-color:#d9dcf5}
.mini-stat small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:6px;line-height:1.3}
.mini-stat b{font-size:18px;font-variant-numeric:tabular-nums;font-weight:750}
/* 生效配置 / 接口清单：结构化 chip 行，替代长串小字 */
.info-chips{display:flex;flex-wrap:wrap;gap:8px 10px;margin:2px 0 0}
.info-chip{display:inline-flex;align-items:center;gap:6px;background:#f6f7fb;border:1px solid var(--yg-line);border-radius:9px;padding:6px 11px;font-size:12px;color:#4b5270}
.info-chip b{font-weight:700;color:var(--yg-ink)}
.info-chip.on b{color:#12897f}.info-chip.off b{color:var(--yg-rose)}
.endpoint-list{display:flex;flex-wrap:wrap;gap:6px;margin:6px 0 0}
.endpoint-list code{font-size:11px;background:#eef1fb;color:#4b5270;border:1px solid #e2e6f5;border-radius:6px;padding:3px 8px;word-break:break-all}
/* v0.9.3：分发与互动中心三板块标题（a / b / c） */
.sec-band{display:flex;align-items:center;gap:12px;margin:22px 0 14px}
.sec-band .sec-key{display:inline-grid;place-items:center;width:30px;height:30px;border-radius:9px;background:linear-gradient(135deg,#5b5bd6,#3b3b8f);color:#fff;font-weight:800;font-size:15px;flex:none}
.sec-band h3{margin:0;font-size:16px;font-weight:750;color:var(--yg-ink)}
.sec-band p{margin:2px 0 0;font-size:11px;color:var(--yg-muted)}
.sec-band .sec-line{flex:1;height:1px;background:var(--yg-line)}
/* v0.9.0：Bot 交互中心卡片（参考 embyhub） */
.bot-card{background:#fff;border:1px solid var(--yg-line);border-radius:16px;padding:22px 24px;margin-bottom:16px;box-shadow:0 2px 12px rgba(28,36,72,.04)}
.bot-card-head{display:flex;align-items:center;gap:12px;margin-bottom:18px}
.bot-card-head h3{font-size:16px;margin:0;font-weight:700}
.bot-ico{display:inline-flex;align-items:center;justify-content:center;width:38px;height:38px;border-radius:10px;background:#eef1fb;color:#4b5270;font-size:16px;font-weight:700}
.restrict-sub{font-size:12px;color:#929bb5}
.cmd-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.cmd-item{display:flex;align-items:center;gap:12px;background:#f6f7fb;border:1px solid #eef0f6;border-radius:11px;padding:12px 14px}
.cmd-chip{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:13px;color:#3b5bdb;background:#eaf0ff;border:1px solid #d6e0ff;border-radius:7px;padding:4px 10px;white-space:nowrap}
.cmd-desc{font-size:12px;color:#5a6178;flex:1}
.cmd-add{display:flex;gap:10px;flex-wrap:wrap;margin-top:16px;padding-top:16px;border-top:1px dashed var(--yg-line)}
.restrict-card{background:linear-gradient(180deg,#f4fbf6,#fff);border-color:#d8efdd}
.restrict-card .bot-ico{background:#e3f6e8;color:#1f8a4c}
.restrict-row{display:flex;align-items:center;gap:14px;padding:12px 0;border-bottom:1px dashed #e2f0e6}
.restrict-row.col{flex-direction:column;align-items:stretch;gap:8px}
.restrict-row:last-of-type{border-bottom:0}
.restrict-label b{display:block;font-size:12px;color:#2f6b45;margin-bottom:4px}
.restrict-label small{display:block;font-size:11px;color:#6f9279;line-height:1.7}
.restrict-row .unit{font-size:12px;color:#2f6b45}
.hint.warn{color:#b8860b}
.cursor-box{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}.cursor-box small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:5px}.cursor-box b{font-size:18px}
.view-switch{display:flex;border:1px solid var(--yg-line);border-radius:9px;overflow:hidden}.view-switch button{border:0;background:#fff;padding:7px 10px;cursor:pointer;color:#9aa3bd}.view-switch button.on{background:#eeedff;color:var(--yg-primary)}
.media-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(172px,1fr));gap:14px}
/* v0.6.4：参考 STRM 刮削海报墙——紧凑竖版卡片、统一 2:3 海报、状态角标 */
.series-card{border-radius:11px}.series-card .media-poster{height:auto;aspect-ratio:2/3;background:#f0f2f8}
.series-card .media-body{padding:9px 10px}.series-card .media-body b{font-size:12px}.series-card .media-body small{font-size:10px;color:var(--yg-muted);margin-top:3px}
.series-card .media-meta{flex-wrap:wrap;gap:4px 8px;margin-top:6px}.series-card .media-actions{padding:0 9px 10px}.series-card .media-actions .el-button{margin:0 2px 4px 0;padding:5px 8px;font-size:11px}
.series-card .media-badge{border-radius:6px;padding:3px 7px;font-size:9px}
.scrape-ok{position:absolute;top:8px;left:8px;width:20px;height:20px;border-radius:50%;background:#19b3a6;color:#fff;font-size:12px;font-weight:800;display:grid;place-items:center;box-shadow:0 2px 8px rgba(0,0,0,.2)}
.ep-count{position:absolute;bottom:6px;right:6px;padding:2px 7px;border-radius:10px;background:rgba(20,24,40,.75);color:#fff;font-size:9px;font-weight:700}
.head-tools{flex-wrap:wrap;gap:6px 8px;justify-content:flex-end}
.schedule-controls{align-items:center;gap:8px}.schedule-label{color:var(--yg-muted);font-size:11px;white-space:nowrap}
.media-card{background:#fff;border:1px solid var(--yg-line);border-radius:14px;overflow:hidden;box-shadow:var(--yg-shadow);transition:.2s}.media-card:hover{transform:translateY(-3px);box-shadow:0 16px 34px rgba(28,36,66,.12)}
.media-poster{height:270px;position:relative;background:#eef0f6}.no-poster{height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:8px;color:#b4bcd2}.no-poster .ico{width:30px;height:30px}.no-poster span{font-size:11px;letter-spacing:1px}
.media-badge{position:absolute;top:10px;left:10px;padding:4px 9px;border-radius:20px;font-size:10px;font-weight:700;color:#fff;background:#98a1b8}.badge-active{background:#19b3a6}.badge-not_created{background:#e29328}.badge-failed{background:#e5526b}.badge-cancelled{background:#98a1b8}
.series-card .media-badge{left:auto;right:10px}.scrape-ok{position:absolute;top:10px;left:10px;width:22px;height:22px;border-radius:50%;background:#19b3a6;color:#fff;font-size:13px;font-weight:800;display:grid;place-items:center;box-shadow:0 2px 8px rgba(0,0,0,.2)}.ep-count{position:absolute;bottom:8px;right:8px;padding:3px 8px;border-radius:12px;background:rgba(20,24,40,.78);color:#fff;font-size:10px;font-weight:700}
.media-body{padding:12px 13px}.media-body b{display:block;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.media-body small{display:block;color:var(--yg-muted);font-size:10px;margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.media-meta{display:flex;gap:10px;margin-top:8px;color:var(--yg-primary);font-size:10px}
.media-actions{display:flex;gap:5px;padding:0 12px 12px;flex-wrap:wrap}
.step-panel .step-head{display:flex;align-items:center;gap:10px;margin-bottom:16px}.step-head span{width:28px;height:28px;border-radius:9px;background:#eeedff;color:var(--yg-primary);display:grid;place-items:center;font-size:12px;font-weight:800}.step-head h3{margin:0;font-size:14px}
/* v0.6.2：分发规则三卡片管理布局 */
.manage-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;align-items:start}.manage-card{margin-bottom:0;min-width:0}.manage-card .panel-head h3{font-size:15px}.manage-card .panel-head .hint{display:block;font-size:11px;color:var(--yg-muted);margin-top:3px}.manage-card .el-table{font-size:12px}.manage-card .el-table .el-button+.el-button{margin-left:2px}
.mini-row{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:9px 0;border-bottom:1px dashed var(--yg-line);font-size:11px}.mini-row:last-child{border:0}.mini-row b{font-size:12px;flex:none}.mini-row>span:not(.row-actions){color:var(--yg-muted);max-width:45%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.row-actions{display:flex;gap:4px;flex:none;max-width:none!important;overflow:visible!important}
.row-btn{border:1px solid var(--yg-line);background:#fff;border-radius:6px;padding:3px 7px;font-size:10px;color:var(--yg-primary);cursor:pointer;white-space:nowrap}
.row-btn:hover{background:#eeedff}.row-btn.danger{color:var(--yg-rose)}.row-btn.danger:hover{background:#ffedf0}.row-btn:disabled{opacity:.5;cursor:wait}
.status-tag{padding:3px 9px;border-radius:14px;font-size:11px;font-weight:600;background:#eef1f7;color:#7e88a8}.st-sent{background:#e5f8f5;color:#129688}.st-pending{background:#fff3e2;color:#cf7f1c}.st-review{background:#f2edff;color:#7a55dd}.st-retry{background:#ffedf0;color:#d63f59}.st-failed{background:#f1f2f6;color:#7e88a8}
.tpl-layout{align-items:start}.var-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:18px}.var-grid button{border:1px solid var(--yg-line);background:#fafbff;border-radius:8px;padding:8px 4px;font-size:11px;color:var(--yg-primary);cursor:pointer}.var-grid button:hover{background:#eeedff}
/* v0.9.6：模板列表项可点编辑 + 删除；名称区域可点 */
.tpl-mini{display:flex;align-items:center;justify-content:space-between;gap:8px}.tpl-mini-name{display:inline-flex;align-items:center;gap:8px;cursor:pointer;flex:1;min-width:0}.tpl-mini-name:hover b{color:var(--yg-primary)}
.preview-box{border:1px dashed var(--yg-line);border-radius:12px;padding:14px;background:#fafbff}.preview-title{font-size:11px;color:var(--yg-muted);margin-bottom:8px}.preview-body{font-size:13px;white-space:pre-wrap;line-height:1.7;min-height:60px}
.mt{margin-top:12px}.ml{margin-left:18px}
.login-screen{min-height:100vh;display:grid;place-items:center;background:linear-gradient(135deg,#eef0fb 0%,#f7f9fd 60%,#eaf7f6 100%);position:relative;overflow:hidden}
.login-deco{position:absolute;border-radius:50%;filter:blur(2px)}.login-deco.d1{width:420px;height:420px;background:rgba(91,91,214,.10);top:-140px;left:-100px}.login-deco.d2{width:360px;height:360px;background:rgba(25,179,166,.11);bottom:-120px;right:-80px}
.login-card{width:400px;max-width:90vw;background:#fff;border:1px solid var(--yg-line);border-radius:22px;padding:34px;box-shadow:0 24px 70px rgba(40,48,90,.13);z-index:1}
.login-brand{display:flex;align-items:center;gap:14px}.login-brand h1{margin:0;font-size:24px;letter-spacing:-.5px}.login-brand p{margin:3px 0 0;color:var(--yg-muted);font-size:12px}
.login-divider{height:1px;background:var(--yg-line);margin:26px 0}.login-card h3{margin:0 0 5px;font-size:18px}.login-hint{color:var(--yg-muted);font-size:12px;margin:0 0 18px}
.login-btn{width:100%;margin-top:16px}.login-foot{display:flex;align-items:center;gap:7px;justify-content:center;color:#9aa3bd;font-size:11px;margin-top:20px}
.type-chip{padding:2px 7px;border-radius:10px;font-size:10px;font-weight:700;color:#fff;background:#98a1b8}
.type-tv{background:#8b6cf0}.type-movie{background:#5b5bd6}
.identify-chip{color:#d8861a;background:#fff3e2;padding:2px 7px;border-radius:10px;font-size:10px}
.match-path{background:#f2f6ff;border:1px solid #dbe6ff;border-radius:10px;padding:12px 14px;margin-bottom:14px}
.match-path small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:5px}
.match-path b{font-size:13px;word-break:break-all}
.dir-clue{display:block;margin-top:3px;color:#6b74a8;font-size:11px;word-break:break-all}
.match-controls{display:flex;gap:10px}
.match-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(215px,1fr));gap:12px;max-height:46vh;overflow-y:auto;margin-top:12px}
.match-card{display:flex;gap:10px;border:1px solid var(--yg-line);border-radius:12px;padding:10px;cursor:pointer;background:#fff;transition:.18s}
.match-card:hover{border-color:#a9a4f5;transform:translateY(-2px)}
.match-card.on{border-color:var(--yg-primary);box-shadow:0 0 0 2px rgba(91,91,214,.18);background:#fafaff}
.match-poster{width:68px;height:96px;flex:none;border-radius:8px;overflow:hidden;background:#eef0f6}
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
.ai-result{margin-top:10px;background:#f7f8fc;border:1px solid var(--yg-line);border-radius:10px;padding:10px}
.ai-result pre{margin:0;font-size:11px;white-space:pre-wrap;word-break:break-all;color:#4a5578}
/* ============ v0.7.0：状态栏 / 批量管理 / 角标 ============ */
.status-tabs{display:flex;flex-wrap:wrap;gap:12px 20px;align-items:center;margin-bottom:14px;padding:10px 14px;background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:12px}
.status-tab-group{display:flex;align-items:center;gap:5px;flex-wrap:wrap}
.stab-label{font-size:11px;color:var(--yg-muted);margin-right:2px}
.status-tab-group button{border:1px solid transparent;background:#f4f6fb;color:#6b74a8;border-radius:16px;padding:5px 12px;font-size:12px;cursor:pointer;transition:.16s}
.status-tab-group button:hover{background:#eeedff;color:var(--yg-primary)}
.status-tab-group button.on{background:var(--yg-primary);color:#fff;box-shadow:0 4px 10px rgba(91,91,214,.22)}
.batch-bar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:14px;padding:10px 14px;background:linear-gradient(135deg,#eeedff,#f6f4ff);border:1px solid #d9d4ff;border-radius:12px;font-size:12px}
.batch-bar b{color:var(--yg-primary)}
.grid-toolbar{display:flex;align-items:center;margin:0 0 8px 2px;font-size:12px;color:var(--yg-muted)}
.select-all{display:flex;align-items:center;gap:6px;cursor:pointer;user-select:none}
.select-all input,.card-check{width:15px;height:15px;accent-color:var(--yg-primary);cursor:pointer}
.media-card.series-card{cursor:pointer;position:relative;transition:.18s}
.media-card.series-card.picked{border-color:var(--yg-primary);box-shadow:0 0 0 2px rgba(91,91,214,.25)}
.card-check{position:absolute;top:8px;left:8px;z-index:5;width:18px;height:18px;background:rgba(255,255,255,.9);border-radius:4px}
.finish-badge{position:absolute;bottom:8px;left:8px;padding:2px 7px;border-radius:9px;font-size:9px;font-weight:700;color:#fff;z-index:4}
.fin-ended{background:rgba(25,179,166,.92)}.fin-returning{background:rgba(242,163,60,.95)}
.dur-badge{position:absolute;top:8px;right:8px;padding:2px 7px;border-radius:9px;background:rgba(20,24,40,.72);color:#fff;font-size:9px;z-index:4}
.tg-extra-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:14px}
/* ============ v0.8.0：间隔按钮 / 镜像 / 互动 ============ */
.preset-row{display:flex;flex-wrap:wrap;gap:8px;margin:6px 0 4px}
.preset-btn{border:1px solid var(--yg-line);background:var(--yg-surface);color:var(--yg-ink);border-radius:8px;padding:6px 12px;font-size:12px;cursor:pointer;transition:.15s}
.preset-btn:hover{border-color:var(--yg-primary)}
.preset-btn.on{background:var(--yg-primary);border-color:var(--yg-primary);color:#fff;font-weight:600}
.inline-label{display:flex;align-items:center;gap:7px;cursor:pointer;font-weight:400}
.inline-form{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:14px}
@media(max-width:1100px){.sidebar{width:72px}.brand>div:last-child,.nav-group-label,.nav-text,.nav-badge,.sidebar-foot{display:none}.nav-item{justify-content:center;padding:12px}.content{padding:20px}.two-col,.three-col,.form-grid,.auth-summary,.kv-grid,.credential-bar,.manage-grid{grid-template-columns:1fr}}

/* ============ v0.6.5 手机 / 触屏适配 ============ */
/* ≤900px：侧栏从左侧竖排改为顶部横向导航，主区占满宽度 */
@media(max-width:900px){
  .app-shell{flex-direction:column}
  .sidebar{width:100%;height:auto;position:sticky;top:0;z-index:30;flex-direction:column;border-radius:0;box-shadow:0 4px 16px rgba(23,27,53,.18)}
  .brand{padding:10px 14px;border-bottom:1px solid rgba(255,255,255,.08)}
  .brand-mark{width:30px;height:30px;border-radius:9px;font-size:12px}
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
</style>
