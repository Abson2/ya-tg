<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

const password = ref(localStorage.getItem('ya_tg_password') || '')
const authed = ref(false)
const tab = ref('概览')
const clock = ref('')
const viewMode = ref('grid')
const keyword = ref('')
const statusFilter = ref('')

const overview = ref({}), task = ref({ cron: '*/30 * * * *', root_id: '', enabled: true })
const resources = ref([]), bots = ref([]), channels = ref([]), rules = ref([])
const reviews = ref([]), queue = ref([]), templates = ref([]), audit = ref([])
const auth = ref({}), shareStatus = ref({}), device = ref(null), pkceUrl = ref('')
const smsStatus = ref({}), smsPhone = ref(''), smsCode = ref(''), smsStep = ref(1), smsBusy = ref(false)
const tmdbStatus = ref({}), tmdbForm = ref({ api_key: '', bearer_token: '', language: 'zh-CN' }), tmdbTesting = ref(false), tmdbSaving = ref(false)
const aiStatus = ref({}), aiForm = ref({ enabled: false, base_url: '', api_key: '', model: '', prompt: '', timeout: 30 }), aiSaving = ref(false), aiTesting = ref(false)
const aiParseName = ref(''), aiParseResult = ref(null)
const typeFilter = ref(''), identifyFilter = ref('')
// v0.6.1：链路自检、Telegram 反代地址、批量重识别/立即推送状态
const diagnostics = ref({ blockers: [] }), telegramBase = ref(''), telegramSaving = ref(false)
const reidentifying = ref(false), publishing = ref(0), runningQueue = ref(false)
const matchTried = ref([]), matchTranslated = ref([]), testingBotMsg = ref(0)

async function loadTmdb() {
  try { tmdbStatus.value = await api('/tmdb/status') } catch (e) {}
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
function resetSms() { smsStep.value = 1; smsCode.value = '' }
const bot = ref({ name: '', token: '' })
const channel = ref({ bot_id: null, name: '', chat_id: '' })
const rule = ref({ name: '', directory_id: '', keyword: '', channel_id: null, auto_publish: true })
const tpl = ref({ name: '', content: '', enabled: true, is_default: false })

let pollTimer = null, clockTimer = null

const navGroups = [
  { label: '工作台', items: [{ key: '概览', icon: 'grid', label: '概览' }] },
  { label: '光鸭资源', items: [
    { key: '光鸭授权', icon: 'key', label: '光鸭授权' },
    { key: '扫描设置', icon: 'scan', label: '扫描设置' },
    { key: '资源库', icon: 'folder', label: '资源库' }
  ] },
  { label: '分发中心', items: [
    { key: '分发规则', icon: 'route', label: '分发规则' },
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
const templateVars = ['title', 'overview', 'filename', 'year', 'resolution', 'size', 'share_url',
  'tmdb_id', 'tmdb_url', 'type', 'type_label', 'origin_country', 'origin_country_code',
  'genres', 'tags', 'vote_average', 'original_title', 'season', 'episode', 'episode_tag', 'media_type']
const previewDemo = {
  title: '保镖恋人', overview: '一个西班牙富商的女儿，在纽约的生活因一次绑架发生彻底颠覆。',
  filename: '保镖恋人.2026.2160p.WEB-DL.mkv', year: '2026', resolution: '2160P',
  size: '14.9GB', share_url: 'https://share.example/xxx', tmdb_id: '1440098',
  tmdb_url: 'https://www.themoviedb.org/movie/1440098',
  type: 'movie', type_label: '电影', origin_country: '西班牙', origin_country_code: 'ES',
  genres: '爱情、惊悚', tags: '#保镖恋人 #电影 #西班牙 #爱情 #惊悚',
  vote_average: '7.0', original_title: 'Marfil', season: '', episode: '',
  episode_tag: '', media_type: 'movie'
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
async function api(path, opt = {}) {
  const r = await fetch('/api' + path, { ...opt, headers: { ...headers(), ...(opt.headers || {}) } })
  const d = await r.json().catch(() => ({}))
  if (!r.ok) throw Error(errText(d.detail ?? d.message, r.statusText))
  return d
}
async function load() {
  overview.value = await api('/overview')
  task.value = await api('/scan-task')
  resources.value = await api('/resources')
  bots.value = await api('/bots')
  channels.value = await api('/channels')
  rules.value = await api('/rules')
  reviews.value = await api('/review')
  queue.value = await api('/queue')
  templates.value = await api('/templates')
  audit.value = await api('/audit')
  try { diagnostics.value = await api('/diagnostics') } catch (e) { diagnostics.value = { blockers: [] } }
  try { const t = await api('/telegram/config'); telegramBase.value = t.base_url || t.default || '' } catch (e) {}
  await loadAuth()
  await loadTmdb()
  await loadAi()
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
  try { await api('/scan-task', { method: 'PUT', body: JSON.stringify(task.value) }); ElMessage.success('扫描计划已保存'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
async function scan() {
  try { const d = await api('/scan-now', { method: 'POST' }); ElMessage.success(`扫描完成：${d.files} 个文件（${d.resumed ? '断点续扫' : '全新扫描'}）`); await load() }
  catch (e) { ElMessage.error(e.message) }
}
async function addBot() {
  try { await api('/bots', { method: 'POST', body: JSON.stringify(bot.value) }); bot.value = { name: '', token: '' }; manageDialog.value = ''; ElMessage.success('Bot 已添加'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
async function addChannel() {
  try { await api('/channels', { method: 'POST', body: JSON.stringify(channel.value) }); channel.value = { bot_id: null, name: '', chat_id: '' }; manageDialog.value = ''; ElMessage.success('频道已添加'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
async function addRule() {
  if (!channels.value.length) { ElMessage.warning('请先在「频道管理」里添加一个频道'); return }
  if (!rule.value.channel_id) { ElMessage.warning('请选择「目标频道」'); return }
  if (!String(rule.value.name || '').trim()) { ElMessage.warning('请填写规则名称'); return }
  try {
    const d = await api('/rules', { method: 'POST', body: JSON.stringify(rule.value) })
    ElMessage.success(d.message || '规则已保存')
    rule.value = { name: '', directory_id: '', keyword: '', channel_id: rule.value.channel_id, auto_publish: true }
    manageDialog.value = ''
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
// v0.6.2：三个管理卡片的「新增」弹窗
const manageDialog = ref('')
function openManage(kind) {
  manageDialog.value = kind
  if (kind === 'bot') bot.value = { name: '', token: '' }
  if (kind === 'channel') channel.value = { bot_id: bots.value[0]?.id || null, name: '', chat_id: '' }
  if (kind === 'rule') rule.value = { name: '', directory_id: '', keyword: '', channel_id: channels.value[0]?.id || null, auto_publish: true }
}
// ---------- v0.6.1：独立启停、真实测试消息、立即推送、反代地址 ----------
async function toggleBot(b) {
  try {
    const d = await api(`/bots/${b.id}`, { method: 'PUT', body: JSON.stringify({ enabled: !b.enabled }) })
    ElMessage.success(d.enabled ? `Bot「${b.name}」已启用` : `Bot「${b.name}」已停用（其下频道暂停推送）`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function toggleChannel(c) {
  try {
    const d = await api(`/channels/${c.id}`, { method: 'PUT', body: JSON.stringify({ enabled: !c.enabled }) })
    ElMessage.success(d.enabled ? `频道「${c.name}」已启用` : `频道「${c.name}」已停用（其它频道不受影响）`)
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function sendBotTestMessage(b) {
  let chatId = ''
  try {
    const { value } = await ElMessageBox.prompt(
      '留空则自动发给该 Bot 下第一个频道；也可直接填频道 ID（如 -1001234567890）',
      `向 Bot「${b.name}」发送测试消息`, { inputPlaceholder: '可留空', inputValue: '' })
    chatId = value || ''
  } catch (e) { return }
  testingBotMsg.value = b.id
  try {
    const d = await api(`/bots/${b.id}/test-message`, { method: 'POST', body: JSON.stringify({ chat_id: chatId }) })
    if (d.ok) ElMessage.success(d.message + (d.message_id ? `（消息 #${d.message_id}）` : ''))
    else ElMessage.error(d.message)
  } catch (e) { ElMessage.error(e.message) }
  finally { testingBotMsg.value = 0 }
}
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
    if (d.detail?.length) {
      ElMessageBox.alert(d.detail.map(x => `频道 #${x.channel_id}：${x.error}`).join('\n'),
        `${base}（失败原因）`, { confirmButtonText: '知道了', type: 'warning' })
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
      '按目录剧名重新识别库中资源（含已识别项，用于修正误配）。升级后请先到「扫描设置」执行一次「立即扫描」补全目录线索。',
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
// ---------- Bot / 频道 / 规则：测试与删除 ----------
const testingBot = ref(0), testingChannel = ref(0)
async function testBot(b) {
  testingBot.value = b.id
  try {
    const d = await api(`/bots/${b.id}/test`, { method: 'POST' })
    if (d.ok) ElMessage.success(d.message)
    else ElMessage.error(d.message)
  } catch (e) { ElMessage.error(e.message) }
  finally { testingBot.value = 0 }
}
async function testChannel(c) {
  testingChannel.value = c.id
  try {
    const d = await api(`/channels/${c.id}/test`, { method: 'POST' })
    if (d.ok) ElMessage.success(d.message + (d.message_id ? `（消息 #${d.message_id}）` : ''))
    else ElMessage.error(d.message)
  } catch (e) { ElMessage.error(e.message) }
  finally { testingChannel.value = 0 }
}
async function delBot(b) {
  const chs = channels.value.filter(c => c.bot_id === b.id)
  const nRules = chs.reduce((n, c) => n + rules.value.filter(r => r.channel_id === c.id).length, 0)
  let msg = `确定删除 Bot「${b.name}」？`
  if (chs.length) msg += `\n将同时删除其下 ${chs.length} 个频道`
  if (nRules) msg += `、${nRules} 条关联分发规则`
  msg += '\n关联的待推送队列项也会被清理，此操作不可撤销。'
  try {
    await ElMessageBox.confirm(msg, '删除 Bot', { confirmButtonText: '确认删除', cancelButtonText: '取消', type: 'warning' })
    await api(`/bots/${b.id}`, { method: 'DELETE' })
    ElMessage.success('已删除 Bot' + (chs.length ? `及 ${chs.length} 个频道` : ''))
    await load()
  } catch (e) { if (e !== 'cancel' && e !== 'close') ElMessage.error(e.message) }
}
async function delChannel(c) {
  const n = rules.value.filter(r => r.channel_id === c.id).length
  let msg = `确定删除频道「${c.name}」？`
  if (n) msg += `\n将同时删除 ${n} 条引用该频道的分发规则`
  msg += '\n该频道的待推送队列项也会被清理，此操作不可撤销。'
  try {
    await ElMessageBox.confirm(msg, '删除频道', { confirmButtonText: '确认删除', cancelButtonText: '取消', type: 'warning' })
    await api(`/channels/${c.id}`, { method: 'DELETE' })
    ElMessage.success('频道已删除'); await load()
  } catch (e) { if (e !== 'cancel' && e !== 'close') ElMessage.error(e.message) }
}
async function delRule(r) {
  try {
    await ElMessageBox.confirm(`确定删除分发规则「${r.name}」？`, '删除规则',
      { confirmButtonText: '确认删除', cancelButtonText: '取消', type: 'warning' })
    await api(`/rules/${r.id}`, { method: 'DELETE' })
    ElMessage.success('规则已删除'); await load()
  } catch (e) { if (e !== 'cancel' && e !== 'close') ElMessage.error(e.message) }
}
async function createShare(row) {
  try {
    const { value } = await ElMessageBox.prompt('0=永久 · 86400=1天 · 604800=7天 · 2592000=30天', '创建光鸭分享', { inputValue: '0' })
    const d = await api(`/resources/${row.id}/share?duration=${value}`, { method: 'POST' })
    ElMessage.success(d.share_url ? '分享已创建并保存链接' : '分享已创建')
    await load()
  } catch (e) { if (e !== 'cancel') ElMessage.error(e.message) }
}
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
async function startDevice() {
  try { device.value = await api('/auth/device/start', { method: 'POST' }); startPolling(); ElMessage.success('请在光鸭 App 打开授权链接') }
  catch (e) { ElMessage.error(e.message) }
}
function startPolling() {
  clearInterval(pollTimer)
  pollTimer = setInterval(async () => {
    try {
      const d = await api('/auth/device/poll', { method: 'POST' })
      if (d.status === 'authorized') { clearInterval(pollTimer); device.value = null; ElMessage.success('授权成功'); await load() }
      else if (d.status !== 'pending') clearInterval(pollTimer)
    } catch (e) { clearInterval(pollTimer) }
  }, 4000)
}
async function startPkce() {
  try { const d = await api('/auth/pkce/start'); pkceUrl.value = d.auth_url; window.open(d.auth_url, '_blank') }
  catch (e) { ElMessage.error(e.message) }
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
    ElMessage.success('模板已保存'); await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function restoreDefaultTpl() {
  try {
    const d = await api('/templates/default', { method: 'POST' })
    tpl.value = { name: '默认图文模板', content: d.content, enabled: true, is_default: true }
    ElMessage.success('已载入 默认模板（含分类标签），保存后生效')
    await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function copyCode() {
  try { await navigator.clipboard.writeText(device.value.user_code); ElMessage.success('授权码已复制') }
  catch (e) { ElMessage.error('复制失败，请手动选择') }
}
function insertVar(key) { tpl.value.content = (tpl.value.content || '') + '{{' + key + '}}' }
function tick() {
  clock.value = new Date().toLocaleTimeString('zh-CN', { hour12: false })
  if (authed.value && smsStatus.value && smsStatus.value.logged_in) updateCountdown()
}

onMounted(() => { tick(); clockTimer = setInterval(tick, 1000); if (password.value) login() })
onUnmounted(() => { clearInterval(pollTimer); clearInterval(clockTimer) })
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
        <div class="foot-version">v0.6.1 · Asia/Shanghai</div>
      </div>
    </aside>

    <div class="main-wrap">
      <header class="topbar">
        <div class="crumb"><span>Ya-TG</span><b>/</b><strong>{{ tab }}</strong></div>
        <div class="top-actions">
          <span class="clock">{{ clock }}</span>
          <el-button size="small" plain @click="verify">验证连接</el-button>
          <el-button size="small" type="primary" plain @click="load">
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
                <template #extra><el-button type="primary" @click="resetSms">重新登录</el-button><el-button plain @click="refreshSms">立即刷新凭证</el-button></template>
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
            <div class="auth-summary-item"><small>用户 ID</small><b>{{ smsStatus.account || auth.sub || '—' }}</b></div>
            <div class="auth-summary-item"><small>过期时间</small><b>{{ fmtBj(smsStatus.expires_at) || auth.expires_at || '—' }}</b></div>
          </div>
          <div class="sub-head"><span>高级方式 · 直连光鸭开放平台（需自行申请应用凭证）</span></div>
          <div class="two-col">
            <div class="method-card">
              <div class="method-top"><span class="method-icon indigo"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('shield')"/></svg></span><div><h3>Device Code 授权</h3><p>适合 NAS / 无浏览器跳转场景，需报备出口 IP</p></div></div>
              <ol class="steps"><li>后端用 sign_secret 签名申请设备码</li><li>打开授权链接，在光鸭 App 确认</li><li>系统自动轮询并保存 Token</li></ol>
              <div class="method-foot"><el-tag :type="auth.device_supported ? 'success' : 'info'" size="small">{{ auth.device_supported ? '配置就绪' : '缺少配置' }}</el-tag><el-button type="primary" :disabled="!auth.device_supported" @click="startDevice">发起设备码授权</el-button></div>
            </div>
            <div class="method-card">
              <div class="method-top"><span class="method-icon teal"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('link')"/></svg></span><div><h3>Web OAuth 2.0 + PKCE</h3><p>浏览器跳转授权，需登记 redirect_uri</p></div></div>
              <ol class="steps"><li>生成 code_verifier 与 state</li><li>跳转光鸭授权页完成确认</li><li>回调换 Token 并丢弃 verifier</li></ol>
              <div class="method-foot"><el-tag :type="auth.pkce_supported ? 'success' : 'info'" size="small">{{ auth.pkce_supported ? '配置就绪' : '缺少配置' }}</el-tag><el-button :disabled="!auth.pkce_supported" @click="startPkce">打开授权页</el-button></div>
            </div>
          </div>
          <div v-if="device" class="panel device-panel">
            <div class="panel-head"><h3>请在光鸭 App 中授权</h3><span class="pulse">等待确认…</span></div>
            <div class="device-code">{{ device.user_code }}</div>
            <div class="device-actions"><el-button plain @click="copyCode"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('copy')"/></svg> 复制授权码</el-button><a :href="device.verification_uri_complete" target="_blank" class="device-link">打开授权页 <svg class="ico" viewBox="0 0 24 24"><path :d="icon('external')"/></svg></a></div>
            <p class="hint">轮询间隔 {{ device.interval }} 秒，授权完成后自动关闭本卡片。</p>
          </div>
          <div class="panel">
            <div class="panel-head"><h3>网页分享会话</h3><el-tag :type="shareStatus.configured ? 'success' : 'warning'" size="small">{{ shareStatus.configured ? '已配置' : '未配置' }}</el-tag></div>
            <div class="kv-grid"><div><small>来源</small><b>{{ shareStatus.source || '—' }}</b></div><div><small>DID</small><b>{{ shareStatus.did_set ? '已设置' : '未设置' }}</b></div><div><small>SMID</small><b>{{ shareStatus.smid_set ? '已设置' : '未设置' }}</b></div><div><small>刷新</small><el-button size="small" :disabled="!auth.has_refresh_token" @click="auth.flow==='sms' ? refreshSms() : refreshToken()">立即刷新</el-button></div></div>
            <p class="hint">{{ shareStatus.note }}</p>
          </div>
        </section>

        <section v-else-if="tab === '扫描设置'" class="fade">
          <div class="page-head"><div><h2>扫描设置</h2><p>五段式 Cron，固定 Asia/Shanghai（UTC+8）</p></div></div>
          <div class="panel narrow">
            <div class="form-grid">
              <div class="field"><label>Cron 表达式</label><el-input v-model="task.cron" placeholder="*/30 * * * *"/><small>例：*/10 * * * * 每 10 分钟；0 3 * * * 每天 03:00</small></div>
              <div class="field"><label>扫描根目录 ID</label><el-input v-model="task.root_id" placeholder="留空为根目录"/></div>
              <div class="field inline"><label>启用定时扫描</label><el-switch v-model="task.enabled"/></div>
              <div class="field inline"><label>时区</label><el-tag>Asia/Shanghai · UTC+8</el-tag></div>
            </div>
            <div class="panel-foot"><el-button type="primary" @click="saveTask">保存计划</el-button><el-button @click="scan"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('play')"/></svg> 立即扫描</el-button></div>
          </div>
          <div class="panel narrow">
            <div class="panel-head"><h3>断点与上次结果</h3><el-tag :type="task.last_status === 'success' ? 'success' : 'info'" size="small">{{ task.last_status || 'never' }}</el-tag></div>
            <div class="cursor-box"><div><small>游标状态</small><b>{{ task.cursor_status }}</b></div><div><small>已处理文件</small><b>{{ task.cursor_files }}</b></div><div><small>遍历目录</small><b>{{ task.cursor_dirs }}</b></div></div>
            <p class="hint">{{ task.last_message || '尚未执行扫描' }}{{ task.cursor_error ? ' · 错误：' + task.cursor_error : '' }}</p>
          </div>
        </section>

        <section v-else-if="tab === '资源库'" class="fade">
          <div class="page-head"><div><h2>资源库</h2><p>fileId 去重 · TMDB 识别 · 分享链接管理</p></div><div class="head-tools"><el-button size="small" :loading="reidentifying" @click="reidentifyAll">按目录重识别</el-button><el-input v-model="keyword" placeholder="搜索资源名或 TMDB 标题" clearable style="width:260px"><template #prefix><svg class="ico" viewBox="0 0 24 24"><path :d="icon('search')"/></svg></template></el-input><el-select v-model="statusFilter" placeholder="分享状态" clearable style="width:140px"><el-option label="已创建" value="active"/><el-option label="未创建" value="not_created"/><el-option label="失败" value="failed"/><el-option label="已取消" value="cancelled"/></el-select><el-select v-model="typeFilter" placeholder="媒体类型" clearable style="width:120px"><el-option label="电影" value="movie"/><el-option label="剧集" value="tv"/></el-select><el-select v-model="identifyFilter" placeholder="识别状态" clearable style="width:130px"><el-option label="已识别" value="identified"/><el-option label="匹配存疑" value="ambiguous"/><el-option label="识别失败" value="failed"/><el-option label="无匹配" value="none"/><el-option label="未识别" value="unidentified"/></el-select><div class="view-switch"><button :class="{on:viewMode==='grid'}" @click="viewMode='grid'"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('grid')"/></svg></button><button :class="{on:viewMode==='list'}" @click="viewMode='list'"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('file')"/></svg></button></div></div></div>
          <div v-if="viewMode === 'grid'" class="media-grid">
            <div v-for="r in filteredResources" :key="r.id" class="media-card">
              <div class="media-poster"><el-image v-if="r.poster_path" :src="'https://image.tmdb.org/t/p/w342'+r.poster_path" fit="cover" style="width:100%;height:100%"/><div v-else class="no-poster"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('image')"/></svg><span>{{ (r.ext||'FILE').toUpperCase() }}</span></div><span class="media-badge" :class="'badge-'+r.share_status">{{ r.share_status }}</span></div>
              <div class="media-body"><b :title="r.name">{{ r.media_title || r.name }}</b><small>{{ r.name }}</small><small v-if="r.path_hint" class="dir-clue">目录：{{ r.path_hint }}</small><div class="media-meta"><span v-if="r.media_type" class="type-chip" :class="'type-'+r.media_type">{{ mediaLabel(r.media_type) }}</span><span v-if="r.season && r.episode">S{{ String(r.season).padStart(2,'0') }}E{{ String(r.episode).padStart(2,'0') }}</span><span v-if="r.tmdb_id">TMDB #{{ r.tmdb_id }}</span><span v-if="r.identify_status && r.identify_status !== 'identified'" class="identify-chip">{{ identifyLabel(r.identify_status) }}</span><span v-if="r.revision">更新 {{ r.revision }} 次</span></div></div>
              <div class="media-actions"><el-button size="small" @click="createShare(r)">分享</el-button><el-button size="small" @click="identify(r,true)">自动识别</el-button><el-button size="small" text @click="identify(r,false)">手动</el-button><el-button size="small" type="primary" plain :loading="publishing === r.id" @click="publishResource(r)">立即推送</el-button></div>
            </div>
            <div v-if="!filteredResources.length" class="empty big">没有匹配的资源</div>
          </div>
          <div v-else class="panel">
            <el-table :data="filteredResources" stripe max-height="640">
              <el-table-column prop="name" label="资源名称" min-width="220" show-overflow-tooltip/>
              <el-table-column prop="media_title" label="TMDB 标题" min-width="170" show-overflow-tooltip/>
              <el-table-column label="类型" width="80"><template #default="s">{{ mediaLabel(s.row.media_type) || '—' }}</template></el-table-column>
              <el-table-column label="识别" width="100"><template #default="s">{{ identifyLabel(s.row.identify_status) }}</template></el-table-column>
              <el-table-column prop="share_status" label="分享状态" width="110"/>
              <el-table-column prop="revision" label="更新次数" width="90"/>
              <el-table-column label="操作" width="320"><template #default="s"><el-button size="small" @click="createShare(s.row)">分享</el-button><el-button size="small" @click="identify(s.row,true)">自动识别</el-button><el-button size="small" text @click="identify(s.row,false)">手动</el-button><el-button size="small" type="primary" plain :loading="publishing === s.row.id" @click="publishResource(s.row)">立即推送</el-button></template></el-table-column>
            </el-table>
          </div>
        </section>

        <section v-else-if="tab === '分发规则'" class="fade">
          <div class="page-head"><div><h2>分发规则</h2><p>Bot → 频道 → 路由规则，三步建立推送链路</p></div></div>
          <div class="panel" style="margin-bottom:14px">
            <div class="panel-head"><h3>链路自检</h3><el-button size="small" type="primary" :loading="runningQueue" @click="runQueue">立即推送队列</el-button></div>
            <div class="cursor-box"><div><small>启用 Bot</small><b>{{ diagnostics.bots?.enabled ?? 0 }} / {{ diagnostics.bots?.total ?? 0 }}</b></div><div><small>启用频道</small><b>{{ diagnostics.channels?.enabled ?? 0 }} / {{ diagnostics.channels?.total ?? 0 }}</b></div><div><small>启用规则</small><b>{{ diagnostics.rules_enabled ?? 0 }}</b></div><div><small>待推送</small><b>{{ diagnostics.queue?.pending ?? 0 }}</b></div><div><small>待审核</small><b>{{ diagnostics.queue?.review ?? 0 }}</b></div><div><small>已推送</small><b>{{ diagnostics.queue?.sent ?? 0 }}</b></div></div>
            <p v-if="diagnostics.blockers?.length" class="hint" style="color:#c0392b;margin:8px 0">待处理：{{ diagnostics.blockers.join('；') }}</p>
            <p v-else class="hint" style="margin:8px 0">链路完整：Bot / 频道 / 规则均已启用，队列每 30 秒自动推送一次；也可点「立即推送队列」马上验证。</p>
            <p v-if="diagnostics.last_error" class="hint" style="color:#c0392b;margin:8px 0">最近失败：{{ diagnostics.last_error }}</p>
            <div class="match-controls"><el-input v-model="telegramBase" placeholder="Telegram API 地址，默认 https://api.telegram.org（国内网络可填反代）"/><el-button :loading="telegramSaving" @click="saveTelegramBase">保存地址</el-button></div>
          </div>
          <div class="manage-grid">
            <!-- Bot 管理卡片 -->
            <div class="panel manage-card">
              <div class="panel-head"><div><h3>Bot 管理</h3><span class="hint">{{ bots.filter(b => b.enabled).length }} / {{ bots.length }} 启用</span></div><el-button type="primary" size="small" @click="openManage('bot')">+ 添加 Bot</el-button></div>
              <el-table :data="bots" size="small" empty-text="还没有 Bot，点右上角添加"><el-table-column label="名称" min-width="90"><template #default="s">{{ s.row.name }}</template></el-table-column><el-table-column label="状态" width="72"><template #default="s"><el-tag :type="s.row.enabled ? 'success' : 'info'" size="small">{{ s.row.enabled ? '启用' : '停用' }}</el-tag></template></el-table-column><el-table-column label="操作" min-width="210"><template #default="s"><el-button size="small" text :loading="testingBot === s.row.id" @click="testBot(s.row)">验证 Token</el-button><el-button size="small" text :loading="testingBotMsg === s.row.id" @click="sendBotTestMessage(s.row)">测试推送</el-button><el-button size="small" text @click="toggleBot(s.row)">{{ s.row.enabled ? '停用' : '启用' }}</el-button><el-button size="small" text type="danger" @click="delBot(s.row)">删除</el-button></template></el-table-column></el-table>
            </div>
            <!-- 频道管理卡片 -->
            <div class="panel manage-card">
              <div class="panel-head"><div><h3>频道管理</h3><span class="hint">{{ channels.filter(c => c.enabled).length }} / {{ channels.length }} 启用</span></div><el-button type="primary" size="small" @click="openManage('channel')">+ 添加频道</el-button></div>
              <el-table :data="channels" size="small" empty-text="还没有频道，点右上角添加"><el-table-column label="频道" min-width="90"><template #default="s">{{ s.row.name }}</template></el-table-column><el-table-column label="状态" width="72"><template #default="s"><el-tag :type="s.row.enabled ? 'success' : 'info'" size="small">{{ s.row.enabled ? '启用' : '停用' }}</el-tag></template></el-table-column><el-table-column label="操作" min-width="210"><template #default="s"><el-button size="small" text :loading="testingChannel === s.row.id" @click="testChannel(s.row)">测试推送</el-button><el-button size="small" text @click="toggleChannel(s.row)">{{ s.row.enabled ? '停用' : '启用' }}</el-button><el-button size="small" text type="danger" @click="delChannel(s.row)">删除</el-button></template></el-table-column></el-table>
            </div>
            <!-- 规则管理卡片 -->
            <div class="panel manage-card">
              <div class="panel-head"><div><h3>规则管理</h3><span class="hint">{{ rules.filter(r => r.enabled).length }} / {{ rules.length }} 启用</span></div><el-button type="primary" size="small" @click="openManage('rule')">+ 添加规则</el-button></div>
              <el-table :data="rules" size="small" empty-text="还没有规则，点右上角添加"><el-table-column label="规则名" min-width="90"><template #default="s">{{ s.row.name }}</template></el-table-column><el-table-column label="模式" width="72"><template #default="s"><el-tag :type="s.row.auto_publish ? 'success' : 'warning'" size="small">{{ s.row.auto_publish ? '自动' : '审核' }}</el-tag></template></el-table-column><el-table-column label="操作" min-width="210"><template #default="s"><el-button size="small" text type="danger" @click="delRule(s.row)">删除</el-button></template></el-table-column></el-table>
              <p class="hint" style="margin:10px 0 0">保存规则时会自动把已扫描资源补入分发队列；规则未启用时不参与推送。</p>
            </div>
          </div>

          <!-- v0.6.2：新增统一管理弹窗 -->
        <el-dialog v-model="manageDialog" width="460px" destroy-on-close>
          <template #header><h3 style="margin:0">{{ manageDialog === 'bot' ? '添加 Bot' : (manageDialog === 'channel' ? '添加频道' : '添加分发规则') }}</h3></template>
          <div v-if="manageDialog === 'bot'">
            <div class="field"><label>Bot 名称</label><el-input v-model="bot.name" placeholder="例如：我的机器人"/></div>
            <div class="field"><label>Bot Token</label><el-input v-model="bot.token" type="password" show-password placeholder="@BotFather 给的 token，加密存储"/></div>
          </div>
          <div v-if="manageDialog === 'channel'">
            <div class="field"><label>所属 Bot</label><el-select v-model="channel.bot_id" placeholder="选择 Bot" style="width:100%"><el-option v-for="b in bots" :key="b.id" :label="b.name" :value="b.id"/></el-select></div>
            <div class="field"><label>频道名称</label><el-input v-model="channel.name" placeholder="例如：影视发布"/></div>
            <div class="field"><label>频道 ID</label><el-input v-model="channel.chat_id" placeholder="形如 -1001234567890"/></div>
          </div>
          <div v-if="manageDialog === 'rule'">
            <div class="field"><label>规则名称</label><el-input v-model="rule.name" placeholder="例如：全部资源"/></div>
            <div class="field"><label>目标频道</label><el-select v-model="rule.channel_id" placeholder="选择频道" style="width:100%"><el-option v-for="c in channels" :key="c.id" :label="c.name" :value="c.id"/></el-select></div>
            <div class="field"><label>目录 ID（可空）</label><el-input v-model="rule.directory_id" placeholder="只推送该目录下的资源"/></div>
            <div class="field"><label>文件名关键词（可空）</label><el-input v-model="rule.keyword" placeholder="只推送文件名含此关键词的资源"/></div>
            <div class="field"><label>发布模式</label><el-switch v-model="rule.auto_publish" active-text="自动发布" inactive-text="人工审核"/></div>
          </div>
          <template #footer><el-button @click="manageDialog = ''">取消</el-button><el-button type="primary" @click="manageDialog === 'bot' ? addBot() : (manageDialog === 'channel' ? addChannel() : addRule())">保存</el-button></template>
        </el-dialog>
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
              <el-button type="primary" :loading="tmdbSaving" @click="saveTmdb">保存密钥</el-button>
              <el-button plain :loading="tmdbTesting" @click="testTmdb">测试连接</el-button>
            </div>
            <p class="hint" style="margin-top:14px">
              没有密钥？到 <a href="https://www.themoviedb.org/settings/api" target="_blank" class="device-link">TMDB 账号设置 → API</a> 免费申请，
              复制 <b>API Read Access Token</b> 粘贴到上面第一个框。
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
          <div class="page-head"><div><h2>消息模板</h2><p>支持双花括号变量与可嵌套条件块；有海报时自动发送图片</p></div><el-button plain @click="restoreDefaultTpl">载入 默认模板</el-button></div>
          <div class="two-col tpl-layout">
            <div class="panel">
              <div class="field"><label>模板名称</label><el-input v-model="tpl.name" placeholder="例如：电影发布模板"/></div>
              <div class="field mt"><label>消息内容</label><el-input v-model="tpl.content" type="textarea" :rows="10" placeholder="在这里编写 Telegram 消息内容"/></div>
              <div class="mt"><el-switch v-model="tpl.enabled" active-text="启用"/><el-switch v-model="tpl.is_default" active-text="设为默认" class="ml"/></div>
              <el-button type="primary" class="mt" @click="saveTpl">保存模板</el-button>
              <div class="tpl-list"><div v-for="t in templates" :key="t.id" class="mini-row"><b>{{ t.name }}</b><el-tag v-if="t.is_default" type="success" size="small">默认</el-tag></div></div>
            </div>
            <div class="panel">
              <div class="panel-head"><h3>可用变量</h3><span>点击插入</span></div>
              <div class="var-grid"><button v-for="v in templateVars" :key="v" @click="insertVar(v)">{{ v }}</button></div>
              <p class="hint" style="margin:0 0 12px">{{ tagHint }}</p>
              <div class="preview-box"><div class="preview-title">预览效果</div><div class="preview-body">{{ previewText }}</div></div>
              <p class="hint">条件块写法：{{ syntaxHint }}</p>
            </div>
          </div>
        </section>

        <section v-else-if="tab === '操作审计'" class="fade">
          <div class="page-head"><div><h2>操作审计</h2><p>授权、建分享、推送、审核与重试的全量记录</p></div><el-tag>{{ audit.length }} 条</el-tag></div>
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
  --yg-bg:#f4f6fb;--yg-surface:#fff;--yg-ink:#1c2442;--yg-muted:#7e88a8;--yg-line:#e9edf6;
  --yg-primary:#5b5bd6;--yg-primary-2:#8271ef;--yg-teal:#19b3a6;--yg-amber:#f2a33c;--yg-rose:#e5526b;
  --yg-shadow:0 12px 34px rgba(28,36,66,.07);--yg-radius:16px;
  --el-color-primary:#5b5bd6;--el-border-radius-base:10px;--el-font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
}
*{box-sizing:border-box}
body{margin:0;background:var(--yg-bg);color:var(--yg-ink);font-family:var(--el-font-family);font-size:14px;-webkit-font-smoothing:antialiased}
button{font-family:inherit}
.ico{width:16px;height:16px;fill:none;stroke:currentColor;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round;flex:none}
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
.page-head{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:20px;gap:16px;flex-wrap:wrap}
.page-head h2{margin:0 0 5px;font-size:22px;letter-spacing:-.3px}.page-head p{margin:0;color:var(--yg-muted);font-size:12px}
.head-tools{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.hero{border-radius:20px;background:linear-gradient(120deg,#4f4fc4 0%,#6b5ce0 52%,#3aa9c9 100%);color:#fff;padding:26px 30px;display:flex;justify-content:space-between;align-items:center;gap:20px;box-shadow:0 18px 40px rgba(91,91,214,.22);position:relative;overflow:hidden}
.hero:after{content:"";position:absolute;width:280px;height:280px;border-radius:50%;border:40px solid rgba(255,255,255,.07);right:-70px;top:-120px}
.hero-tag{font-size:10px;letter-spacing:2px;opacity:.75;margin-bottom:6px}.hero h2{margin:0 0 6px;font-size:26px}.hero p{margin:0;opacity:.82;font-size:12px}
.hero-actions{display:flex;align-items:center;gap:14px;z-index:1}.hero-pill{padding:6px 13px;border-radius:20px;font-size:12px;font-weight:600}.pill-on{background:rgba(57,212,176,.22);color:#b9f7e6}.pill-off{background:rgba(255,255,255,.18)}
.stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(168px,1fr));gap:14px;margin:20px 0}
.stat-card{background:var(--yg-surface);border-radius:var(--yg-radius);padding:18px;box-shadow:var(--yg-shadow);position:relative;overflow:hidden;transition:.2s}
.stat-card:hover{transform:translateY(-3px);box-shadow:0 18px 40px rgba(28,36,66,.11)}
.stat-icon{width:34px;height:34px;border-radius:10px;display:grid;place-items:center;margin-bottom:12px}
.tone-indigo .stat-icon{background:#eeedff;color:#5b5bd6}.tone-teal .stat-icon{background:#e5f8f5;color:#19b3a6}.tone-amber .stat-icon{background:#fff3e2;color:#e29328}.tone-violet .stat-icon{background:#f2edff;color:#8b6cf0}.tone-rose .stat-icon{background:#ffedf0;color:#e5526b}.tone-slate .stat-icon{background:#eef1f7;color:#8a93aa}
.stat-num{font-size:27px;font-weight:750;line-height:1.1;font-variant-numeric:tabular-nums}.stat-label{color:var(--yg-muted);font-size:12px;margin-top:5px}
.stat-bar{height:3px;background:#f0f2f8;border-radius:3px;margin-top:14px;overflow:hidden}.stat-bar i{display:block;height:100%;width:42%;background:linear-gradient(90deg,var(--yg-primary),var(--yg-primary-2));border-radius:3px}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:18px}.three-col{display:grid;grid-template-columns:repeat(3,1fr);gap:18px}
.panel{background:var(--yg-surface);border:1px solid var(--yg-line);border-radius:var(--yg-radius);padding:20px;box-shadow:var(--yg-shadow);margin-bottom:18px}
.panel.narrow{max-width:760px}.panel-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}.panel-head h3{margin:0;font-size:15px}.panel-head span{font-size:11px;color:var(--yg-muted)}
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
.device-actions{display:flex;gap:12px;justify-content:center;align-items:center}.device-link{color:var(--yg-primary);text-decoration:none;font-size:13px;display:flex;align-items:center;gap:5px}
.kv-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.kv-grid small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:5px}.kv-grid b{font-size:13px;word-break:break-all}
.hint{color:#929bb5;font-size:11px;line-height:1.8;margin:12px 0 0}
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.field label{display:block;font-size:12px;font-weight:600;margin-bottom:7px}.field small{display:block;color:var(--yg-muted);font-size:11px;margin-top:6px}.field.inline{display:flex;align-items:center;gap:14px}.field.inline label{margin:0}
.panel-foot{display:flex;gap:10px;margin-top:22px;padding-top:18px;border-top:1px dashed var(--yg-line)}
.cursor-box{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}.cursor-box small{display:block;color:var(--yg-muted);font-size:11px;margin-bottom:5px}.cursor-box b{font-size:18px}
.view-switch{display:flex;border:1px solid var(--yg-line);border-radius:9px;overflow:hidden}.view-switch button{border:0;background:#fff;padding:7px 10px;cursor:pointer;color:#9aa3bd}.view-switch button.on{background:#eeedff;color:var(--yg-primary)}
.media-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:16px}
.media-card{background:#fff;border:1px solid var(--yg-line);border-radius:14px;overflow:hidden;box-shadow:var(--yg-shadow);transition:.2s}.media-card:hover{transform:translateY(-3px);box-shadow:0 16px 34px rgba(28,36,66,.12)}
.media-poster{height:270px;position:relative;background:#eef0f6}.no-poster{height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:8px;color:#b4bcd2}.no-poster .ico{width:30px;height:30px}.no-poster span{font-size:11px;letter-spacing:1px}
.media-badge{position:absolute;top:10px;left:10px;padding:4px 9px;border-radius:20px;font-size:10px;font-weight:700;color:#fff;background:#98a1b8}.badge-active{background:#19b3a6}.badge-not_created{background:#e29328}.badge-failed{background:#e5526b}.badge-cancelled{background:#98a1b8}
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
@media(max-width:1100px){.sidebar{width:72px}.brand>div:last-child,.nav-group-label,.nav-text,.nav-badge,.sidebar-foot{display:none}.nav-item{justify-content:center;padding:12px}.content{padding:20px}.two-col,.three-col,.form-grid,.auth-summary,.kv-grid,.credential-bar,.manage-grid{grid-template-columns:1fr}}
</style>
