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
const bot = ref({ name: '', token: '' })
const channel = ref({ bot_id: null, name: '', chat_id: '' })
const rule = ref({ name: '', directory_id: '', keyword: '', channel_id: null, auto_publish: false })
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

const syntaxHint = computed(() => '{{#if year}}年份 {{year}}{{/if}}')

const previewText = computed(() => {
  const raw = tpl.value.content || ''
  if (!raw) return '在左侧编写模板后显示预览'
  return raw.split('{{title}}').join('示例影片').split('{{share_url}}').join('https://share.example/xxx')
})

const filteredResources = computed(() => resources.value.filter(r => {
  const hitKey = !keyword.value || (r.name + (r.media_title || '')).toLowerCase().includes(keyword.value.toLowerCase())
  const hitStatus = !statusFilter.value || r.share_status === statusFilter.value
  return hitKey && hitStatus
}))

const healthChecks = computed(() => [
  { label: '光鸭 OpenAPI 授权', ok: overview.value.auth === 'ok', hint: auth.value.authenticated ? (auth.value.flow || '已授权') : '尚未完成授权' },
  { label: '网页分享会话', ok: !!shareStatus.value.configured, hint: shareStatus.value.configured ? '已配置会话凭证' : '需在 .env 配置' },
  { label: 'TMDB 识别', ok: true, hint: '搜索与详情接口已接入' },
  { label: '队列 Worker', ok: true, hint: '每 30 秒轮询一次' }
])

const headers = () => ({ 'X-Admin-Password': password.value, 'Content-Type': 'application/json' })
async function api(path, opt = {}) {
  const r = await fetch('/api' + path, { ...opt, headers: { ...headers(), ...(opt.headers || {}) } })
  const d = await r.json().catch(() => ({}))
  if (!r.ok) throw Error(d.detail || r.statusText)
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
  await loadAuth()
}
async function loadAuth() {
  try { auth.value = await api('/auth/status'); shareStatus.value = await api('/share/status') } catch (e) {}
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
  try { await api('/bots', { method: 'POST', body: JSON.stringify(bot.value) }); bot.value = { name: '', token: '' }; ElMessage.success('Bot 已添加'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
async function addChannel() {
  try { await api('/channels', { method: 'POST', body: JSON.stringify(channel.value) }); channel.value = { bot_id: null, name: '', chat_id: '' }; ElMessage.success('频道已添加'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
async function addRule() {
  try { await api('/rules', { method: 'POST', body: JSON.stringify(rule.value) }); ElMessage.success('规则已保存'); await load() }
  catch (e) { ElMessage.error(e.message) }
}
async function createShare(row) {
  try {
    const { value } = await ElMessageBox.prompt('0=永久 · 86400=1天 · 604800=7天 · 2592000=30天', '创建光鸭分享', { inputValue: '0' })
    const d = await api(`/resources/${row.id}/share?duration=${value}`, { method: 'POST' })
    ElMessage.success(d.share_url ? '分享已创建并保存链接' : '分享已创建')
    await load()
  } catch (e) { if (e !== 'cancel') ElMessage.error(e.message) }
}
async function identify(row, auto) {
  try {
    let d
    if (auto) d = await api(`/resources/${row.id}/auto-identify`, { method: 'POST' })
    else {
      const { value } = await ElMessageBox.prompt('输入 TMDB ID', '手动识别')
      if (!value) return
      d = await api(`/resources/${row.id}/identify`, { method: 'POST', body: JSON.stringify({ tmdb_id: Number(value), media_type: 'movie' }) })
    }
    ElMessage.success(d.ok ? '识别完成' : '未找到候选，请用手动识别')
    await load()
  } catch (e) { if (e !== 'cancel') ElMessage.error(e.message) }
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
  try { const d = await api('/auth/openapi/verify'); ElMessage.success(`OpenAPI 可用 · 用户 ${d.user_id || ''}`) }
  catch (e) { ElMessage.error(e.message) }
}
async function saveTpl() {
  try {
    if (tpl.value.id) await api(`/templates/${tpl.value.id}`, { method: 'PUT', body: JSON.stringify(tpl.value) })
    else await api('/templates', { method: 'POST', body: JSON.stringify(tpl.value) })
    tpl.value = { name: '', content: '', enabled: true, is_default: false }
    ElMessage.success('模板已保存'); await load()
  } catch (e) { ElMessage.error(e.message) }
}
async function copyCode() {
  try { await navigator.clipboard.writeText(device.value.user_code); ElMessage.success('授权码已复制') }
  catch (e) { ElMessage.error('复制失败，请手动选择') }
}
function insertVar(key) { tpl.value.content = (tpl.value.content || '') + '{{' + key + '}}' }
function tick() { clock.value = new Date().toLocaleTimeString('zh-CN', { hour12: false }) }

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
        <div class="foot-version">v0.3.0 · Asia/Shanghai</div>
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
          <div class="page-head"><div><h2>光鸭授权</h2><p>官方 OpenAPI 使用 x-client-id + Bearer Token；网页分享会话独立配置</p></div><el-button plain @click="loadAuth">刷新状态</el-button></div>
          <div class="auth-summary">
            <div class="auth-summary-item"><small>授权状态</small><b :class="auth.authenticated ? 'good' : 'bad'">{{ auth.authenticated ? '已授权' : '未授权' }}</b></div>
            <div class="auth-summary-item"><small>授权方式</small><b>{{ auth.flow || '—' }}</b></div>
            <div class="auth-summary-item"><small>用户 ID</small><b>{{ auth.sub || '—' }}</b></div>
            <div class="auth-summary-item"><small>过期时间</small><b>{{ auth.expires_at || '—' }}</b></div>
          </div>
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
            <div class="kv-grid"><div><small>Base</small><b>{{ shareStatus.base }}</b></div><div><small>DID</small><b>{{ shareStatus.did_set ? '已设置' : '未设置' }}</b></div><div><small>SMID</small><b>{{ shareStatus.smid_set ? '已设置' : '未设置' }}</b></div><div><small>Token 刷新</small><el-button size="small" :disabled="!auth.has_refresh_token" @click="refreshToken">立即刷新</el-button></div></div>
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
          <div class="page-head"><div><h2>资源库</h2><p>fileId 去重 · TMDB 识别 · 分享链接管理</p></div><div class="head-tools"><el-input v-model="keyword" placeholder="搜索资源名或 TMDB 标题" clearable style="width:260px"><template #prefix><svg class="ico" viewBox="0 0 24 24"><path :d="icon('search')"/></svg></template></el-input><el-select v-model="statusFilter" placeholder="分享状态" clearable style="width:150px"><el-option label="已创建" value="active"/><el-option label="未创建" value="not_created"/><el-option label="失败" value="failed"/><el-option label="已取消" value="cancelled"/></el-select><div class="view-switch"><button :class="{on:viewMode==='grid'}" @click="viewMode='grid'"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('grid')"/></svg></button><button :class="{on:viewMode==='list'}" @click="viewMode='list'"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('file')"/></svg></button></div></div></div>
          <div v-if="viewMode === 'grid'" class="media-grid">
            <div v-for="r in filteredResources" :key="r.id" class="media-card">
              <div class="media-poster"><el-image v-if="r.poster_path" :src="'https://image.tmdb.org/t/p/w342'+r.poster_path" fit="cover" style="width:100%;height:100%"/><div v-else class="no-poster"><svg class="ico" viewBox="0 0 24 24"><path :d="icon('image')"/></svg><span>{{ (r.ext||'FILE').toUpperCase() }}</span></div><span class="media-badge" :class="'badge-'+r.share_status">{{ r.share_status }}</span></div>
              <div class="media-body"><b :title="r.name">{{ r.media_title || r.name }}</b><small>{{ r.name }}</small><div class="media-meta"><span v-if="r.tmdb_id">TMDB #{{ r.tmdb_id }}</span><span v-if="r.revision">更新 {{ r.revision }} 次</span></div></div>
              <div class="media-actions"><el-button size="small" @click="createShare(r)">分享</el-button><el-button size="small" @click="identify(r,true)">自动识别</el-button><el-button size="small" text @click="identify(r,false)">手动</el-button></div>
            </div>
            <div v-if="!filteredResources.length" class="empty big">没有匹配的资源</div>
          </div>
          <div v-else class="panel">
            <el-table :data="filteredResources" stripe max-height="640">
              <el-table-column prop="name" label="资源名称" min-width="220" show-overflow-tooltip/>
              <el-table-column prop="media_title" label="TMDB 标题" min-width="170" show-overflow-tooltip/>
              <el-table-column prop="share_status" label="分享状态" width="110"/>
              <el-table-column prop="revision" label="更新次数" width="90"/>
              <el-table-column label="操作" width="250"><template #default="s"><el-button size="small" @click="createShare(s.row)">分享</el-button><el-button size="small" @click="identify(s.row,true)">自动识别</el-button><el-button size="small" text @click="identify(s.row,false)">手动</el-button></template></el-table-column>
            </el-table>
          </div>
        </section>

        <section v-else-if="tab === '分发规则'" class="fade">
          <div class="page-head"><div><h2>分发规则</h2><p>Bot → 频道 → 路由规则，三步建立推送链路</p></div></div>
          <div class="three-col">
            <div class="panel step-panel"><div class="step-head"><span>01</span><h3>添加 Bot</h3></div><el-input v-model="bot.name" placeholder="Bot 名称"/><el-input v-model="bot.token" type="password" show-password placeholder="Bot Token（加密存储）" class="mt"/><el-button type="primary" class="mt" @click="addBot">添加 Bot</el-button><div v-for="b in bots" :key="b.id" class="mini-row"><b>{{ b.name }}</b><span>{{ b.token_masked }}</span></div></div>
            <div class="panel step-panel"><div class="step-head"><span>02</span><h3>添加频道</h3></div><el-select v-model="channel.bot_id" placeholder="选择 Bot" style="width:100%"><el-option v-for="b in bots" :key="b.id" :label="b.name" :value="b.id"/></el-select><el-input v-model="channel.name" placeholder="频道名称" class="mt"/><el-input v-model="channel.chat_id" placeholder="频道 ID，如 -100..." class="mt"/><el-button type="primary" class="mt" @click="addChannel">添加频道</el-button><div v-for="c in channels" :key="c.id" class="mini-row"><b>{{ c.name }}</b><span>{{ c.chat_id }}</span></div></div>
            <div class="panel step-panel"><div class="step-head"><span>03</span><h3>分发规则</h3></div><el-input v-model="rule.name" placeholder="规则名称"/><el-input v-model="rule.directory_id" placeholder="目录 ID（可空）" class="mt"/><el-input v-model="rule.keyword" placeholder="文件名关键词（可空）" class="mt"/><el-select v-model="rule.channel_id" placeholder="目标频道" style="width:100%" class="mt"><el-option v-for="c in channels" :key="c.id" :label="c.name" :value="c.id"/></el-select><div class="mt"><el-switch v-model="rule.auto_publish" active-text="自动发布" inactive-text="人工审核"/></div><el-button type="primary" class="mt" @click="addRule">保存规则</el-button><div v-for="r in rules" :key="r.id" class="mini-row"><b>{{ r.name }}</b><span>{{ r.auto_publish ? '自动' : '审核' }}</span></div></div>
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

        <section v-else-if="tab === '消息模板'" class="fade">
          <div class="page-head"><div><h2>消息模板</h2><p>支持双花括号变量与条件块；有海报时自动发送图片</p></div></div>
          <div class="two-col tpl-layout">
            <div class="panel">
              <div class="field"><label>模板名称</label><el-input v-model="tpl.name" placeholder="例如：电影发布模板"/></div>
              <div class="field mt"><label>消息内容</label><el-input v-model="tpl.content" type="textarea" :rows="10" placeholder="在这里编写 Telegram 消息内容"/></div>
              <div class="mt"><el-switch v-model="tpl.enabled" active-text="启用"/><el-switch v-model="tpl.is_default" active-text="设为默认" class="ml"/></div>
              <el-button type="primary" class="mt" @click="saveTpl">保存模板</el-button>
              <div class="tpl-list"><div v-for="t in templates" :key="t.id" class="mini-row"><b>{{ t.name }}</b><el-tag v-if="t.is_default" type="success" size="small">默认</el-tag></div></div>
            </div>
            <div class="panel"><div class="panel-head"><h3>可用变量</h3><span>点击插入</span></div><div class="var-grid"><button v-for="v in ['title','overview','filename','year','resolution','size','share_url','tmdb_url']" :key="v" @click="insertVar(v)">{{ v }}</button></div><div class="preview-box"><div class="preview-title">预览效果</div><div class="preview-body">{{ previewText }}</div></div><p class="hint">条件块写法：{{ syntaxHint }}</p></div>
          </div>
        </section>

        <section v-else-if="tab === '操作审计'" class="fade">
          <div class="page-head"><div><h2>操作审计</h2><p>授权、建分享、推送、审核与重试的全量记录</p></div><el-tag>{{ audit.length }} 条</el-tag></div>
          <div class="panel"><div class="timeline full"><div v-for="a in audit" :key="a.id" class="tl-row"><span class="tl-dot"></span><span class="tl-time">{{ a.ts }}</span><b class="tl-action">{{ a.action }}</b><span class="tl-target">{{ a.target_type }} {{ a.target_id }}</span><span class="tl-detail">{{ a.detail }}</span></div><div v-if="!audit.length" class="empty big">暂无审计记录</div></div></div>
        </section>
      </main>
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
.mini-row{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:9px 0;border-bottom:1px dashed var(--yg-line);font-size:11px}.mini-row:last-child{border:0}.mini-row b{font-size:12px}.mini-row span{color:var(--yg-muted);max-width:60%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
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
@media(max-width:1100px){.sidebar{width:72px}.brand>div:last-child,.nav-group-label,.nav-text,.nav-badge,.sidebar-foot{display:none}.nav-item{justify-content:center;padding:12px}.content{padding:20px}.two-col,.three-col,.form-grid,.auth-summary,.kv-grid{grid-template-columns:1fr}}
</style>
