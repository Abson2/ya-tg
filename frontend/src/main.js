import {createApp} from 'vue'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
// 阶段六：Element Plus 官方暗色 CSS 变量（由 html.dark 类激活，纯附加层）
import 'element-plus/theme-chalk/dark/css-vars.css'
import App from './App.vue'
createApp(App).use(ElementPlus).mount('#app')
