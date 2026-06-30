import { registerPlugins } from '@/plugins'
import App from './App.vue'
import { createApp } from 'vue'
import store from "@/store";
import dotenv from "dotenv"

const app = createApp(App)

app.use(store)
registerPlugins(app)

app.mount('#app')
