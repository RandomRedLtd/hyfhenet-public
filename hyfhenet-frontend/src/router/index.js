import { createRouter, createWebHistory } from 'vue-router/auto'

import IotDevices from "@/pages/IotDevices.vue"
import InferenceHistories from "@/pages/InferenceHistories.vue"
import CreateIotDevice from "@/pages/CreateIotDevice.vue";

const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    routes: [
        {
            path: "/",
            name: "IotDevices",
            component: IotDevices
        },
        {
            path: "/inference-histories",
            name: "InferenceHistories",
            component: InferenceHistories
        },
        {
            path: "/create-iot-device",
            name: "CreateIotDevice",
            component: CreateIotDevice
        }
    ],
})

// Workaround for https://github.com/vitejs/vite/issues/11804
router.onError((err, to) => {
    if (err?.message?.includes?.('Failed to fetch dynamically imported module')) {
        if (!localStorage.getItem('vuetify:dynamic-reload')) {
            console.log('Reloading page to fix dynamic import error')
            localStorage.setItem('vuetify:dynamic-reload', 'true')
            location.assign(to.fullPath)
        } else {
            console.error('Dynamic import error, reloading page did not fix it', err)
        }
    } else {
        console.error(err)
    }
})

router.isReady().then(() => {
    localStorage.removeItem('vuetify:dynamic-reload')
})

export default router
