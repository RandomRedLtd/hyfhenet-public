import {createStore} from "vuex";
import apiClient from "@/util/apiClient.js";

export default createStore({
    state() {
        return {
            loggedIn: false
        }
    },
    mutations: {
        SET_LOGGED_IN(state, payload) {
            state.loggedIn = payload
        },
    },
    actions: {
        logIn({ commit, state }, apiKey) {
            apiClient.logIn(apiKey).then(res => {
                if (res.status === 200) {
                    localStorage.setItem("loggedIn", true);
                    commit("SET_LOGGED_IN", true)
                }
            });
        },
        logOut({ commit }) {
            apiClient.logOut().then(res => {
                localStorage.removeItem("loggedIn");
                commit("SET_LOGGED_IN", false);
            });
        },
        fetchAllIotDevices({ commit, state }, page) {
            return apiClient.fetchIotDevices(page).then(res => {
                if (res.status === 401) {
                    localStorage.setItem("loggedIn", false);
                    commit("SET_LOGGED_IN", false)
                }

                return res;
            });
        },
        fetchAllInferenceHistories({ commit, state }, page) {
            return apiClient.inferenceHistories(page).then(res => {
                if (res.status === 401) {
                    localStorage.setItem("loggedIn", false);
                    commit("SET_LOGGED_IN", false)
                }

                return res;
            });
        },
        createIotDevice({}, newIotDevice) {
            return apiClient.createIotDevice(newIotDevice);
        },
        deleteIotDevice({}, id) {
            return apiClient.deleteIotDevice(id).then(res => {
                if (res.status === 401) {
                    localStorage.setItem("loggedIn", false);
                    commit("SET_LOGGED_IN", false)
                }

                return res;
            });
        }
    }
})
