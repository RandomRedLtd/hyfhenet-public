<template>
    <v-app style="background-color: #f1f1f1; overflow: auto">
        <v-main>
            <v-container style="max-width: 1920px; min-width: 1920px; height: 100%">
                <v-row style="height: calc(100% - 32px)">
                    <template v-if="loggedIn">
                        <v-col cols="2">
                            <Sidebar></Sidebar>
                        </v-col>
                        <v-col cols="10" style="padding: 8px">
                            <router-view />
                        </v-col>
                    </template>
                    <template v-else>
                        <v-row align="center" justify="center" style="height: 100%">
                            <v-col cols="12" style="max-width: 700px;" class="d-flex flex-column justify-center align-center">
                                <h1>Hyfhenet</h1>
                                <br>
                                <h2>To get started enter your API key</h2>
                                <v-text-field v-model="apiKeyInput" style="width: 100%" type="password" autocomplete="off"></v-text-field>
                                <v-btn @click.prevent="_logIn">Log in</v-btn>
                            </v-col>
                        </v-row>
                    </template>
                </v-row>
            </v-container>
        </v-main>
    </v-app>
</template>

<script>
import Sidebar from "@/components/Sidebar.vue";
import {mapActions, mapState} from "vuex";

export default {
    components: {
        Sidebar
    },
    beforeCreate() {
        this.$store.commit("SET_LOGGED_IN", localStorage.getItem("loggedIn"));
    },
    data() {
        return {
            apiKeyInput: null,
        }
    },
    computed: {
        ...mapState(["loggedIn"])
    },
    methods: {
        ...mapActions(["logIn"]),
        _logIn() {
            this.logIn(this.apiKeyInput)
            this.apiKeyInput = null;
        }
    },
}

</script>
