<template>
    <v-container style="height: 100%; background-color: #fcfcfc; border-radius: 8px">
        <v-row no-gutters justify="center" style="width: 100%">
            <v-col style="max-width: 800px" align-self="center">
                <v-form fast-fail @submit.prevent v-model="valid" ref="form">
                    <v-text-field
                        variant="outlined"
                        v-model="newIotDevice.device_name"
                        required
                        :rules="[v => !!v || 'IoT device name is required']"
                        label="IoT device name"
                        autocomplete="off"
                    ></v-text-field>
                    <v-text-field
                        variant="outlined"
                        v-model="newIotDevice.api_key"
                        required
                        :rules="[v => !!v || 'IoT device API key is required']"
                        label="IoT device API key"
                        autocomplete="off"
                    ></v-text-field>
                </v-form>
                <v-row justify="space-between" style="padding: 32px;">
                    <v-btn @click="clear">Clear</v-btn>
                    <v-btn :disabled="!valid" @click="createNewIotDevice">Create</v-btn>
                </v-row>
            </v-col>
        </v-row>
        <v-snackbar
            v-model="snackbar"
            :timeout="5000">
            <h2>IoT device successfully created, ID: {{ newIotDeviceId }}</h2>
        </v-snackbar>
    </v-container>
</template>

<script>
import {mapActions} from "vuex";

export default {
    data() {
        return {
            valid: false,
            snackbar: false,
            newIotDeviceId: null,
            newIotDevice: {
                device_name: null,
                api_key: null,
            },
        }
    },
    methods: {
        ...mapActions(["createIotDevice"]),
        createNewIotDevice: function() {
            this.createIotDevice(this.newIotDevice).then(res => {this.snackbar = true; this.clear(); return res.json()}).then(res => this.newIotDeviceId = res.id);

        },
        clear: function() {
            this.$refs.form.reset();

            this.newIotDevice = {
                device_name: null,
                api_key: null,
            }

            this.data = null;
        }
    },
}
</script>
