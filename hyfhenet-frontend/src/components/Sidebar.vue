<template>
    <v-row style="padding: 8px">
        <v-col xs="12" style="padding: 0px 0px; height: 100%; width: 100%; background-color: #fcfcfc; border-radius: 12px;">
            <v-row style="margin: 8px 0px">
                <v-btn :ripple="false" :class="{ hyactive: $route.fullPath === '/' }" style="width: 100%" prepend-icon="mdi-devices" variant="text" size="x-large" @click="iotDevices">
                    IoT Devices
                </v-btn>
            </v-row>
            <v-row style="margin: 8px 0px">
                <v-btn :ripple="false" :class="{ hyactive: $route.fullPath === '/inference-histories' }" style="width: 100%" prepend-icon="mdi-history" variant="text" size="x-large" @click="inferenceHistories">
                    Inference histories
                </v-btn>
            </v-row>
            <v-row style="margin: 8px 0px">
                <v-btn :ripple="false" :class="{ hyactive: $route.fullPath === '/create-iot-device' }" style="width: 100%" prepend-icon="mdi-plus-network-outline" variant="text" size="x-large" @click="createIotDevice">
                    Create IoT device
                </v-btn>
            </v-row>
            <br><br><br><br><br><br><br>
            <v-row style="margin: 8px 0px">
                <v-btn :ripple="false" style="width: 100%" prepend-icon="mdi-logout" variant="text" size="x-large" @click="logOut">
                    Log out
                </v-btn>
            </v-row>
        </v-col>
    </v-row>
</template>

<script>
import {mapActions} from "vuex";

export default {
    data: function() {
        return {

        }
    },
    methods: {
        ...mapActions(["logOut"]),
        iotDevices() {
            this.$router.push("/")
        },
        inferenceHistories() {
            this.$router.push("/inference-histories")
        },
        createIotDevice() {
            this.$router.push("/create-iot-device")
        }
    }
}

</script>

<style>
.hyactive {
    background-color: #444444 !important;
    color: #fdfdfd !important;
}

</style>
