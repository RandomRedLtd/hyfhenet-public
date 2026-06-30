<template>
    <v-container style="height: 100%; background-color: #fcfcfc; border-radius: 8px">
        <v-row no-gutters>
            <v-col cols="12">
                <v-data-table-server
                    :items-per-page="pageSize"
                    :page="page"
                    :headers="headers"
                    :items="iotDevices"
                    :items-length="totalCount"
                    style="background-color: #fcfcfc;"
                    @update:options="fetchIotDevices">
                    <template #[`header.score`]="{ column }">
                        <v-tooltip text="Score value for models is calculated using default scoring method for that model type in scikit-learn">
                            <template #activator="{ props }">
                                <div class="v-data-table-header__content">
                                    <div class="v-data-table-header__content">
                                        <span>{{ column.title }}</span>&nbsp;&nbsp;
                                    </div>
                                    <v-icon v-bind="props" icon="mdi-information-outline" />
                                </div>
                            </template>
                        </v-tooltip>
                    </template>
                    <template v-slot:body="{ items }">
                        <tr v-for="item in iotDevices" :key="item.id">
                            <td>{{ item.id }}</td>
                            <td>{{ item.device_name }}</td>
                            <td>{{ item.api_key }}</td>
                            <td>{{ item.deleted ? "Yes" : "No" }}</td>
                            <td align="end">
                                <template v-if="item.deleted">
                                    <v-btn @click="_deleteIotDevice(item)" color="green">Restore</v-btn>
                                </template>
                                <template v-else>
                                    <v-btn @click="_deleteIotDevice(item)" color="red">Delete</v-btn>
                                </template>
                            </td>
                        </tr>
                    </template>
                    <template v-slot:bottom>
                        <div class="text-center pt-2">
                            <v-pagination
                                v-model="page"
                                :length="totalPages"
                            ></v-pagination>
                        </div>
                    </template>
                </v-data-table-server>
            </v-col>
        </v-row>
    </v-container>
</template>

<script>
import {mapActions, mapState} from "vuex";

export default {
    data() {
        return {
            headers: [
                { title: "Id", key: "id" },
                { title: "Device name", key: "device_name" },
                { title: "API key", key: "api_key" },
                { title: "Deleted", key: "deleted", value: item => item.deleted ? "Yes" : "No" },
                {}
            ],
            iotDevices: [],
            page: 1,
            pageSize: 20,
            totalPages: 0,
            totalCount: 0
        }
    },
    methods: {
        ...mapActions(["fetchAllIotDevices", "deleteIotDevice"]),
        fetchIotDevices({ page }) {
            this.fetchAllIotDevices(page - 1)
                .then(res => {
                    this.totalCount = res.headers.get("X-Total-Count");
                    this.totalPages = Math.ceil(this.totalCount / this.pageSize);
                    return res.json();
                })
                .then(res => {
                    this.iotDevices = res;
                })
                .catch(e => e);
        },
        _deleteIotDevice(iotDevice) {
            this.deleteIotDevice(iotDevice.id).then(res => {
                iotDevice.deleted = !iotDevice.deleted;
            });
        }
    }
}
</script>

<style>
.v-data-table__tbody > tr:hover {
    background-color: #444444;
    color: #fcfcfc;
    cursor: pointer;
}

</style>
