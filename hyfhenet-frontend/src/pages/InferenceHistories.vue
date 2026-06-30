<template>
    <v-container style="height: 100%; background-color: #fcfcfc; border-radius: 8px">
        <v-row no-gutters>
            <v-col cols="12">
                <v-data-table-server
                    :items-per-page="pageSize"
                    :page="page"
                    :headers="headers"
                    :items="inferenceHistories"
                    :items-length="totalCount"
                    style="background-color: #fcfcfc;"
                    @update:options="fetchInferenceHistories">
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
                        <tr v-for="item in inferenceHistories" :key="item.id">
                            <td>{{ item.id }}</td>
                            <td>{{ item.model }}</td>
                            <td>{{ item.inference_time_ms }}</td>
                            <td>{{ item.cipher_size_bytes }}</td>
                            <td>{{ item.iot_device_id }}</td>
                            <td>{{ item.date }}</td>
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
import { mapActions } from "vuex";

export default {
    data() {
        return {
            headers: [
                { title: "Id", key: "id" },
                { title: "Model", key: "model" },
                { title: "Inference time (ms)", key: "inference_time_ms" },
                { title: "Cipher size (bytes)", key: "cipher_size_bytes" },
                { title: "IoT device ID", key: "iot_device_id" },
                { title: "Date", key: "date" }
            ],
            inferenceHistories: [],
            page: 1,
            pageSize: 20,
            totalPages: 0,
            totalCount: 0
        }
    },
    methods: {
        ...mapActions(["fetchAllInferenceHistories"]),
        fetchInferenceHistories: function({ page }) {
            this.fetchAllInferenceHistories(page - 1)
                .then(res => {
                    this.totalCount = res.headers.get("X-Total-Count");
                    this.totalPages = Math.ceil(this.totalCount / this.pageSize);
                    return res.json();
                })
                .then(res => {
                    this.inferenceHistories = res;
                })
                .catch(e => e);
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
