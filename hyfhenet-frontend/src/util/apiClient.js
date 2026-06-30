const apiUrl = "http://localhost:8000/api"

export default {
    logIn(apiKey) {
        return fetch(`${apiUrl}/admin/log-in`, { method: "GET", headers: { "X-Api-Key": apiKey }, credentials: "include" });
    },
    logOut() {
        return fetch(`${apiUrl}/admin/log-out`, { method: "GET", credentials: "include" });
    },
    inferenceHistories(page) {
        return fetch(`${apiUrl}/inference-histories/?page=${page}`, { method: "GET", credentials: "include" });
    },
    fetchIotDevices(page) {
        return fetch(`${apiUrl}/iot-devices/?page=${page}`, { method: "GET", credentials: "include" });
    },
    createIotDevice(iotDevice) {
        return fetch(`${apiUrl}/iot-devices`, { method: "POST", headers: { "Content-Type": "application/json" }, credentials: "include", body: JSON.stringify(iotDevice) });
    },
    deleteIotDevice(id) {
        return fetch(`${apiUrl}/iot-devices/${id}`, { method: "DELETE", credentials: "include" });
    }
}
