const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("desktopApp", {
  getBackendStatus: () => ipcRenderer.invoke("backend-status"),
  onBackendStatus: (callback) => {
    const listener = (_event, payload) => callback(payload);
    ipcRenderer.on("backend-status", listener);
    return () => ipcRenderer.removeListener("backend-status", listener);
  }
});
