const { app, BrowserWindow, ipcMain } = require("electron");
const path = require("path");
const { spawn } = require("child_process");

let mainWindow = null;
let apiProcess = null;
let apiReady = false;

const projectRoot = path.resolve(__dirname, "..");
const pythonPath = path.join(projectRoot, ".venv", "bin", "python");

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1320,
    height: 820,
    minWidth: 980,
    minHeight: 680,
    backgroundColor: "#f7f5f1",
    titleBarStyle: "hiddenInset",
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  mainWindow.loadFile(path.join(projectRoot, "ui", "index.html"));
}

function startApi() {
  if (apiProcess) {
    return;
  }

  apiProcess = spawn(
    pythonPath,
    ["-m", "uvicorn", "redis_search_engine.main:app", "--host", "127.0.0.1", "--port", "8000"],
    {
      cwd: projectRoot,
      stdio: ["ignore", "pipe", "pipe"]
    }
  );

  const watchReady = (chunk) => {
    const text = chunk.toString();
    if (text.includes("Application startup complete") || text.includes("Uvicorn running on")) {
      apiReady = true;
      BrowserWindow.getAllWindows().forEach((window) => {
        window.webContents.send("backend-status", { ready: true });
      });
    }
  };

  apiProcess.stdout.on("data", watchReady);
  apiProcess.stderr.on("data", watchReady);

  apiProcess.on("exit", () => {
    apiProcess = null;
    apiReady = false;
    BrowserWindow.getAllWindows().forEach((window) => {
      window.webContents.send("backend-status", { ready: false });
    });
  });
}

app.whenReady().then(() => {
  startApi();
  createWindow();

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

ipcMain.handle("backend-status", async () => ({
  ready: apiReady
}));

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("before-quit", () => {
  if (apiProcess) {
    apiProcess.kill("SIGTERM");
  }
});
