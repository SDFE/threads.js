"use strict";
// tslint:disable function-constructor no-eval max-classes-per-file
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
const callsites_1 = __importDefault(require("callsites"));
const events_1 = __importDefault(require("events"));
const os_1 = require("os");
const path = __importStar(require("path"));
let tsNodeAvailable;
const defaultPoolSize = os_1.cpus().length;
function detectTsNode() {
    if (typeof __non_webpack_require__ === "function") {
        // Webpack build: => No ts-node required or possible
        return false;
    }
    if (tsNodeAvailable) {
        return tsNodeAvailable;
    }
    try {
        require.resolve("ts-node");
        tsNodeAvailable = true;
    }
    catch (error) {
        if (error && error.code === "MODULE_NOT_FOUND") {
            tsNodeAvailable = false;
        }
        else {
            // Re-throw
            throw error;
        }
    }
    return tsNodeAvailable;
}
function createTsNodeModule(scriptPath) {
    const content = `
    require("ts-node/register/transpile-only");
    require(${JSON.stringify(scriptPath)});
  `;
    return content;
}
function rebaseScriptPath(scriptPath, ignoreRegex) {
    const parentCallSite = callsites_1.default().find((callsite) => {
        const filename = callsite.getFileName();
        return Boolean(filename && !filename.match(ignoreRegex) && !filename.match(/[\/\\]master[\/\\]implementation/));
    });
    const callerPath = parentCallSite ? parentCallSite.getFileName() : null;
    const rebasedScriptPath = callerPath ? path.join(path.dirname(callerPath), scriptPath) : scriptPath;
    return rebasedScriptPath;
}
function resolveScriptPath(scriptPath) {
    // eval() hack is also webpack-related
    const workerFilePath = typeof __non_webpack_require__ === "function"
        ? __non_webpack_require__.resolve(path.join(eval("__dirname"), scriptPath))
        : require.resolve(rebaseScriptPath(scriptPath, /[\/\\]worker_threads[\/\\]/));
    return workerFilePath;
}
function initWorkerThreadsWorker() {
    // Webpack hack
    const NativeWorker = typeof __non_webpack_require__ === "function"
        ? __non_webpack_require__("worker_threads").Worker
        : eval("require")("worker_threads").Worker;
    let allWorkers = [];
    class Worker extends NativeWorker {
        constructor(scriptPath) {
            const resolvedScriptPath = resolveScriptPath(scriptPath);
            if (resolvedScriptPath.match(/\.tsx?$/i) && detectTsNode()) {
                super(createTsNodeModule(resolvedScriptPath), { eval: true });
            }
            else {
                super(resolvedScriptPath);
            }
            this.mappedEventListeners = new WeakMap();
            allWorkers.push(this);
        }
        addEventListener(eventName, rawListener) {
            const listener = (message) => {
                rawListener({ data: message });
            };
            this.mappedEventListeners.set(rawListener, listener);
            this.on(eventName, listener);
        }
        removeEventListener(eventName, rawListener) {
            const listener = this.mappedEventListeners.get(rawListener) || rawListener;
            this.off(eventName, listener);
        }
    }
    const terminateAll = () => {
        allWorkers.forEach(worker => worker.terminate());
        allWorkers = [];
    };
    // Take care to not leave orphaned processes behind. See #147.
    process.on("SIGINT", () => terminateAll());
    process.on("SIGTERM", () => terminateAll());
    return Worker;
}
function initTinyWorker() {
    const TinyWorker = require("tiny-worker");
    let allWorkers = [];
    class Worker extends TinyWorker {
        constructor(scriptPath) {
            // Need to apply a work-around for Windows or it will choke upon the absolute path
            // (`Error [ERR_INVALID_PROTOCOL]: Protocol 'c:' not supported`)
            const resolvedScriptPath = process.platform === "win32"
                ? `file:///${resolveScriptPath(scriptPath).replace(/\\/g, "/")}`
                : resolveScriptPath(scriptPath);
            if (resolvedScriptPath.match(/\.tsx?$/i) && detectTsNode()) {
                super(new Function(createTsNodeModule(resolveScriptPath(scriptPath))), [], { esm: true });
            }
            else {
                super(resolvedScriptPath, [], { esm: true });
            }
            allWorkers.push(this);
            this.emitter = new events_1.default();
            this.onerror = (error) => this.emitter.emit("error", error);
            this.onmessage = (message) => this.emitter.emit("message", message);
        }
        addEventListener(eventName, listener) {
            this.emitter.addListener(eventName, listener);
        }
        removeEventListener(eventName, listener) {
            this.emitter.removeListener(eventName, listener);
        }
        terminate() {
            allWorkers = allWorkers.filter(worker => worker !== this);
            return super.terminate();
        }
    }
    const terminateAll = () => {
        allWorkers.forEach(worker => worker.terminate());
        allWorkers = [];
    };
    // Take care to not leave orphaned processes behind
    // See <https://github.com/avoidwork/tiny-worker#faq>
    process.on("SIGINT", () => terminateAll());
    process.on("SIGTERM", () => terminateAll());
    return Worker;
}
function selectWorkerImplementation() {
    try {
        return initWorkerThreadsWorker();
    }
    catch (error) {
        // tslint:disable-next-line no-console
        console.debug("Node worker_threads not available. Trying to fall back to tiny-worker polyfill...");
        return initTinyWorker();
    }
}
exports.default = {
    defaultPoolSize,
    selectWorkerImplementation
};
