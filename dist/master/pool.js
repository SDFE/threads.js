"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const debug_1 = __importDefault(require("debug"));
const zen_observable_1 = __importDefault(require("zen-observable"));
const observable_promise_1 = require("../observable-promise");
const implementation_1 = __importDefault(require("./implementation"));
const thread_1 = require("./thread");
exports.Thread = thread_1.Thread;
let nextPoolID = 1;
const hasSymbols = () => typeof Symbol === 'function';
const hasSymbol = (name) => hasSymbols() && Boolean(Symbol[name]);
function flatMap(array, mapper) {
    return array.reduce((flattened, element) => [...flattened, ...mapper(element)], []);
}
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
function slugify(text) {
    return text.replace(/\W/g, " ").trim().replace(/\s+/g, "-");
}
/** Pool event type. Specifies the type of each `PoolEvent`. */
var PoolEventType;
(function (PoolEventType) {
    PoolEventType["initialized"] = "initialized";
    PoolEventType["taskCanceled"] = "taskCanceled";
    PoolEventType["taskCompleted"] = "taskCompleted";
    PoolEventType["taskFailed"] = "taskFailed";
    PoolEventType["taskQueued"] = "taskQueued";
    PoolEventType["taskQueueDrained"] = "taskQueueDrained";
    PoolEventType["taskStart"] = "taskStart";
    PoolEventType["terminated"] = "terminated";
})(PoolEventType = exports.PoolEventType || (exports.PoolEventType = {}));
function createArray(size) {
    const array = [];
    for (let index = 0; index < size; index++) {
        array.push(index);
    }
    return array;
}
function findIdlingWorker(workers, maxConcurrency) {
    return workers.find(worker => worker.runningTasks.length < maxConcurrency);
}
function runPoolTask(task, availableWorker, workerID, eventSubject, debug, TIMEOUT = 2000 * 60) {
    return __awaiter(this, void 0, void 0, function* () {
        debug(`Running task #${task.id} on worker #${workerID}...`);
        eventSubject.next({
            type: PoolEventType.taskStart,
            taskID: task.id,
            workerID
        });
        try {
            const returnValue = yield Promise.race([
                task.run(yield availableWorker.init),
                sleep(TIMEOUT).then(() => {
                    throw new Error('Timeout worker thread');
                })
            ]);
            debug(`Task #${task.id} completed successfully`);
            eventSubject.next({
                type: PoolEventType.taskCompleted,
                returnValue,
                taskID: task.id,
                workerID
            });
        }
        catch (error) {
            debug(`Task #${task.id} failed`);
            eventSubject.next({
                type: PoolEventType.taskFailed,
                taskID: task.id,
                error,
                workerID
            });
            if (error.message === 'Timeout worker thread') {
                throw error;
            }
        }
    });
}
function spawnWorkers(spawnWorker, count) {
    return createArray(count).map(() => ({
        init: spawnWorker(),
        runningTasks: []
    }));
}
function PoolConstructor(spawnWorker, optionsOrSize) {
    const options = typeof optionsOrSize === "number"
        ? { size: optionsOrSize }
        : optionsOrSize || {};
    const debug = debug_1.default(`threads:pool:${slugify(options.name || String(nextPoolID++))}`);
    const { concurrency = 1, size = implementation_1.default.defaultPoolSize } = options;
    let isClosing = false;
    let nextTaskID = 1;
    let taskQueue = [];
    const initErrors = [];
    const workers = spawnWorkers(spawnWorker, size);
    let eventSubject;
    const eventObservable = observable_promise_1.makeHot(new zen_observable_1.default(subscriber => {
        eventSubject = subscriber;
    }));
    Promise.all(workers.map(worker => worker.init)).then(() => eventSubject.next({
        type: PoolEventType.initialized,
        size: workers.length
    }), error => {
        debug("Error while initializing pool worker:", error);
        eventSubject.error(error);
        initErrors.push(error);
    });
    const scheduleWork = () => {
        debug(`Attempt de-queueing a task in order to run it...`);
        const availableWorker = findIdlingWorker(workers, concurrency);
        if (!availableWorker)
            return;
        const nextTask = taskQueue.shift();
        if (!nextTask) {
            debug(`Task queue is empty`);
            eventSubject.next({ type: PoolEventType.taskQueueDrained });
            return;
        }
        const workerID = workers.indexOf(availableWorker) + 1;
        const run = (worker, task) => __awaiter(this, void 0, void 0, function* () {
            const removeTaskFromWorkersRunningTasks = () => {
                worker.runningTasks = worker.runningTasks.filter(someRunPromise => someRunPromise !== runPromise);
            };
            // Defer task execution by one tick to give handlers time to subscribe
            yield sleep(0);
            let workerTimedOut = false;
            try {
                yield runPoolTask(task, availableWorker, workerID, eventSubject, debug);
            }
            catch (err) {
                if (err.message === 'Timeout worker thread') {
                    workerTimedOut = true;
                }
                else {
                    throw err;
                }
            }
            finally {
                removeTaskFromWorkersRunningTasks();
                if (workerTimedOut) {
                    yield thread_1.Thread.terminate(yield availableWorker.init);
                    const workerIndex = workers.indexOf(availableWorker);
                    if (workerIndex === -1) {
                        throw new Error('Cannot replace thread that timed out');
                    }
                    workers[workerIndex] = {
                        init: spawnWorker(),
                        runningTasks: []
                    };
                }
                if (!isClosing) {
                    scheduleWork();
                }
            }
        });
        const runPromise = run(availableWorker, nextTask);
        availableWorker.runningTasks.push(runPromise);
    };
    const pool = {
        completed(allowResolvingImmediately = false) {
            return __awaiter(this, void 0, void 0, function* () {
                const getCurrentlyRunningTasks = () => flatMap(workers, worker => worker.runningTasks);
                if (initErrors.length > 0) {
                    return Promise.reject(initErrors[0]);
                }
                if (allowResolvingImmediately && taskQueue.length === 0) {
                    return Promise.all(getCurrentlyRunningTasks());
                }
                const poolEventPromise = new Promise((resolve, reject) => {
                    const subscription = eventObservable.subscribe(event => {
                        if (event.type === PoolEventType.taskQueueDrained) {
                            subscription.unsubscribe();
                            resolve();
                        }
                        else if (event.type === PoolEventType.taskFailed) {
                            subscription.unsubscribe();
                            reject(event.error);
                        }
                    });
                });
                yield Promise.race([
                    poolEventPromise,
                    eventObservable // make a pool-wide error reject the completed() result promise
                ]);
                yield Promise.all(getCurrentlyRunningTasks());
            });
        },
        events() {
            return eventObservable;
        },
        queue(taskFunction) {
            if (isClosing) {
                throw Error(`Cannot schedule pool tasks after terminate() has been called.`);
            }
            if (initErrors.length > 0) {
                throw initErrors[0];
            }
            let resultPromiseThen;
            const createResultPromise = () => new Promise((resolve, reject) => {
                const eventSubscription = pool.events().subscribe(event => {
                    if (event.type === PoolEventType.taskCompleted && event.taskID === task.id) {
                        eventSubscription.unsubscribe();
                        resolve(event.returnValue);
                    }
                    else if (event.type === PoolEventType.taskFailed && event.taskID === task.id) {
                        eventSubscription.unsubscribe();
                        reject(event.error);
                    }
                    else if (event.type === PoolEventType.terminated) {
                        eventSubscription.unsubscribe();
                        reject(Error("Pool has been terminated before task was run."));
                    }
                });
            });
            const task = {
                id: nextTaskID++,
                run: taskFunction,
                cancel() {
                    if (taskQueue.indexOf(task) === -1)
                        return;
                    taskQueue = taskQueue.filter(someTask => someTask !== task);
                    eventSubject.next({
                        type: PoolEventType.taskCanceled,
                        taskID: task.id
                    });
                },
                get then() {
                    if (!resultPromiseThen) {
                        const resultPromise = createResultPromise();
                        resultPromiseThen = resultPromise.then.bind(resultPromise);
                    }
                    return resultPromiseThen;
                }
            };
            debug(`Queueing task #${task.id}...`);
            taskQueue.push(task);
            eventSubject.next({
                type: PoolEventType.taskQueued,
                taskID: task.id
            });
            scheduleWork();
            return task;
        },
        terminate(force) {
            return __awaiter(this, void 0, void 0, function* () {
                isClosing = true;
                if (!force) {
                    yield pool.completed(true);
                }
                eventSubject.next({
                    type: PoolEventType.terminated,
                    remainingQueue: [...taskQueue]
                });
                eventSubject.complete();
                yield Promise.all(workers.map((worker) => __awaiter(this, void 0, void 0, function* () { return thread_1.Thread.terminate(yield worker.init); })));
            });
        }
    };
    if (hasSymbols() && hasSymbol("toStringTag")) {
        pool[Symbol.toStringTag] = () => `[object Pool]`;
    }
    return pool;
}
PoolConstructor.EventType = PoolEventType;
/**
 * Thread pool constructor. Creates a new pool and spawns its worker threads.
 */
exports.Pool = PoolConstructor;
