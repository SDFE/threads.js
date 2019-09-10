import Observable from "zen-observable";
import { Thread } from "./thread";
export { Thread };
export declare namespace Pool {
    type Event<ThreadType extends Thread = any> = PoolEvent<ThreadType>;
    type EventType = PoolEventType;
}
/** Pool event type. Specifies the type of each `PoolEvent`. */
export declare enum PoolEventType {
    initialized = "initialized",
    taskCanceled = "taskCanceled",
    taskCompleted = "taskCompleted",
    taskFailed = "taskFailed",
    taskQueued = "taskQueued",
    taskQueueDrained = "taskQueueDrained",
    taskStart = "taskStart",
    terminated = "terminated"
}
declare type TaskRunFunction<ThreadType extends Thread, Return> = (worker: ThreadType) => Promise<Return>;
/** Pool event. Subscribe to those events using `pool.events()`. Useful for debugging. */
export declare type PoolEvent<ThreadType extends Thread> = {
    type: PoolEventType.initialized;
    size: number;
} | {
    type: PoolEventType.taskQueued;
    taskID: number;
} | {
    type: PoolEventType.taskQueueDrained;
} | {
    type: PoolEventType.taskStart;
    taskID: number;
    workerID: number;
} | {
    type: PoolEventType.taskCompleted;
    returnValue: any;
    taskID: number;
    workerID: number;
} | {
    type: PoolEventType.taskFailed;
    error: Error;
    taskID: number;
    workerID: number;
} | {
    type: PoolEventType.taskCanceled;
    taskID: number;
} | {
    type: PoolEventType.terminated;
    remainingQueue: Array<QueuedTask<ThreadType, any>>;
};
/**
 * Task that has been `pool.queued()`-ed.
 */
export interface QueuedTask<ThreadType extends Thread, Return> {
    /** @private */
    id: number;
    /** @private */
    run: TaskRunFunction<ThreadType, Return>;
    /**
     * Queued tasks can be cancelled until the pool starts running them on a worker thread.
     */
    cancel(): void;
    /**
     * `QueuedTask` is thenable, so you can `await` it.
     * Resolves when the task has successfully been executed. Rejects if the task fails.
     */
    then: Promise<Return>["then"];
}
/**
 * Thread pool managing a set of worker threads.
 * Use it to queue tasks that are run on those threads with limited
 * concurrency.
 */
export interface Pool<ThreadType extends Thread> {
    /**
     * Returns a promise that resolves once the task queue is emptied.
     *
     * @param allowResolvingImmediately Set to `true` to resolve immediately if task queue is currently empty.
     */
    completed(allowResolvingImmediately?: boolean): Promise<any>;
    /**
     * Returns an observable that yields pool events.
     */
    events(): Observable<PoolEvent<ThreadType>>;
    /**
     * Queue a task and return a promise that resolves once the task has been dequeued,
     * started and finished.
     *
     * @param task An async function that takes a thread instance and invokes it.
     */
    queue<Return>(task: TaskRunFunction<ThreadType, Return>): QueuedTask<ThreadType, Return>;
    /**
     * Terminate all pool threads.
     *
     * @param force Set to `true` to kill the thread even if it cannot be stopped gracefully.
     */
    terminate(force?: boolean): Promise<void>;
}
export interface PoolOptions {
    /** Maximum no. of tasks to run on one worker thread at a time. Defaults to one. */
    concurrency?: number;
    /** Gives that pool a name to be used for debug logging, letting you distinguish between log output of different pools. */
    name?: string;
    /** No. of worker threads to spawn and to be managed by the pool. */
    size?: number;
}
declare function PoolConstructor<ThreadType extends Thread>(spawnWorker: () => Promise<ThreadType>, optionsOrSize?: number | PoolOptions): Pool<ThreadType>;
/**
 * Thread pool constructor. Creates a new pool and spawns its worker threads.
 */
export declare const Pool: typeof PoolConstructor & {
    EventType: typeof PoolEventType;
};
