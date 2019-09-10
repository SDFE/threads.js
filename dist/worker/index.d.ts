import { WorkerFunction, WorkerModule } from "../types/worker";
export { Transfer } from "../transferable";
/**
 * Expose a function or a module (an object whose values are functions)
 * to the main thread. Must be called exactly once in every worker thread
 * to signal its API to the main thread.
 *
 * @param exposed Function or object whose values are functions
 */
export declare function expose(exposed: WorkerFunction | WorkerModule<any>): void;