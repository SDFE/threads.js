export declare enum MasterMessageType {
    run = "run"
}
export declare type MasterJobRunMessage = {
    type: MasterMessageType.run;
    uid: number;
    method?: string;
    args: any[];
};
export declare type MasterSentMessage = MasterJobRunMessage;
export declare enum WorkerMessageType {
    error = "error",
    init = "init",
    result = "result",
    running = "running",
    uncaughtError = "uncaughtError"
}
export declare type WorkerUncaughtErrorMessage = {
    type: WorkerMessageType.uncaughtError;
    error: {
        message: string;
        name: string;
        stack?: string;
    };
};
export declare type WorkerInitMessage = {
    type: WorkerMessageType.init;
    exposed: {
        type: "function";
    } | {
        type: "module";
        methods: string[];
    };
};
export declare type WorkerJobErrorMessage = {
    type: WorkerMessageType.error;
    uid: number;
    error: {
        message: string;
        name: string;
        stack?: string;
    };
};
export declare type WorkerJobResultMessage = {
    type: WorkerMessageType.result;
    uid: number;
    complete?: true;
    payload?: any;
};
export declare type WorkerJobStartMessage = {
    type: WorkerMessageType.running;
    uid: number;
    resultType: "observable" | "promise";
};
export declare type WorkerSentMessage = WorkerInitMessage | WorkerJobErrorMessage | WorkerJobResultMessage | WorkerJobStartMessage | WorkerUncaughtErrorMessage;
