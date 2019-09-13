"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const zen_observable_1 = __importDefault(require("zen-observable"));
const doNothing = () => undefined;
const returnInput = (input) => input;
const runDeferred = (fn) => Promise.resolve().then(fn);
function fail(error) {
    throw error;
}
/**
 * Creates a hybrid, combining the APIs of an Observable and a Promise.
 *
 * It is used to proxy async process states when we are initially not sure
 * if that async process will yield values once (-> Promise) or multiple
 * times (-> Observable).
 *
 * Note that the observable promise inherits some of zen-observable's characteristics:
 * The `init` function will be called *once for every time anyone subscribes to it*.
 *
 * If this is undesired, derive a hot observable from it using `makeHot()` and
 * subscribe to that.
 */
function ObservablePromise(init) {
    let initHasRun = false;
    const fulfillmentCallbacks = [];
    const rejectionCallbacks = [];
    let firstValue;
    let firstValueSet = false;
    let rejection;
    let state = "pending";
    const onNext = (value) => {
        if (!firstValueSet) {
            firstValue = value;
            firstValueSet = true;
        }
    };
    const onError = (error) => {
        state = "rejected";
        rejection = error;
        for (const onRejected of rejectionCallbacks) {
            // Promisifying the call to turn errors into unhandled promise rejections
            // instead of them failing sync and cancelling the iteration
            runDeferred(() => onRejected(error));
        }
    };
    const onCompletion = () => {
        state = "fulfilled";
        for (const onFulfilled of fulfillmentCallbacks) {
            // Promisifying the call to turn errors into unhandled promise rejections
            // instead of them failing sync and cancelling the iteration
            runDeferred(() => onFulfilled(firstValue));
        }
    };
    const observable = new zen_observable_1.default(originalObserver => {
        const observer = Object.assign({}, originalObserver, { complete() {
                originalObserver.complete();
                onCompletion();
            },
            error(error) {
                originalObserver.error(error);
                onError(error);
            },
            next(value) {
                originalObserver.next(value);
                onNext(value);
            } });
        const resolve = (value) => {
            if (value !== undefined) {
                observer.next(value);
            }
            observer.complete();
        };
        const reject = observer.error.bind(observer);
        try {
            initHasRun = true;
            return init(resolve, reject, observer);
        }
        catch (error) {
            reject(error);
        }
    });
    function then(onFulfilledRaw, onRejectedRaw) {
        const onFulfilled = onFulfilledRaw || returnInput;
        const onRejected = onRejectedRaw || fail;
        let onRejectedCalled = false;
        return new Promise((resolve, reject) => {
            const rejectionCallback = (error) => {
                if (onRejectedCalled)
                    return;
                onRejectedCalled = true;
                try {
                    resolve(onRejected(error));
                }
                catch (anotherError) {
                    reject(anotherError);
                }
            };
            const fulfillmentCallback = (value) => {
                try {
                    resolve(onFulfilled(value));
                }
                catch (error) {
                    rejectionCallback(error);
                }
            };
            if (!initHasRun) {
                observable.subscribe({ error: rejectionCallback });
            }
            if (state === "fulfilled") {
                return resolve(onFulfilled(firstValue));
            }
            if (state === "rejected") {
                onRejectedCalled = true;
                return resolve(onRejected(rejection));
            }
            fulfillmentCallbacks.push(fulfillmentCallback);
            rejectionCallbacks.push(rejectionCallback);
        });
    }
    const catchFn = (onRejected) => {
        return then(undefined, onRejected);
    };
    const finallyFn = (onCompleted) => {
        onCompleted = onCompleted || doNothing;
        return then((value) => {
            onCompleted();
            return value;
        }, () => onCompleted());
    };
    // tslint:disable-next-line prefer-object-spread
    return Object.assign(observable, {
        [Symbol.toStringTag]: "[object ObservablePromise]",
        then: then,
        catch: catchFn,
        finally: finallyFn
    });
}
exports.ObservablePromise = ObservablePromise;
/**
 * Turns a cold observable into a hot observable.
 *
 * Returns a new observable promise that does exactly the same, but acts as a subscription aggregator,
 * so that N subscriptions to it only result in one subscription to the input observable promise.
 *
 * That one subscription on the input observable promise is setup immediately.
 */
function makeHot(async) {
    let observers = [];
    async.subscribe({
        complete() {
            observers.forEach(observer => observer.complete());
        },
        error(error) {
            observers.forEach(observer => observer.error(error));
        },
        next(value) {
            observers.forEach(observer => observer.next(value));
        }
    });
    const aggregator = ObservablePromise((resolve, reject, observer) => {
        observers.push(observer);
        const unsubscribe = () => {
            observers = observers.filter(someObserver => someObserver !== observer);
        };
        return unsubscribe;
    });
    return aggregator;
}
exports.makeHot = makeHot;
