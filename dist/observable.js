"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const zen_observable_1 = __importDefault(require("zen-observable"));
exports.Observable = zen_observable_1.default;
const $observers = Symbol("observers");
/**
 * Observable subject. Implements the Observable interface, but also exposes
 * the `next()`, `error()`, `complete()` methods to initiate observable
 * updates "from the outside".
 *
 * Use `Observable.from(subject)` to derive an observable that proxies all
 * values, errors and the completion raised on this subject, but does not
 * expose the `next()`, `error()`, `complete()` methods.
 */
class Subject extends zen_observable_1.default {
    constructor() {
        super(observer => {
            this[$observers] = [
                ...(this[$observers] || []),
                observer
            ];
            const unsubscribe = () => {
                this[$observers] = this[$observers].filter(someObserver => someObserver !== observer);
            };
            return unsubscribe;
        });
    }
    complete() {
        this[$observers].forEach(observer => observer.complete());
    }
    error(error) {
        this[$observers].forEach(observer => observer.error(error));
    }
    next(value) {
        this[$observers].forEach(observer => observer.next(value));
    }
}
exports.Subject = Subject;
