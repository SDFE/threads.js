"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const implementation_1 = __importDefault(require("./implementation"));
var pool_1 = require("./pool");
exports.Pool = pool_1.Pool;
var spawn_1 = require("./spawn");
exports.spawn = spawn_1.spawn;
var thread_1 = require("./thread");
exports.Thread = thread_1.Thread;
/** Worker implementation. Either web worker or a node.js Worker class. */
exports.Worker = implementation_1.default.selectWorkerImplementation();
