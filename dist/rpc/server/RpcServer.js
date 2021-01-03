"use strict";
var __classPrivateFieldSet = (this && this.__classPrivateFieldSet) || function (receiver, privateMap, value) {
    if (!privateMap.has(receiver)) {
        throw new TypeError("attempted to set private field on non-instance");
    }
    privateMap.set(receiver, value);
    return value;
};
var __classPrivateFieldGet = (this && this.__classPrivateFieldGet) || function (receiver, privateMap) {
    if (!privateMap.has(receiver)) {
        throw new TypeError("attempted to get private field on non-instance");
    }
    return privateMap.get(receiver);
};
var _host, _port, _channel;
Object.defineProperty(exports, "__esModule", { value: true });
const structures_1 = require("../../clients/Api/structures");
const constants_1 = require("../../constants");
const services_1 = require("../services");
const structures_2 = require("../structures");
const grpc = require('@grpc/grpc-js');
class RpcServer extends grpc.Server {
    constructor(options = {}) {
        super();
        _host.set(this, void 0);
        _port.set(this, void 0);
        _channel.set(this, void 0);
        const { host, port, channel, emitter, apiClient, identifyLock, } = options;
        __classPrivateFieldSet(this, _host, host !== null && host !== void 0 ? host : '127.0.0.1');
        __classPrivateFieldSet(this, _port, port !== null && port !== void 0 ? port : '50051');
        __classPrivateFieldSet(this, _channel, channel !== null && channel !== void 0 ? channel : grpc.ServerCredentials.createInsecure());
        this.emitter = emitter;
        this.apiClient = apiClient;
        this.identifyLock = identifyLock !== null && identifyLock !== void 0 ? identifyLock : new structures_2.Lock(this.emitter);
        this.rateLimitCache = new structures_1.RateLimitCache(false);
    }
    get bindArgs() {
        const callback = (e) => {
            if (e !== null) {
                this.emit('DEBUG', {
                    source: constants_1.LOG_SOURCES.RPC,
                    level: constants_1.LOG_LEVELS.FATAL,
                    message: e.message,
                });
            }
            else {
                try {
                    this.start();
                }
                catch (err) {
                    if (err.message === 'server must be bound in order to start') {
                        console.error('server must be bound in order to start. maybe this host:port is already in use?');
                    }
                }
                const message = `Rpc server running at http://${__classPrivateFieldGet(this, _host)}:${__classPrivateFieldGet(this, _port)}`;
                this.emit('DEBUG', {
                    source: constants_1.LOG_SOURCES.RPC,
                    level: constants_1.LOG_LEVELS.INFO,
                    message,
                });
            }
        };
        return [`${__classPrivateFieldGet(this, _host)}:${__classPrivateFieldGet(this, _port)}`, __classPrivateFieldGet(this, _channel), callback];
    }
    addRequestService(token, apiOptions = {}) {
        services_1.addRequestService(this, token, apiOptions);
    }
    addLockService() {
        services_1.addIdentifyLockService(this);
    }
    addRateLimitService() {
        services_1.addRateLimitService(this);
    }
    serve() {
        const [dest, channel, callback] = this.bindArgs;
        this.bindAsync(dest, channel, callback);
    }
    log(level, message) {
        this.emit('DEBUG', {
            source: constants_1.LOG_SOURCES.RPC,
            level: constants_1.LOG_LEVELS[level],
            message,
        });
    }
    emit(type, event) {
        this.emitter && this.emitter.emit(type, event);
    }
}
exports.default = RpcServer;
_host = new WeakMap(), _port = new WeakMap(), _channel = new WeakMap();
