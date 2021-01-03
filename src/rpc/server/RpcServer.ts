/* eslint-disable no-sync */
import { ServerCredentials } from '@grpc/grpc-js';
import type { EventEmitter } from 'events';
import Api from '../../clients/Api/Api';
import { RateLimitCache } from '../../clients/Api/structures';
import { IApiOptions } from '../../clients/Api/types';
import { DebugLevel } from '../../common';
import { LOG_LEVELS, LOG_SOURCES } from '../../constants';
import { addIdentifyLockService, addRateLimitService, addRequestService } from '../services';
import { Lock } from '../structures';
import { IDebugEvent, RpcServerOptions } from '../types';

/* eslint-disable-next-line @typescript-eslint/no-var-requires */
const grpc = require('@grpc/grpc-js');

/**
 * Rpc server.
 * @extends grpc.Server
 */
export default class RpcServer extends grpc.Server {
  /** Emitter for debug logging. */
  public emitter?: EventEmitter;

  /** Api client when the "request" service is added. */
  public apiClient?: Api;

  /** Cache for rate limits when having client authorize against server. */
  public rateLimitCache: RateLimitCache;

  /** Lock instance when the "identify lock" service is added. */
  public identifyLock: Lock;

  /** Destination host. */
  #host: string;

  /** Destination port. */
  #port: string | number;

  /** GRPC channel to receive connections with. */
  #channel: ServerCredentials;

  /**
   * Creates a new rpc Server.
   * @param options
   */
  public constructor(options: RpcServerOptions = {}) {
    super();
    const {
      host, port, channel, emitter, apiClient, identifyLock,
    } = options;

    this.#host = host ?? '127.0.0.1';
    this.#port = port ?? '50051';
    this.#channel = channel ?? grpc.ServerCredentials.createInsecure();
    this.emitter = emitter;
    this.apiClient = apiClient;
    this.identifyLock = identifyLock ?? new Lock(this.emitter);
    this.rateLimitCache = new RateLimitCache(false);
  }

  /** Establishes the arguments that will be passed to `bindAsync()` when starting the server. */
  private get bindArgs(): [string, ServerCredentials, (e: Error | null, port?: number) => void] {
    const callback = (e: Error | null) => {
      if (e !== null) {
        this.emit('DEBUG', {
          source: LOG_SOURCES.RPC,
          level: LOG_LEVELS.FATAL,
          message: e.message,
        });
      } else {
        try {
          this.start();
        } catch (err) {
          if (err.message === 'server must be bound in order to start') {
            /* eslint-disable-next-line no-console */
            console.error('server must be bound in order to start. maybe this host:port is already in use?');
          }
        }

        const message = `Rpc server running at http://${this.#host}:${this.#port}`;
        this.emit('DEBUG', {
          source: LOG_SOURCES.RPC,
          level: LOG_LEVELS.INFO,
          message,
        });
      }
    };

    return [`${this.#host}:${this.#port}`, this.#channel, callback];
  }

  /**
   * Adds the request service to this server. Allows the server to handle Discord API requests from clients.
   * @param token Discord token. Will be coerced into a bot token.
   * @param apiOptions Optional parameters for the api handler.
   */
  public addRequestService(token: string, apiOptions: IApiOptions = {}): void {
    addRequestService(this, token, apiOptions);
  }

  /** Adds the identify lock service to this server. Allows the server to maintain a lock for clients. */
  public addLockService(): void {
    addIdentifyLockService(this);
  }

  /** Adds the rate limit service to this server. Stores app-wide rate limits centrally and authorizes requests.. */
  public addRateLimitService(): void {
    addRateLimitService(this);
  }

  /** Start the server. */
  public serve(): void {
    const [dest, channel, callback] = this.bindArgs;
    this.bindAsync(dest, channel, callback);
  }

  /** Emits a log event. */
  public log(level: DebugLevel, message: string): void {
    this.emit('DEBUG', {
      source: LOG_SOURCES.RPC,
      level: LOG_LEVELS[level],
      message,
    });
  }

  /**
   * Emits logging events.
   * @param type Event name.
   * @param event Data emitted.
   */
  public emit(type: string, event: IDebugEvent): void {
    this.emitter && this.emitter.emit(type, event);
  }
}
