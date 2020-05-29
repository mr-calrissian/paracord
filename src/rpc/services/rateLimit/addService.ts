/* eslint-disable callback-return */
import {
  RequestMetaMessage,
  AuthorizationMessage,
  RateLimitStateMessage,
} from '../../structures';
import { BaseRequest, RateLimitHeaders } from '../../../clients/Api/structures';
import { LOG_SOURCES, LOG_LEVELS } from '../../../constants';
import { loadProto } from '../common';
import RpcServer from '../../server/RpcServer';
import { TServiceCallbackError, AuthorizationProto, RateLimitStateProto } from '../../types';

const rateLimitProto = loadProto('rate_limit');

/**
 * Create callback functions for the rate limit service.
 * @param server
 */
export default (server: RpcServer): void => {
  server.rateLimitCache.startSweepInterval();

  /* eslint-disable-next-line @typescript-eslint/ban-ts-comment */
  // @ts-ignore: interface can in fact be extended
  server.addService(rateLimitProto.LockService, {
    authorize: authorize.bind(server),
    update: update.bind(server),
  });

  server.emit('DEBUG', {
    source: LOG_SOURCES.RPC,
    level: LOG_LEVELS.INFO,
    message: 'The rate limit service has been to the server.',
  });
};

function authorize(
  this: RpcServer,
  call: { request: RequestMetaMessage },
  callback: (a: TServiceCallbackError, b?: AuthorizationProto) => void,
) {
  try {
    const { method, url } = RequestMetaMessage.fromProto(call.request);
    const request = new BaseRequest(method, url);
    const resetAfter = this.rateLimitCache.authorizeRequestFromClient(request);

    if (resetAfter === 0) {
      const message = `Request approved. ${method} ${url}`;
      this.log('DEBUG', message);
    } else {
      const message = `Request denied. ${method} ${url}`;
      this.log('DEBUG', message);
    }

    const message = new AuthorizationMessage(resetAfter).proto;

    callback(null, message);
  } catch (err) {
    this.emit('DEBUG', {
      source: LOG_SOURCES.RPC,
      level: LOG_LEVELS.ERROR,
      message: err,
    });
    callback(err);
  }
}

function update(
  this: RpcServer,
  call: { request: RateLimitStateProto },
  callback: (a: TServiceCallbackError) => void,
) {
  try {
    const {
      requestMeta,
      global,
      bucket,
      limit,
      remaining,
      resetAfter,
    } = RateLimitStateMessage.fromProto(call.request);

    const { method, url } = requestMeta;
    const request = new BaseRequest(method, url);

    if (bucket !== undefined) {
      const rateLimitHeaders = new RateLimitHeaders(
        global,
        bucket,
        limit,
        remaining,
        resetAfter,
      );
      this.rateLimitCache.update(request, rateLimitHeaders);
    }

    const message = `Rate limit cache updated: ${method} ${url} | Remaining: ${remaining}`;
    this.log('DEBUG', message);

    callback(null);
  } catch (err) {
    this.emit('DEBUG', {
      source: LOG_SOURCES.RPC,
      level: LOG_LEVELS.ERROR,
      message: err.message,
    });
    callback(err);
  }
}
