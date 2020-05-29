import { ChannelCredentials } from '@grpc/grpc-js';
import { Options } from '@grpc/proto-loader';

export interface IServerOptions {
  /** Server host to connect to. */
  host: string;
  /** Server port to connect to. */
  port: number | string;
  /** GRPC channel to connect to server over. */
  channel: ChannelCredentials;
  /** If the service is allowed to fallback on alternate functionality. Defined differently for each service. */
  allowFallback: boolean;
  /** Additional protobuf options. */
  protoOptions: Options;
}

export interface ILockServiceOptions extends IServerOptions{
  /** How long ion ms the client will request the lock for when acquiring. */
  duration: number;
}

export type DebugLevel = 'FATAL' | 'ERROR' | 'WARNING' | 'INFO' | 'DEBUG';
