import * as protoLoader from '@grpc/proto-loader';
import * as grpc from '@grpc/grpc-js';
import { ProtoGrpcType } from '@dcfjs/proto';
import { SerializedFunction, FunctionEnv } from './serializeFunction';

export const PROTO_ROOT_FN = require.resolve('@dcfjs/proto/proto/index.proto');

export const packageDefinition = protoLoader.loadSync(PROTO_ROOT_FN);
export const protoDescriptor = (grpc.loadPackageDefinition(
  packageDefinition
) as unknown) as ProtoGrpcType;

export type WorkerExecFunction<T = unknown> = () => T | Promise<T>;

export interface MasterExecParams {
  dispatchWork: <T = unknown>(
    func: SerializedFunction | WorkerExecFunction<T>,
    env?: FunctionEnv
  ) => Promise<T>;
}

export type MasterExecFunction<T = unknown> = (
  params: MasterExecParams
) => T | Promise<T>;
