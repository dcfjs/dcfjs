import * as protoLoader from '@grpc/proto-loader';
import { ProtoGrpcType } from '@dcfjs/proto';
import { SerializedFunction, FunctionEnv } from './serializeFunction';
export declare const PROTO_ROOT_FN: string;
export declare const packageDefinition: protoLoader.PackageDefinition;
export declare const protoDescriptor: ProtoGrpcType;
export declare type WorkerExecFunction<T = unknown> = () => T | Promise<T>;
export interface MasterExecParams {
    dispatchWork: <T = unknown>(func: SerializedFunction | WorkerExecFunction<T>, env?: FunctionEnv) => Promise<T>;
}
export declare type MasterExecFunction<T = unknown> = (params: MasterExecParams) => T | Promise<T>;
