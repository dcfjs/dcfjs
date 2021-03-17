import * as grpc from '@grpc/grpc-js';
import { SerializedFunction, FunctionEnv } from '@dcfjs/common';
export declare function dispatchWork<T = any>(func: SerializedFunction | (() => T | Promise<T>), env?: FunctionEnv): Promise<T>;
export declare function createMasterServer(): grpc.Server;
