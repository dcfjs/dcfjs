import { WorkerExecFunction } from '@dcfjs/common/src/proto';
import * as grpc from '@grpc/grpc-js';
import {
  protoDescriptor,
  decode,
  deserializeFunction,
  SerializedFunction,
} from '@dcfjs/common';
import { WorkerServiceHandlers } from '@dcfjs/proto/dcf/WorkerService';
import * as v8 from 'v8';

export function createWorkerServer() {
  const server = new grpc.Server();

  const handlers: WorkerServiceHandlers = {
    async exec({ request }, cb) {
      try {
        if (!request.func) {
          throw { code: grpc.status.INVALID_ARGUMENT };
        }
        const func = deserializeFunction<WorkerExecFunction>(
          v8.deserialize(request.func) as SerializedFunction
        );
        const ret = await func();
        cb(null, {
          result: v8.serialize(ret),
        });
        if (global.gc) {
          global.gc();
        }
      } catch (e) {
        console.warn(e.stack);
        cb(e);
      }
    },
  };

  server.addService(protoDescriptor.dcf.WorkerService.service, handlers);

  return server;
}
