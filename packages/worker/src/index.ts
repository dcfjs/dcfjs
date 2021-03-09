import { WorkerStatus } from '@dcfjs/proto/dcf/WorkerStatus';
import * as grpc from '@grpc/grpc-js';
import {
  protoDescriptor,
  decode,
  deserializeFunction,
  SerializedFunction,
  encode,
} from '@dcfjs/common';
import { WorkerServiceHandlers } from '@dcfjs/proto/dcf/WorkerService';

export function createWorkerServer() {
  const server = new grpc.Server();

  const handlers: WorkerServiceHandlers = {
    async exec({ request }, cb) {
      try {
        if (!request.func) {
          throw { code: grpc.status.INVALID_ARGUMENT };
        }
        const func = deserializeFunction(
          decode(request.func) as SerializedFunction
        );
        const ret = await func();
        cb(null, {
          result: encode(ret),
        });
      } catch (e) {
        cb(e);
      }
    },
  };

  server.addService(protoDescriptor.dcf.WorkerService.service, handlers);

  return server;
}
