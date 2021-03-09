import { WorkerExecResponse__Output } from './../../proto/dcf/WorkerExecResponse';
import { WorkerServiceClient } from '@dcfjs/proto/dcf/WorkerService';
import { MasterServiceHandlers } from '@dcfjs/proto/dcf/MasterService';
import { WorkerStatus } from '@dcfjs/proto/dcf/WorkerStatus';
import * as grpc from '@grpc/grpc-js';
import {
  protoDescriptor,
  encode,
  SerializedFunction,
  decode,
} from '@dcfjs/common';

const PING_INTERVAL = 60000;

const workers = new Map<string, WorkerClient>();

class WorkerClient {
  readonly endpoint: string;
  readonly client: WorkerServiceClient;
  status: WorkerStatus = WorkerStatus.UNKNOWN;

  currentTask: Promise<void> | null = null;

  constructor(endpoint: string) {
    this.endpoint = endpoint;
    this.client = new protoDescriptor.dcf.WorkerService(
      endpoint,
      grpc.credentials.createInsecure()
    );
    workers.set(endpoint, this);
  }

  close() {
    this.client.close();
    workers.delete(this.endpoint);
  }

  ping = async () => {
    const result = await new Promise<WorkerExecResponse__Output | undefined>(
      (resolve, reject) =>
        this.client.exec(
          {
            func: encode({
              __type: 'function',
              source: 'function () {return "pong";}',
              args: [],
              values: [],
            } as SerializedFunction),
          },
          (err, result) => (err ? reject(err) : resolve(result))
        )
    );
    if (!result || !result.result || decode(result.result) !== 'pong') {
      throw new Error('Invalid response.');
    }
  };
}

setInterval(() => {
  for (const worker of workers.values()) {
    if (worker.status === WorkerStatus.READY) {
      worker.ping().catch((e) => (worker.status = WorkerStatus.ERROR));
    }
  }
}, PING_INTERVAL);

export function createMasterServer() {
  const server = new grpc.Server();

  const handlers: MasterServiceHandlers = {
    async registerWorker({ request }, cb) {
      try {
        if (!request.endpoint) {
          throw {
            code: grpc.status.INVALID_ARGUMENT,
          };
        }
        const worker = new WorkerClient(request.endpoint);
        try {
          await worker.ping();
        } catch (e) {
          worker.close();
          throw e;
        }
        console.log(`Worker ${request.endpoint} registerd.`);
        cb(null);
      } catch (e) {
        cb(e);
      }
    },
    async unregisterWorker({ request }, cb) {
      try {
        if (!request.endpoint) {
          throw {
            code: grpc.status.INVALID_ARGUMENT,
          };
        }
        const worker = workers.get(request.endpoint);
        if (!worker) {
          cb(null);
          return;
        }
        worker.status = WorkerStatus.SHUTDOWN;
        if (worker.currentTask) {
          console.log(`Worker ${request.endpoint} unregistering.`);
          await worker.currentTask;
        }
        worker.close();
        console.log(`Worker ${request.endpoint} unregisterd.`);
        cb(null);
      } catch (e) {
        cb(e);
      }
    },
  };

  server.addService(protoDescriptor.dcf.MasterService.service, handlers);

  return server;
}
