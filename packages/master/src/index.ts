import { WorkerExecResponse__Output } from '@dcfjs/proto/dcf/WorkerExecResponse';
import { WorkerServiceClient } from '@dcfjs/proto/dcf/WorkerService';
import { MasterServiceHandlers } from '@dcfjs/proto/dcf/MasterService';
import { WorkerStatus } from '@dcfjs/proto/dcf/WorkerStatus';
import * as grpc from '@grpc/grpc-js';
import {
  protoDescriptor,
  encode,
  SerializedFunction,
  decode,
  serializeFunction,
  FunctionEnv,
  deserializeFunction,
  MasterExecFunction,
} from '@dcfjs/common';

const workers = new Map<string, WorkerClient>();

const idleList: WorkerClient[] = [];

type Work<T> = [SerializedFunction, (result: T) => void, (reason: any) => void];
const pendingList: Work<any>[] = [];

class WorkerClient {
  readonly endpoint: string;
  readonly client: WorkerServiceClient;
  status: WorkerStatus = WorkerStatus.UNKNOWN;

  currentTask: Promise<unknown> | null = null;

  idleListPos: number | null = null;

  constructor(endpoint: string) {
    this.endpoint = endpoint;
    this.client = new protoDescriptor.dcf.WorkerService(
      endpoint,
      grpc.credentials.createInsecure()
    );
    workers.set(endpoint, this);
  }

  close() {
    this.status = WorkerStatus.SHUTDOWN;
    this.client.close();
    workers.delete(this.endpoint);
    this._removeFromIdleList();
  }

  async initReady() {
    try {
      await new Promise((resolve, reject) =>
        this.client.waitForReady(Infinity, (err) =>
          err ? reject(err) : resolve(null)
        )
      );
      const channel = this.client.getChannel();
      const state = channel.getConnectivityState(false);
      if (state !== grpc.connectivityState.READY) {
        throw new Error('Bad connection state');
      }
      channel.watchConnectivityState(state, Infinity, () => {
        console.log(`Worker ${this.endpoint} connection lost.`);
        this.close();
      });

      this.status = WorkerStatus.READY;
      this._addToIdleList();
    } catch (e) {
      this.close();
    }
  }

  private _becomeIdle() {
    if (this.status !== WorkerStatus.READY) {
      return;
    }
    if (pendingList.length) {
      const [func, resolve, reject] = pendingList.shift()!;
      this.handleWork<any>(func).then(resolve, reject);
    } else {
      this._addToIdleList();
    }
  }
  private _addToIdleList() {
    this.idleListPos = idleList.length;
    idleList.push(this);
  }
  private _removeFromIdleList() {
    if (this.idleListPos != null) {
      if (this.idleListPos === idleList.length - 1) {
        idleList.pop();
      } else {
        const tmp = idleList.pop()!;
        idleList[this.idleListPos] = tmp;
        tmp.idleListPos = this.idleListPos;
      }
      this.idleListPos = null;
    }
  }

  private handleWork<T>(func: SerializedFunction): Promise<T> {
    const result = new Promise<WorkerExecResponse__Output | undefined>(
      (resolve, reject) =>
        this.client.exec(
          {
            func: encode(func),
          },
          (err, result) => {
            err ? reject(err) : resolve(result);
            this._becomeIdle();
          }
        )
    );

    this.currentTask = result;

    return result.then((v) => {
      if (!v || !v.result) {
        return (undefined as unknown) as T;
      }
      return decode(v.result) as T;
    });
  }

  static dispatchWork<T>(
    func: SerializedFunction,
    resolve: (result: T) => void,
    reject: (reason: any) => void
  ) {
    const worker = idleList.pop();
    if (worker) {
      worker.idleListPos = null;
      worker.handleWork<T>(func).then(resolve, reject);
    } else if (workers.size === 0) {
      reject(new Error('No workers available.'));
    } else {
      pendingList.push([func, resolve, reject]);
    }
  }
}

export function dispatchWork<T = any>(
  func: SerializedFunction | (() => T | Promise<T>),
  env?: FunctionEnv
): Promise<T> {
  if (typeof func === 'function') {
    func = serializeFunction(func, env);
  }
  return new Promise<T>((resolve, reject) => {
    WorkerClient.dispatchWork(func as SerializedFunction, resolve, reject);
  });
}

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
        await worker.initReady();
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
          await worker.currentTask.catch(() => {});
        }
        worker.close();
        console.log(`Worker ${request.endpoint} unregisterd.`);
        cb(null);
      } catch (e) {
        cb(e);
      }
    },
    async exec(call) {
      const { request } = call;
      try {
        if (!request.func) {
          throw { code: grpc.status.INVALID_ARGUMENT };
        }
        const func = deserializeFunction<MasterExecFunction>(
          decode(request.func) as SerializedFunction
        );
        const ret = await func({ dispatchWork });
        call.write({
          result: encode(ret),
        });

        if (global.gc) {
          global.gc();
        }
      } catch (e) {
        console.warn(e.stack);
        call.write({
          errorMessage: e.message,
        });
      }
      call.end();
    },
  };

  server.addService(protoDescriptor.dcf.MasterService.service, handlers);

  return server;
}
