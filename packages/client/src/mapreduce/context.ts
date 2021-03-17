import { WorkerExecFunction } from './../../../common/src/proto';
import * as grpc from '@grpc/grpc-js';
import { MasterServiceClient } from '@dcfjs/proto/dcf/MasterService';
import {
  encode,
  decode,
  MasterExecFunction,
  protoDescriptor,
  serializeFunction,
  requireModule,
  captureEnv,
} from '@dcfjs/common';
import * as dcfCommon from '@dcfjs/common';

export interface DCFMapReduceOptions {
  client?: MasterServiceClient;
  masterEndpoint?: string;
}

export class DCFMapReduceContext {
  readonly client: MasterServiceClient;
  constructor(options: DCFMapReduceOptions = {}) {
    if (options.client) {
      this.client = options.client;
    } else {
      this.client = new protoDescriptor.dcf.MasterService(
        options.masterEndpoint || 'localhost:17731',
        grpc.credentials.createInsecure()
      );
    }
  }

  execute<T>(f: MasterExecFunction<T>) {
    return new Promise<T>((resolve, reject) => {
      const stream = this.client.exec({
        func: encode(serializeFunction(f)),
      });
      stream.on('error', reject);
      stream.on('readable', () => {
        while (stream.readable) {
          const msg = stream.read();
          if (msg) {
            if (msg.result) {
              resolve(decode(msg.result) as T);
            } else if (msg.errorMessage) {
              reject(new Error(msg.errorMessage));
            }
          }
        }
      });
    });
  }

  executeByPartition<T, T1>(
    numPartitions: number,
    partitionFunc: (partitionId: number) => WorkerExecFunction<T>,
    finalFunc: (v: T[]) => T1 | Promise<T1>
  ): Promise<T1> {
    return this.execute(
      captureEnv(
        async ({ dispatchWork }) => {
          const partitionResults = await Promise.all(
            new Array(numPartitions).fill(0).map((v, i) => {
              const f = partitionFunc(i);
              return dispatchWork(f);
            })
          );

          return finalFunc(partitionResults);
        },
        {
          dcfCommon: requireModule('@dcfjs/common'),
          numPartitions,
          partitionFunc,
          finalFunc,
        }
      )
    );
  }
}
