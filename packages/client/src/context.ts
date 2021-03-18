import * as grpc from '@grpc/grpc-js';
import { MasterServiceClient } from '@dcfjs/proto/dcf/MasterService';
import {
  encode,
  decode,
  MasterExecFunction,
  protoDescriptor,
  serializeFunction,
} from '@dcfjs/common';
import * as dcfc from '@dcfjs/common';
import { RDD } from './rdd';

export interface DCFMapReduceOptions {
  client?: MasterServiceClient;
  masterEndpoint?: string;
  defaultPartitions?: number;
}

export class DCFContext {
  readonly client: MasterServiceClient;
  readonly options: DCFMapReduceOptions;
  constructor(options: DCFMapReduceOptions = {}) {
    this.options = options;
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
          if (!msg) {
            break;
          }
          if (msg.result) {
            resolve(decode(msg.result) as T);
          } else if (msg.errorMessage) {
            reject(new Error(msg.errorMessage));
          }
        }
      });
    });
  }

  range(to: number): RDD<number>;
  range(
    from: number,
    to?: number,
    step?: number,
    numPartitions?: number
  ): RDD<number>;
  range(
    from: number,
    to?: number,
    step: number = 1,
    numPartitions?: number
  ): RDD<number> {
    if (to == null) {
      to = from;
      from = 0;
    }
    numPartitions = numPartitions || this.options.defaultPartitions;
    if (!numPartitions || numPartitions < 0) {
      throw new Error('Must specify partitions count.');
    }

    const finalCount = Math.ceil((to - from) / step);
    const rest = finalCount % numPartitions;
    const eachCount = (finalCount - rest) / numPartitions;

    return new RDD<number>(this, {
      n: numPartitions,
      p: dcfc.captureEnv(
        (partitionId) => {
          return dcfc.captureEnv(
            () => {
              let start, end;
              if (partitionId < rest) {
                start = partitionId * (eachCount + 1);
                end = start + eachCount + 1;
              } else {
                start = partitionId * eachCount + rest;
                end = start + eachCount;
              }
              const ret = [];
              for (let i = start; i < end; i++) {
                ret.push(i);
              }
              return ret;
            },
            {
              partitionId,
              eachCount,
              rest,
            }
          );
        },
        {
          eachCount,
          rest,
          dcfc: dcfc.requireModule('@dcfjs/common'),
        }
      ),
      t: `Range(${from},${to})`,
      d: [],
    });
  }
}
