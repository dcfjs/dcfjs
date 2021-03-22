import { StorageClient } from './../../common/src/storage';
import * as grpc from '@grpc/grpc-js';
import { MasterServiceClient } from '@dcfjs/proto/dcf/MasterService';
import {
  MasterExecFunction,
  protoDescriptor,
  serializeFunction,
} from '@dcfjs/common';
import * as dcfc from '@dcfjs/common';
import { RDD } from './rdd';
import { PartitionFunc } from './chain';
import * as v8 from 'v8';

export interface DCFMapReduceOptions {
  client?: MasterServiceClient;
  masterEndpoint?: string;
  defaultPartitions?: number;
}

export class DCFContext {
  readonly client: MasterServiceClient;
  readonly options: DCFMapReduceOptions;
  readonly storages: { [key: string]: StorageClient } = {};
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
        func: v8.serialize(serializeFunction(f)),
      });
      stream.on('error', reject);
      stream.on('readable', () => {
        while (stream.readable) {
          const msg = stream.read();
          if (!msg) {
            break;
          }
          if (msg.result) {
            resolve(v8.deserialize(msg.result) as T);
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
                ret.push(i * step + from);
              }
              return ret;
            },
            {
              partitionId,
              eachCount,
              rest,
              step,
              from,
            }
          );
        },
        {
          eachCount,
          rest,
          step,
          from,
          dcfc: dcfc.requireModule('@dcfjs/common'),
        }
      ),
      t: `range(${from},${to})`,
      d: [],
    });
  }

  union<T>(...rdds: RDD<T>[]): RDD<T> {
    const partitionCounts: number[] = [];
    const rddFuncs: PartitionFunc<T[]>[] = [];
    for (let i = 0; i < rdds.length; i++) {
      partitionCounts.push(rdds[i]._chain.n);
      rddFuncs.push(rdds[i]._chain.p);
    }
    const numPartitions = partitionCounts.reduce((a, b) => a + b);
    return new RDD<T>(this, {
      n: numPartitions,
      p: dcfc.captureEnv(
        (partitionId) => {
          for (let i = 0; i < partitionCounts.length; i++) {
            if (partitionId < partitionCounts[i]) {
              return rddFuncs[i](partitionId);
            }
            partitionId -= partitionCounts[i];
          }
          // `partitionId` should be less than totalPartitions.
          // so it should not reach here.
          throw new Error('Internal error.');
        },
        {
          rddFuncs,
          partitionCounts,
        }
      ),
      t: `union(${rdds.map((v) => v._chain.t).join(',')})`,
      d: dcfc.concatArrays(rdds.map((v) => v._chain.d)),
    });
  }

  emptyRDD(): RDD<never> {
    return new RDD<never>(this, {
      n: 0,
      p: () => () => [],
      t: 'emptyRDD()',
      d: [],
    });
  }
}
