import * as grpc from '@grpc/grpc-js';
import { MasterServiceClient } from '@dcfjs/proto/dcf/MasterService';
import {
  MasterExecFunction,
  protoDescriptor,
  serializeFunction,
  StorageClient,
} from '@dcfjs/common';
import * as dcfc from '@dcfjs/common';
import { RDD } from './rdd';
import { InitializeFunc, PartitionFunc } from './chain';
import * as v8 from 'v8';
import { listFiles } from './fs-helper';

export interface DCFMapReduceOptions {
  client?: MasterServiceClient;
  masterEndpoint?: string;
  defaultPartitions?: number;
  storage?: StorageClient;
}

export class DCFContext {
  readonly client: MasterServiceClient;
  readonly options: DCFMapReduceOptions;
  readonly storage: StorageClient | null = null;

  constructor(options: DCFMapReduceOptions = {}) {
    this.options = options;
    if (options.storage) {
      this.storage = options.storage;
    }
    if (options.client) {
      this.client = options.client;
    } else {
      this.client = new protoDescriptor.dcf.MasterService(
        options.masterEndpoint || 'localhost:17731',
        grpc.credentials.createInsecure()
      );
    }
  }

  getStorage() {
    if (!this.storage) {
      throw new Error('No storage provided.');
    }
    return this.storage;
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

  parallelize<T>(arr: T[], numPartitions?: number): RDD<T> {
    numPartitions = numPartitions || this.options.defaultPartitions;
    if (!numPartitions || numPartitions <= 0) {
      throw new Error('Must specify partitions count.');
    }
    const args: T[][] = [];

    const rest = arr.length % numPartitions;
    const eachCount = (arr.length - rest) / numPartitions;

    let index = 0;
    for (let i = 0; i < numPartitions; i++) {
      const subCount = i < rest ? eachCount + 1 : eachCount;
      const end = index + subCount;
      args.push(arr.slice(index, end));
      index = end;
    }

    return new RDD<T>(this, {
      n: numPartitions,
      p: dcfc.captureEnv(
        (partitionId) => {
          const data = args[partitionId];
          return dcfc.captureEnv(() => data, {
            data,
          });
        },
        {
          args,
          dcfc: dcfc.requireModule('@dcfjs/common'),
        }
      ),
      t: 'parallelize()',
      d: [],
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
    if (!numPartitions || numPartitions <= 0) {
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
    const chainsPromise = Promise.all(rdds.map((v) => v._chain));

    const chainPromise = chainsPromise.then((chains) => {
      const partitionCounts: number[] = [];
      const rddFuncs: PartitionFunc<T[], void>[] = [];
      const dependCounts: number[] = [];

      for (let i = 0; i < rdds.length; i++) {
        partitionCounts.push(chains[i].n);
        rddFuncs.push(chains[i].p);
        dependCounts.push(chains[i].d.length);
      }
      const numPartitions = partitionCounts.reduce((a, b) => a + b);
      return {
        n: numPartitions,
        p: dcfc.captureEnv(
          (partitionId, dependValues) => {
            let dependStart = 0;
            for (let i = 0; i < partitionCounts.length; i++) {
              if (partitionId < partitionCounts[i]) {
                return rddFuncs[i](
                  partitionId,
                  dependValues.slice(dependStart, dependStart + dependCounts[i])
                );
              }
              partitionId -= partitionCounts[i];
              dependStart += dependCounts[i];
            }
            // `partitionId` should be less than totalPartitions.
            // so it should not reach here.
            throw new Error('Internal error.');
          },
          {
            rddFuncs,
            partitionCounts,
            dependCounts,
          }
        ),
        t: `union(${chains.map((v) => v.t).join(',')})`,
        d: dcfc.concatArrays(chains.map((v) => v.d)),
      };
    });

    return new RDD<T>(this, chainPromise);
  }

  emptyRDD(): RDD<never> {
    return new RDD<never>(this, {
      n: 0,
      p: () => () => [],
      t: 'emptyRDD()',
      d: [],
    });
  }

  binaryFiles(
    path: string,
    {
      recursive = false,
    }: {
      recursive?: boolean;
    } = {}
  ): RDD<[string, Buffer]> {
    throw new Error('TODO');
  }
}
