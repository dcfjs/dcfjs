import { DCFContext } from './context';
import * as dcfc from '@dcfjs/common';
import {
  RDDWorkChain,
  RDDFinalizedWorkChain,
  finalizeChain,
  mapChain,
  runWorkChain,
  finalizeChainWithContext,
} from './chain';
import { StorageClient, StorageSession } from '@dcfjs/common';
import * as XXHash from 'js-xxhash';

function hashPartitionFunc<V>(numPartitions: number) {
  const seed = ((Math.random() * 0xffffffff) | 0) >>> 0;
  return dcfc.captureEnv(
    (data: V) => {
      return XXHash.xxHash32(dcfc.encode(data), seed) % numPartitions;
    },
    {
      numPartitions,
      seed,
      XXHash: dcfc.requireModule('js-xxhash'),
      dcfc: dcfc.requireModule('@dcfjs/common'),
    }
  );
}

export class RDD<T> {
  protected _context: DCFContext;
  readonly _chain: RDDWorkChain<T[]>;
  constructor(context: DCFContext, chain: RDDWorkChain<T[]>) {
    this._context = context;
    this._chain = chain;
  }

  getNumPartitions(): number {
    const chain = this._chain;
    return chain.n;
  }

  protected execute<T, T1, Context>(
    chain: RDDFinalizedWorkChain<T, T1, Context>
  ): Promise<T1> {
    return this._context.execute(
      dcfc.captureEnv(
        async ({ dispatchWork }) => {
          const [ret, dispose] = await runWorkChain(dispatchWork, chain);
          await dispose();
          return ret;
        },
        {
          chain,
          runWorkChain,
        }
      )
    );
  }

  union(...others: RDD<T>[]): RDD<T> {
    return this._context.union(this, ...others);
  }

  collect(): Promise<T[]> {
    return this.execute(
      finalizeChain(
        this._chain,
        dcfc.captureEnv((v) => dcfc.concatArrays(v), {
          dcfc: dcfc.requireModule('@dcfjs/common'),
        }),
        (t) => `${t}.collect()`
      )
    );
  }

  count(): Promise<number> {
    return this.execute(
      finalizeChain(
        mapChain(this._chain, (v) => v.length),
        (v) => v.reduce((a, b) => a + b, 0),
        (t) => `${t}.count()`
      )
    );
  }

  take(limit: number): Promise<T[]> {
    return this.execute(
      finalizeChain(
        mapChain(
          this._chain,
          dcfc.captureEnv((v) => v.slice(0, limit), {
            limit,
          })
        ),
        dcfc.captureEnv((v) => dcfc.takeArrays(v, limit), {
          limit,
          dcfc: dcfc.requireModule('@dcfjs/common'),
        }),
        (t) => `${t}.collect()`
      )
    );
  }

  reduce(reduceFunc: (a: T, b: T) => T): Promise<T | undefined> {
    return this.execute(
      finalizeChain(
        mapChain(
          this._chain,
          dcfc.captureEnv(
            (v) => {
              if (v.length === 0) {
                return [];
              }
              return [v.reduce(reduceFunc)];
            },
            {
              reduceFunc,
            }
          )
        ),
        dcfc.captureEnv(
          (v) => {
            const a = dcfc.concatArrays(v);
            if (a.length === 0) {
              return undefined;
            }
            return a.reduce(reduceFunc);
          },
          {
            reduceFunc,
            dcfc: dcfc.requireModule('@dcfjs/common'),
          }
        ),
        (t) => `${t}.reduce()`
      )
    );
  }

  max(
    this: RDD<number>,
    comparator?: (a: number, b: number) => number
  ): Promise<T | undefined>;
  max(comparator: (a: T, b: T) => number): Promise<T | undefined>;
  max(comparator: any = dcfc.defaultComparator): Promise<T | undefined> {
    return this.reduce(
      dcfc.captureEnv(
        (a, b) => {
          return comparator(a, b) > 0 ? a : b;
        },
        {
          comparator,
        }
      )
    );
  }

  min(
    this: RDD<number>,
    comparator?: (a: number, b: number) => number
  ): Promise<T | undefined>;
  min(comparator: (a: T, b: T) => number): Promise<T | undefined>;
  min(comparator: any = dcfc.defaultComparator): Promise<T | undefined> {
    return this.reduce(
      dcfc.captureEnv(
        (a, b) => {
          return comparator(a, b) < 0 ? a : b;
        },
        {
          comparator,
        }
      )
    );
  }

  mapPartitions<T1>(transformer: (input: T[]) => T1[]): RDD<T1> {
    return new RDD<T1>(this._context, mapChain(this._chain, transformer));
  }

  glom(): RDD<T[]> {
    return this.mapPartitions((v) => [v]);
  }

  map<T1>(transformer: (input: T) => T1): RDD<T1> {
    return this.mapPartitions(
      dcfc.captureEnv((v) => v.map((v) => transformer(v)), { transformer })
    );
  }

  flatMap<T1>(transformer: (input: T) => T1[]): RDD<T1> {
    return this.mapPartitions(
      dcfc.captureEnv((v) => dcfc.concatArrays(v.map((v) => transformer(v))), {
        transformer,
        dcfc: dcfc.requireModule('@dcfjs/common'),
      })
    );
  }

  filter(filterFunc: (input: T) => boolean): RDD<T> {
    return this.mapPartitions(
      dcfc.captureEnv((v) => v.filter((v) => filterFunc(v)), { filterFunc })
    );
  }

  partitionBy(numPartitions: number, partitionFunc: (v: T) => number): RDD<T> {
    const storage = this._context.getStorage();

    const dep = finalizeChainWithContext(
      this._chain,
      dcfc.captureEnv(
        () => {
          return storage.startSession();
        },
        {
          storage,
        }
      ),
      dcfc.captureEnv(
        (partitionId, ctx, pp) => {
          return dcfc.captureEnv(
            async () => {
              const data = await pp();
              const regrouped: T[][] = [];
              for (let i = 0; i < numPartitions; i++) {
                regrouped[i] = [];
              }
              for (const item of data) {
                const parId = partitionFunc(item);
                regrouped[parId].push(item);
              }

              const ret: (string | null)[] = [];
              const promises = [];
              for (let i = 0; i < numPartitions; i++) {
                if (regrouped[i].length === 0) {
                  ret.push(null);
                  continue;
                }
                const key = `${partitionId}-${i}`;
                const buf = dcfc.encode(regrouped[i]);
                promises.push(ctx.writeFile(key, buf));
                ret.push(key);
              }
              await Promise.all(promises);
              return ret;
            },
            {
              partitionId,
              numPartitions,
              partitionFunc,
              pp,
              ctx,
              dcfc: dcfc.requireModule('@dcfjs/common'),
            }
          );
        },
        {
          numPartitions,
          partitionFunc,
          dcfc: dcfc.requireModule('@dcfjs/common'),
        }
      ),
      (keys, ctx) => [keys, ctx],
      (ctx) => ctx.close(),
      (title) => `${title}.repartition()`
    );

    return new RDD(this._context, {
      n: numPartitions,
      p: dcfc.captureEnv(
        (partitionId, dependRets) => {
          const [allKeys, ctx] = dependRets[0] as [
            (string | null)[][],
            dcfc.StorageSession
          ];
          const keys = allKeys.map((v) => v[partitionId]);
          return dcfc.captureEnv(
            () => {
              const loads: (T[] | Promise<T[]>)[] = [];
              for (const key of keys) {
                if (key) {
                  loads.push(
                    ctx.readFile(key).then((buf) => dcfc.decode(buf) as T[])
                  );
                } else {
                  loads.push([]);
                }
              }
              return Promise.all(loads).then((pieces) =>
                dcfc.concatArrays(pieces)
              );
            },
            {
              keys,
              ctx,
              dcfc: dcfc.requireModule('@dcfjs/common'),
            }
          );
        },
        {
          dcfc: dcfc.requireModule('@dcfjs/common'),
        }
      ),
      t: 'repartition()',
      d: [dep as RDDFinalizedWorkChain],
    });
  }

  async cache(storage?: StorageClient) {
    if (!storage) {
      if (!this._context.storage) {
        throw new Error('No storage available.');
      }
      storage = this._context.storage;
    }
    const session = await storage.startSession();
    const keys = await this.execute(
      finalizeChain(
        mapChain(
          this._chain,
          dcfc.captureEnv(
            async (data, partitionId) => {
              if (data.length === 0) {
                return null;
              }
              const key = `${partitionId}`;
              const buf = dcfc.encode(data);
              await session.writeFile(key, buf);
              return key;
            },
            {
              session,
              dcfc: dcfc.requireModule('@dcfjs/common'),
            }
          )
        ),
        (v) => {
          return v;
        },
        (t) => `${t}.reduce()`
      )
    );
    return new CachedRDD(this._context, session, keys);
  }

  async persist(storage?: StorageClient) {
    return this.cache(storage);
  }

  coalesce(numPartitions: number) {
    const originPartitions = this._chain.n;

    // [index: originPartitionId]: [newPartitionIdBase, [rate for each newPartition] ]
    const partitionArgs: [number, number[]][] = [];
    let last: number[] = [];
    partitionArgs.push([0, last]);
    const rate = originPartitions / numPartitions;

    let counter = 0;
    for (let i = 0; i < numPartitions - 1; i++) {
      counter += rate;
      while (counter >= 1) {
        counter -= 1;
        last = [];
        partitionArgs.push([i, last]);
      }
      last.push(counter);
    }
    // manually add last partition to avoid precsion loss.
    while (partitionArgs.length < originPartitions) {
      partitionArgs.push([numPartitions - 1, []]);
    }

    const storage = this._context.getStorage();

    const dep = finalizeChainWithContext(
      this._chain,
      dcfc.captureEnv(
        () => {
          return storage.startSession();
        },
        {
          storage,
        }
      ),
      dcfc.captureEnv(
        (partitionId, ctx, pp) => {
          const arg = partitionArgs[partitionId];
          return dcfc.captureEnv(
            async () => {
              const data = await pp();
              const regrouped: T[][] = [];
              for (let i = 0; i < arg[0]; i++) {
                regrouped.push([]);
              }
              let lastIndex = 0;
              for (const rate of arg[1]) {
                const nextIndex = Math.floor(data.length * rate);
                regrouped.push(data.slice(lastIndex, nextIndex));
                lastIndex = nextIndex;
              }
              regrouped.push(data.slice(lastIndex));

              const ret: (string | null)[] = [];
              const promises = [];
              for (let i = 0; i < regrouped.length; i++) {
                if (regrouped[i].length === 0) {
                  ret.push(null);
                  continue;
                }
                const key = `${partitionId}-${i}`;
                const buf = dcfc.encode(regrouped[i]);
                promises.push(ctx.writeFile(key, buf));
                ret.push(key);
              }
              await Promise.all(promises);
              return ret;
            },
            {
              partitionId,
              arg,
              pp,
              ctx,
              dcfc: dcfc.requireModule('@dcfjs/common'),
            }
          );
        },
        {
          numPartitions,
          partitionArgs,
          dcfc: dcfc.requireModule('@dcfjs/common'),
        }
      ),
      (keys, ctx) => [keys, ctx],
      (ctx) => ctx.close(),
      (title) => `${title}.coalesce()`
    );

    return new RDD(this._context, {
      n: numPartitions,
      p: dcfc.captureEnv(
        (partitionId, dependRets) => {
          const [allKeys, ctx] = dependRets[0] as [
            (string | null)[][],
            dcfc.StorageSession
          ];
          const keys = allKeys.map((v) => v[partitionId]);
          return dcfc.captureEnv(
            () => {
              const loads: (T[] | Promise<T[]>)[] = [];
              for (const key of keys) {
                if (key) {
                  loads.push(
                    ctx.readFile(key).then((buf) => dcfc.decode(buf) as T[])
                  );
                } else {
                  loads.push([]);
                }
              }
              return Promise.all(loads).then((pieces) =>
                dcfc.concatArrays(pieces)
              );
            },
            {
              keys,
              ctx,
              dcfc: dcfc.requireModule('@dcfjs/common'),
            }
          );
        },
        {
          dcfc: dcfc.requireModule('@dcfjs/common'),
        }
      ),
      t: 'coalesce()',
      d: [dep as RDDFinalizedWorkChain],
    });
  }

  repartition(numPartitions: number): RDD<T> {
    return this.partitionBy(
      numPartitions,
      dcfc.captureEnv(() => Math.floor(Math.random() * numPartitions), {
        numPartitions,
      })
    );
  }

  distinct(numPartitions?: number): RDD<T> {
    numPartitions = numPartitions || this._context.options.defaultPartitions;
    if (!numPartitions || numPartitions <= 0) {
      throw new Error('Must specify partitions count.');
    }

    const partitionMapper = dcfc.captureEnv(
      (datas) => {
        const ret = [];
        const map: { [key: string]: T } = {};
        for (const item of datas) {
          const k = dcfc.encode(item).toString('base64');
          if (!map[k]) {
            map[k] = item;
            ret.push(item);
          }
        }
        return ret;
      },
      {
        dcfc: dcfc.requireModule('@dcfjs/common'),
      }
    );

    return this.mapPartitions(partitionMapper)
      .partitionBy(numPartitions, hashPartitionFunc<T>(numPartitions))
      .mapPartitions(partitionMapper);
  }

  combineByKey<K, V, C>(
    this: RDD<[K, V]>,
    createCombiner: (a: V) => C,
    mergeValue: (a: C, b: V) => C,
    mergeCombiners: (a: C, b: C) => C,
    numPartitions?: number,
    partitionFunc?: (v: K) => number
  ): RDD<[K, C]> {
    numPartitions = numPartitions || this._context.options.defaultPartitions;
    if (!numPartitions || numPartitions <= 0) {
      throw new Error('Must specify partitions count.');
    }
    let pFunc = partitionFunc || hashPartitionFunc<K>(numPartitions);

    const mapFunction1 = dcfc.captureEnv(
      (datas: [K, V][]) => {
        const ret = [];
        const map: { [key: string]: [K, C] } = {};
        for (const item of datas) {
          const k = dcfc.encode(item[0]).toString('base64');
          let r = map[k];
          if (!r) {
            r = [item[0], createCombiner(item[1])];
            map[k] = r;
            ret.push(r);
          } else {
            r[1] = mergeValue(r[1], item[1]);
          }
        }
        return ret;
      },
      {
        createCombiner,
        mergeValue,
        dcfc: dcfc.requireModule('@dcfjs/common'),
      }
    );

    const mapFunction2 = dcfc.captureEnv(
      (datas: [K, C][]) => {
        const ret = [];
        const map: { [key: string]: [K, C] } = {};
        for (const item of datas) {
          const k = dcfc.encode(item[0]).toString('base64');
          let r = map[k];
          if (!r) {
            r = [item[0], item[1]];
            map[k] = r;
            ret.push(r);
          } else {
            r[1] = mergeCombiners(r[1], item[1]);
          }
        }
        return ret;
      },
      {
        mergeCombiners,
        dcfc: dcfc.requireModule('@dcfjs/common'),
      }
    );

    const realPartitionFunc = dcfc.captureEnv(
      (data: [K, C]) => {
        return pFunc(data[0]);
      },
      {
        pFunc,
      }
    );

    return this.mapPartitions<[K, C]>(mapFunction1)
      .partitionBy(numPartitions, realPartitionFunc)
      .mapPartitions<[K, C]>(mapFunction2);
  }

  reduceByKey<K, V>(
    this: RDD<[K, V]>,
    func: (a: V, B: V) => V,
    numPartitions?: number,
    partitionFunc?: (v: K) => number
  ): RDD<[K, V]> {
    return this.combineByKey(
      (x) => x,
      func,
      func,
      numPartitions,
      partitionFunc
    );
  }
}

export class CachedRDD<T> extends RDD<T> {
  storageSession: StorageSession;

  constructor(
    context: DCFContext,
    storageSession: StorageSession,
    keys: (string | null)[]
  ) {
    super(context, {
      n: keys.length,
      p: dcfc.captureEnv(
        (partitionId) => {
          const key = keys[partitionId];
          return dcfc.captureEnv(
            async () => {
              if (!key) {
                return [];
              }
              const buf = await storageSession.readFile(key);
              return dcfc.decode(buf) as T[];
            },
            {
              storageSession,
              key,
              dcfc: dcfc.requireModule('@dcfjs/common'),
            }
          );
        },
        {
          storageSession,
          keys,
          dcfc: dcfc.requireModule('@dcfjs/common'),
        }
      ),
      t: 'cache()',
      d: [],
    });
    this.storageSession = storageSession;
  }

  async unpersist(): Promise<void> {
    await this.storageSession.close();
  }
}
