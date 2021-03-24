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
import { toUrl } from './fs-helper';
import { URL } from 'url';

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

function realGroupWith<K>(
  rdds: RDD<[K, any]>[],
  context: DCFContext,
  numPartitions?: number
): RDD<[K, any[][]]> {
  const rddCount = rdds.length;

  return context
    .union(
      ...rdds.map((v, i) =>
        v.map(
          dcfc.captureEnv(
            ([k, v]) => {
              const ret: any[][] = [];
              for (let j = 0; j < rddCount; j++) {
                ret.push(j === i ? [v] : []);
              }
              return [k, ret] as [K, any[][]];
            },
            { rddCount, i }
          )
        )
      )
    )
    .reduceByKey((a: any[][], b: any[][]) => {
      const ret = [];
      for (let i = 0; i < a.length; i++) {
        ret.push(a[i].concat(b[i]));
      }
      return ret;
    }, numPartitions);
}

export class RDD<T> {
  protected _context: DCFContext;
  readonly _chain: (metaonly?: boolean) => Promise<RDDWorkChain<T[]>>;
  constructor(
    context: DCFContext,
    chain: (metaonly?: boolean) => Promise<RDDWorkChain<T[]>>
  ) {
    this._context = context;
    this._chain = chain;
  }

  getNumPartitions(): Promise<number> {
    return this._chain(true).then((v) => v.n);
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
    return this._chain()
      .then((chain) =>
        finalizeChain(
          chain,
          dcfc.captureEnv((v) => dcfc.concatArrays(v), {
            dcfc: dcfc.requireModule('@dcfjs/common'),
          }),
          (t) => `${t}.collect()`
        )
      )
      .then((chain) => this.execute(chain));
  }

  count(): Promise<number> {
    return this._chain()
      .then((chain) =>
        finalizeChain(
          mapChain(chain, (v) => v.length),
          (v) => v.reduce((a, b) => a + b, 0),
          (t) => `${t}.count()`
        )
      )
      .then((chain) => this.execute(chain));
  }

  take(limit: number): Promise<T[]> {
    return this._chain()
      .then((chain) =>
        finalizeChain(
          mapChain(
            chain,
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
      )
      .then((chain) => this.execute(chain));
  }

  reduce(reduceFunc: (a: T, b: T) => T): Promise<T | undefined> {
    return this._chain()
      .then((chain) =>
        finalizeChain(
          mapChain(
            chain,
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
      )
      .then((chain) => this.execute(chain));
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

  mapPartitions<T1>(
    transformer: (input: T[], partitionId: number) => T1[]
  ): RDD<T1> {
    return new RDD<T1>(this._context, () =>
      this._chain().then((chain) => mapChain(chain, transformer))
    );
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

  partitionBy(
    numPartitions: number,
    partitionFunc: (v: T, partitionId: number, index: number) => number
  ): RDD<T> {
    const storage = this._context.getStorage();

    const newChainFactory = (metaonly?: boolean) =>
      this._chain(metaonly).then((chain) => {
        const dep = finalizeChainWithContext(
          chain,
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
                  for (let i = 0; i < data.length; i++) {
                    const item = data[i];
                    const parId = partitionFunc(item, partitionId, i);
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

        return {
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
        };
      });

    return new RDD(this._context, newChainFactory);
  }

  cache(storage?: StorageClient) {
    if (!storage) {
      if (!this._context.storage) {
        throw new Error('No storage available.');
      }
      storage = this._context.storage;
    }
    const sessionPromise = storage.startSession();
    const chainPromise = async (metaonly?: boolean) => {
      if (metaonly) {
        return this._chain(metaonly);
      }
      const session = await sessionPromise;
      const chain = await this._chain();
      const keys = await this.execute(
        finalizeChain(
          mapChain(
            chain,
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
      return {
        n: keys.length,
        p: dcfc.captureEnv(
          (partitionId) => {
            const key = keys[partitionId];
            return dcfc.captureEnv(
              async () => {
                if (!key) {
                  return [];
                }
                const buf = await session.readFile(key);
                return dcfc.decode(buf) as T[];
              },
              {
                session,
                key,
                dcfc: dcfc.requireModule('@dcfjs/common'),
              }
            );
          },
          {
            session,
            keys,
            dcfc: dcfc.requireModule('@dcfjs/common'),
          }
        ),
        t: 'cache()',
        d: [],
      };
    };

    return new CachedRDD(this._context, chainPromise, sessionPromise);
  }

  persist(storage?: StorageClient) {
    return this.cache(storage);
  }

  coalesce(numPartitions: number) {
    const newChainPromise = (metaonly?: boolean) =>
      this._chain(metaonly).then((chain) => {
        const originPartitions = chain.n;

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
          chain,
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
        return {
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
        };
      });

    return new RDD(this._context, newChainPromise);
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

  groupWith<K, V, V1>(
    this: RDD<[K, V]>,
    other1: RDD<[K, V1]>
  ): RDD<[K, [V[], V1[]]]>;
  groupWith<K, V, V1, V2>(
    this: RDD<[K, V]>,
    other1: RDD<[K, V1]>,
    other2: RDD<[K, V2]>
  ): RDD<[K, [V[], V1[], V2[]]]>;
  groupWith<K, V, V1, V2, V3>(
    this: RDD<[K, V]>,
    other1: RDD<[K, V1]>,
    other2: RDD<[K, V2]>,
    other3: RDD<[K, V3]>
  ): RDD<[K, [V[], V1[], V2[], V3[]]]>;
  groupWith<K>(this: RDD<[K, any]>, ...others: RDD<[K, any]>[]): RDD<[K, any]>;
  groupWith<K>(this: RDD<[K, any]>, ...others: RDD<[K, any]>[]): RDD<[K, any]> {
    return realGroupWith([this, ...others], this._context);
  }

  cogroup<K, V, V1>(
    this: RDD<[K, V]>,
    other: RDD<[K, V1]>,
    numPartitions?: number
  ): RDD<[K, [V[], V1[]]]> {
    return realGroupWith([this, other], this._context, numPartitions) as RDD<
      [K, [V[], V1[]]]
    >;
  }

  join<K, V, V1>(
    this: RDD<[K, V]>,
    other: RDD<[K, V1]>,
    numPartitions?: number
  ): RDD<[K, [V, V1]]> {
    return this.cogroup(other, numPartitions).flatMap(([k, [v1s, v2s]]) => {
      const ret = [];
      for (const v1 of v1s) {
        for (const v2 of v2s) {
          ret.push([k, [v1, v2]] as [K, [V, V1]]);
        }
      }
      return ret;
    });
  }

  leftOuterJoin<K, V, V1>(
    this: RDD<[K, V]>,
    other: RDD<[K, V1]>,
    numPartitions?: number
  ): RDD<[K, [V, V1 | null]]> {
    return this.cogroup(other, numPartitions).flatMap(([k, [v1s, v2s]]) => {
      const ret = [];
      if (v2s.length === 0) {
        for (const v1 of v1s) {
          ret.push([k, [v1, null]] as [K, [V, V1 | null]]);
        }
      } else {
        for (const v1 of v1s) {
          for (const v2 of v2s) {
            ret.push([k, [v1, v2]] as [K, [V, V1 | null]]);
          }
        }
      }
      return ret;
    });
  }

  rightOuterJoin<K, V, V1>(
    this: RDD<[K, V]>,
    other: RDD<[K, V1]>,
    numPartitions?: number
  ): RDD<[K, [V | null, V1]]> {
    return this.cogroup(other, numPartitions).flatMap(([k, [v1s, v2s]]) => {
      const ret = [];
      if (v1s.length === 0) {
        for (const v2 of v2s) {
          ret.push([k, [null, v2]] as [K, [V | null, V1]]);
        }
      } else {
        for (const v1 of v1s) {
          for (const v2 of v2s) {
            ret.push([k, [v1, v2]] as [K, [V | null, V1]]);
          }
        }
      }
      return ret;
    });
  }

  fullOuterJoin<K, V, V1>(
    this: RDD<[K, V]>,
    other: RDD<[K, V1]>,
    numPartitions?: number
  ): RDD<[K, [V | null, V1 | null]]> {
    return this.cogroup(other, numPartitions).flatMap(([k, [v1s, v2s]]) => {
      const ret = [];
      if (v1s.length === 0) {
        for (const v2 of v2s) {
          ret.push([k, [null, v2]] as [K, [V | null, V1 | null]]);
        }
      } else if (v2s.length === 0) {
        for (const v1 of v1s) {
          ret.push([k, [v1, null]] as [K, [V | null, V1 | null]]);
        }
      } else {
        for (const v1 of v1s) {
          for (const v2 of v2s) {
            ret.push([k, [v1, v2]] as [K, [V | null, V1 | null]]);
          }
        }
      }
      return ret;
    });
  }

  async saveAsTextFile(
    this: RDD<string>,
    path: string | URL,
    {
      overwrite = true,
      encoding = 'utf8',
      extension = 'txt',
      storage,

      compressor,
    }: {
      overwrite?: boolean;
      encoding?: BufferEncoding;
      extension?: string;
      storage?: StorageClient;

      compressor?: (data: Buffer) => Buffer | Promise<Buffer>;
    } = {}
  ): Promise<void> {
    const url = toUrl(path);
    const loader = this._context.getFileLoader(url);

    const dataChain = await this._chain();
    const baseChain = mapChain(
      dataChain,
      dcfc.captureEnv(
        async (v, partitionId) => {
          let content = Buffer.from(v.join('\n'), encoding);
          if (compressor) {
            content = await compressor(content);
          }
          return [`part-${partitionId}.${extension}`, content] as [
            string,
            Buffer
          ];
        },
        {
          extension,
          encoding,
          compressor,
        }
      )
    );

    const [finalChain, finalFunc] = await loader.getWriteFileChain(
      url,
      baseChain,
      {
        overwrite,
        storage: storage || this._context.storage || undefined,
      }
    );
    const chainResult = await this.execute(finalChain);
    await finalFunc(chainResult);
  }

  sort<K extends string | number>(
    this: RDD<K>,
    ascending: boolean = true,
    numPartitions?: number
  ) {
    return this.sortBy((v) => v, ascending, numPartitions);
  }

  sortBy<K extends string | number>(
    keyFunc: (data: T) => K,
    ascending: boolean = true,
    numPartitions: number = this._context.options.defaultPartitions!,
    storage: StorageClient = this._context.storage!
  ): RDD<T> {
    if (!numPartitions || numPartitions < 0) {
      throw new Error('Must specify partitions count.');
    }
    if (!storage) {
      throw new Error('No storage available.');
    }
    const chainFactory = async (metaonly?: boolean) => {
      const prevChain = await this._chain(metaonly);
      if (metaonly) {
        return {
          n: numPartitions,
          p: () => () => [],
          t: `${prevChain.t}.sort()`,
          d: [],
        };
      }
      // step1: cache if needed
      const cache = this instanceof CachedRDD ? this : this.persist(storage);
      const isCache = this instanceof CachedRDD;

      // Step 2:
      // sample with 1/n fraction where n is origin partition count
      // so we get a sample count near to a single partition.
      // Sample contains partitionIndex & localIndex to avoid performance inssue
      // when dealing with too many same values.
      const originPartitions = prevChain.n;
      const samples = await cache
        .mapPartitions(
          dcfc.captureEnv(
            (arr, partitionId) =>
              arr.map(
                (v, i) => [keyFunc(v), partitionId, i] as [K, number, number]
              ),
            { keyFunc }
          )
        )
        .filter(
          dcfc.captureEnv((v) => Math.random() * originPartitions < 1, {
            originPartitions,
          })
        )
        .collect();

      // Step 3: sort samples, and get seperate points.
      samples.sort((a, b) => {
        if (a !== b) {
          return (a[0] < b[0] ? -1 : 1) * (ascending ? 1 : -1);
        }
        if (a[1] !== b[1]) {
          return a[1] < b[1] ? -1 : 1;
        }
        return a[2] < b[2] ? -1 : 1;
      });

      // get n-1 points in samples.
      const points: [K, number, number][] = [];
      let p = samples.length / numPartitions;
      for (let i = 1; i < numPartitions; i++) {
        let idx = Math.floor(p * i);
        if (idx < samples.length) {
          points.push(samples[idx]);
        }
      }

      // Step 4: Repartition by points.
      const repartitioned = cache
        .mapPartitions(
          dcfc.captureEnv(
            (data, partitionId) => {
              const tmp = data.map(
                (item, i) => [keyFunc(item), i] as [K, number]
              );
              tmp.sort((a, b) => {
                if (a[0] !== b[0]) {
                  return (a[0] < b[0] ? -1 : 1) * (ascending ? 1 : -1);
                }
                return a[1] < b[1] ? -1 : 1;
              });
              const ret: [T, K, number, number][][] = [];
              for (let i = 0; i < numPartitions; i++) {
                ret.push([]);
              }
              let index = 0;
              for (const [key, i] of tmp) {
                // compare with points
                for (; index < points.length; ) {
                  if (
                    ascending ? key < points[index][0] : key > points[index][0]
                  ) {
                    break;
                  }
                  if (
                    ascending ? key > points[index][0] : key < points[index][0]
                  ) {
                    index++;
                    continue;
                  }
                  if (partitionId < points[index][1]) {
                    break;
                  }
                  if (partitionId > points[index][1]) {
                    index++;
                    continue;
                  }
                  if (i < points[index][2]) {
                    break;
                  }
                  index++;
                }
                ret[index].push([data[i], key, partitionId, i]);
              }
              return ret;
            },
            {
              numPartitions,
              points,
              keyFunc,
              ascending,
            }
          )
        )
        .partitionBy(numPartitions, (v, partitionId, index) => index);

      // Step 5: Sort in partition.
      // Maybe reverse (if ascending == false).
      const sorted = repartitioned.mapPartitions(
        dcfc.captureEnv(
          (data) => {
            const tmp = dcfc.concatArrays(data);
            tmp.sort((a, b) => {
              if (a[1] !== b[1]) {
                return (a[1] < b[1] ? -1 : 1) * (ascending ? 1 : -1);
              }
              if (a[2] !== b[2]) {
                return a[2] < b[2] ? -1 : 1;
              }
              return a[3] < b[3] ? -1 : 1;
            });
            return tmp.map((v) => v[0]);
          },
          {
            dcfc: dcfc.requireModule('@dcfjs/common'),
            ascending,
          }
        )
      );

      const chain = await sorted._chain();

      if (!isCache) {
        // remove persisted data after sorted.
        const session = await cache.storageSession;
        session.release();
        chain.d.push({
          n: 0,
          p: () => () => [],
          t: 'cache()',
          d: [],
          i: dcfc.captureEnv(
            () => {
              return session.autoRenew();
            },
            { session }
          ),
          f: () => {},
          c: dcfc.captureEnv(
            () => {
              return session.close();
            },
            { session }
          ),
        });
      }

      return chain;
    };
    return new RDD<T>(this._context, chainFactory);
  }
}

export class CachedRDD<T> extends RDD<T> {
  storageSession: Promise<StorageSession>;

  constructor(
    context: DCFContext,
    chain: (metaonly?: boolean) => Promise<RDDWorkChain<T[]>>,
    storageSession: Promise<StorageSession>
  ) {
    super(context, chain);
    this.storageSession = storageSession;
  }

  async unpersist(): Promise<void> {
    const session = await this.storageSession;
    await session.close();
  }
}
