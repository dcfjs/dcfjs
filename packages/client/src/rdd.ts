import { DCFContext } from './context';
import * as dcfc from '@dcfjs/common';
import {
  RDDWorkChain,
  RDDFinalizedWorkChain,
  finalizeChain,
  mapChain,
  runWorkChain,
} from './chain';
import { requireModule } from '@dcfjs/common';

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

  protected execute<T, T1>(chain: RDDFinalizedWorkChain<T, T1>): Promise<T1> {
    return this._context.execute(
      dcfc.captureEnv(
        ({ dispatchWork }) => {
          return runWorkChain(dispatchWork, chain);
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
            dcfc: requireModule('@dcfjs/common'),
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
}
