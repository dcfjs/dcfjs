import { DCFContext } from './context';
import * as dcfc from '@dcfjs/common';
import {
  RDDWorkChain,
  RDDFinalizedWorkChain,
  finalizeChain,
  mapChain,
  runWorkChain,
} from './chain';

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

  async take(limit: number): Promise<T[]> {
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
}
