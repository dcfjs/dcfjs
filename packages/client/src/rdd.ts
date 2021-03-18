import { DCFContext } from './context';
import * as dcfc from '@dcfjs/common';
import {
  captureEnv,
  FunctionEnv,
  SerializedFunction,
  WorkerExecFunction,
} from '@dcfjs/common';

export type PartitionFunc<T = unknown> = (
  paritionId: number
) => () => T | Promise<T>;

export type FinalizedFunc<T, T1> = (v: T[]) => T1 | Promise<T1>;

export interface RDDWorkChain<T = unknown> {
  // name
  n: number;
  // partition worker
  p: PartitionFunc<T>;
  // title
  t: string;
  // dependencies
  d: RDDFinalizedWorkChain[];
}

export interface RDDFinalizedWorkChain<T = unknown, T1 = unknown>
  extends RDDWorkChain<T> {
  // finalizer worker
  f: FinalizedFunc<T, T1>;
}

async function runWorkChain<T = unknown, T1 = unknown>(
  dispatchWork: <T = unknown>(
    func: SerializedFunction | WorkerExecFunction<T>,
    env?: FunctionEnv
  ) => Promise<T>,
  chain: RDDFinalizedWorkChain<T, T1>
): Promise<T1> {
  await Promise.all(chain.d.map((d) => runWorkChain(dispatchWork, d)));
  const temp = [];
  for (let partitionId = 0; partitionId < chain.n; partitionId++) {
    temp[partitionId] = dispatchWork(chain.p(partitionId));
  }
  return chain.f(await Promise.all(temp));
}

function mapChain<T, T1>(
  { n, p, t, d }: RDDWorkChain<T>,
  mapper: (v: T) => T1,
  titleMapper?: (t: string) => string
): RDDWorkChain<T1> {
  return {
    n,
    p: dcfc.captureEnv(
      (partitionId) => {
        const pp = p(partitionId);
        return dcfc.captureEnv(
          () => {
            const org = pp();
            if (org instanceof Promise) {
              return org.then(mapper);
            }
            return mapper(org);
          },
          {
            pp,
            mapper,
          }
        );
      },
      {
        p,
        mapper,
        dcfc: dcfc.requireModule('@dcfjs/common'),
      }
    ),
    t: titleMapper ? titleMapper(t) : t,
    d,
  };
}

function finalizeChain<T, T1>(
  chain: RDDWorkChain<T>,
  finalizer: (v: T[]) => T1
): RDDFinalizedWorkChain<T, T1> {
  return {
    ...chain,
    f: finalizer,
  };
}

export class RDD<T> {
  protected _context: DCFContext;
  protected _chain: RDDWorkChain<T[]>;
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

  count(): Promise<number> {
    return this.execute(
      finalizeChain(
        mapChain(this._chain, (v) => v.length),
        (v) => v.reduce((a, b) => a + b, 0)
      )
    );
  }
}
