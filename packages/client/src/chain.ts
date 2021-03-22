import * as dcfc from '@dcfjs/common';
import {
  FunctionEnv,
  SerializedFunction,
  WorkerExecFunction,
} from '@dcfjs/common';

export type InitializeFunc<Context = unknown> = () =>
  | Context
  | Promise<Context>;

export type PartitionFunc<T = unknown, Context = unknown> = (
  paritionId: number,
  dependRets: unknown[],
  ctx: Context
) => () => T | Promise<T>;

export type FinalizedFunc<T = unknown, T1 = unknown, Context = unknown> = (
  v: T[],
  ctx: Context
) => T1 | Promise<T1>;

export type CleanUpFunc<Context = unknown> = (
  ctx: Context
) => void | Promise<void>;

export interface RDDWorkChain<T = unknown, Context = void> {
  // name
  n: number;
  // partition worker
  p: PartitionFunc<T, Context>;
  // title
  t: string;
  // dependencies
  d: RDDFinalizedWorkChain[];
}

export interface RDDFinalizedWorkChain<
  T = unknown,
  T1 = unknown,
  Context = unknown
> extends RDDWorkChain<T, Context> {
  // initializer
  i?: InitializeFunc<Context>;
  // finalizer worker
  f: FinalizedFunc<T, T1, Context>;
  // cleanup(runs after dependents over)
  c?: CleanUpFunc<Context>;
}

export async function runWorkChain<
  T = unknown,
  T1 = unknown,
  Context = unknown
>(
  dispatchWork: <T = unknown>(
    func: SerializedFunction | WorkerExecFunction<T>,
    env?: FunctionEnv
  ) => Promise<T>,
  chain: RDDFinalizedWorkChain<T, T1, Context>
): Promise<[T1, () => Promise<void>]> {
  const disposes: (() => void | Promise<void>)[] = [];
  try {
    const dependValues = await Promise.all(
      chain.d.map((d) =>
        runWorkChain(dispatchWork, d).then(([ret, cleanup]) => {
          disposes.push(cleanup);
          return ret;
        })
      )
    );

    const ctx = chain.i && (await chain.i());
    try {
      const temp = [];
      for (let partitionId = 0; partitionId < chain.n; partitionId++) {
        temp[partitionId] = dispatchWork(
          chain.p(partitionId, dependValues, ctx as Context)
        );
      }
      const partitionRets = await Promise.all(temp);
      const ret = await chain.f(partitionRets, ctx as Context);
      return [
        ret,
        async () => {
          return chain.c && chain.c(ctx as Context);
        },
      ];
    } catch (e) {
      chain.c && chain.c(ctx as Context);
      throw e;
    }
  } finally {
    // clean up dependencies after current work.
    await Promise.all(disposes);
  }
}

export function mapChain<T, T1>(
  { n, p, t, d }: RDDWorkChain<T>,
  mapper: (v: T) => T1 | Promise<T1>,
  titleMapper?: (t: string) => string
): RDDWorkChain<T1> {
  return {
    n,
    p: dcfc.captureEnv(
      (partitionId, dependValues) => {
        const pp = p(partitionId, dependValues);
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

export function finalizeChain<T, T1>(
  chain: RDDWorkChain<T>,
  finalizer: (v: T[]) => T1,
  titleMapper?: (t: string) => string
): RDDFinalizedWorkChain<T, T1, void> {
  return {
    ...chain,
    f: finalizer,
    t: titleMapper ? titleMapper(chain.t) : chain.t,
  };
}

export function finalizeChainWithContext<T2, T, T1, Context>(
  chain: RDDWorkChain<T>,
  initializer: InitializeFunc<Context>,
  mapper: (v: T, ctx: Context, partitionId: number) => T2 | Promise<T2>,
  finalizer: (v: T2[], ctx: Context) => T1,
  cleanup?: CleanUpFunc<Context>,
  titleMapper?: (t: string) => string
): RDDFinalizedWorkChain<T2, T1, Context> {
  const { p } = chain;

  return {
    ...chain,
    p: dcfc.captureEnv(
      (partitionId, dependValues, ctx) => {
        const pp = p(partitionId, dependValues);
        return dcfc.captureEnv(
          () => {
            const org = pp();
            if (org instanceof Promise) {
              return org.then((v) => mapper(v, ctx, partitionId));
            }
            return mapper(org, ctx, partitionId);
          },
          {
            pp,
            ctx,
            partitionId,
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
    i: initializer,
    f: finalizer,
    c: cleanup,
    t: titleMapper ? titleMapper(chain.t) : chain.t,
  };
}
