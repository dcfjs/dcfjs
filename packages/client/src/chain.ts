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

export type FinalizedFunc<T = unknown, T1 = unknown> = (
  v: T[]
) => T1 | Promise<T1>;

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

export async function runWorkChain<T = unknown, T1 = unknown>(
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

export function mapChain<T, T1>(
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

export function finalizeChain<T, T1>(
  chain: RDDWorkChain<T>,
  finalizer: (v: T[]) => T1,
  titleMapper?: (t: string) => string
): RDDFinalizedWorkChain<T, T1> {
  return {
    ...chain,
    f: finalizer,
    t: titleMapper ? titleMapper(chain.t) : chain.t,
  };
}
