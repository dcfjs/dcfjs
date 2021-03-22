export interface SerializedFunction {
  __type: 'function';
  source: string;
  args: string[];
  values: any[];
  __deserialized?: Function;
}

export type FunctionEnv = { [key: string]: any };

export interface CustomSerializedObject {
  __type: 'factory';
  getValue: SerializedFunction;
}

export const symbolSerialize = Symbol();

class RequireModule {
  moduleName: string;
  constructor(moduleName: string) {
    this.moduleName = moduleName;
  }
}

function cachePair(obj: any, ser: any) {
  Object.defineProperty(ser, '__deserialized', {
    value: obj,
    enumerable: false,
    configurable: false,
  });
  Object.defineProperty(obj, '__serialized', {
    value: ser,
    enumerable: false,
    configurable: false,
  });
}

function serializeObject(
  obj: Record<string, unknown>
): Record<string, unknown> {
  const ret: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) {
    ret[k] = serializeValue(v);
  }
  return ret;
}

function serializeValue(v: any): any {
  if (typeof v === 'function') {
    return serializeFunction(v);
  }
  if (v && typeof v === 'object') {
    if (v.__serialized) {
      return v.__serialized;
    }
    if (v.constructor === RequireModule) {
      return {
        __type: 'require',
        moduleName: v.moduleName,
      };
    }
    if (Array.isArray(v)) {
      const ret = v.map(serializeValue);
      cachePair(v, ret);
      return ret;
    }
    if (v instanceof RegExp || v instanceof Buffer) {
      return v;
    }
    if (typeof v[symbolSerialize] === 'function') {
      let f = v[symbolSerialize]();
      if (typeof f === 'function') {
        f = serializeFunction(f);
      }
      const ret = {
        __type: 'factory',
        getValue: f,
      };
      cachePair(v, ret);
      return ret;
    }
    if (v.constructor !== Object) {
      throw new Error(
        `Cannot pass a ${v.constructor.name} object. Try to implement [symbolSerialize]() to give a custom serializer.`
      );
    }
    let ret = serializeObject(v);
    if (ret.__type) {
      // handle a native object with __type field. This is a rare case.
      ret = {
        __type: 'object',
        value: v,
      };
    }
    cachePair(v, ret);
    return ret;
  }
  return v;
}

function deserializeObject(
  obj: Record<string, unknown>
): Record<string, unknown> {
  const ret: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) {
    ret[k] = deserializeValue(v);
  }
  return ret;
}

function deserializeValue(v: any): any {
  if (v && typeof v === 'object') {
    if (v.__deserialized) {
      return v.__deserialized;
    }

    if (Array.isArray(v)) {
      const ret = v.map(deserializeValue);
      cachePair(ret, v);
      Object.freeze(ret);
      return ret;
    }
    if (v.__type) {
      switch (v.__type) {
        case 'object':
          const ret = deserializeObject(v.value);
          cachePair(ret, v);
          Object.freeze(ret);
          return ret;
        case 'function':
          // deserializeFunction has cache check.
          return deserializeFunction(v, true);
        case 'require':
          // Do not cache require statement(because it has own cache.)
          return require(v.moduleName);
        case 'factory': {
          const f = deserializeFunction(v.getValue);
          const ret = f();
          cachePair(ret, v);
          return ret;
        }
        default: {
          throw new Error('Unknown');
        }
      }
    } else {
      const ret = deserializeObject(v);
      cachePair(ret, v);
      return ret;
    }
  }
  return v;
}

const requireModuleCache: { [key: string]: RequireModule } = {};

export function requireModule(v: string) {
  return (requireModuleCache[v] =
    requireModuleCache[v] || new RequireModule(v));
}

export function serializeFunction<T extends (...args: any[]) => any>(
  f: T & {
    __env?: FunctionEnv;
    __serialized?: SerializedFunction;
  },
  env?: FunctionEnv
): SerializedFunction {
  if (f.__serialized) {
    return f.__serialized;
  }

  env = env || f.__env;
  const args: string[] = [];
  const values: any[] = [];

  if (env) {
    for (const key of Object.keys(env)) {
      args.push(key);
      values.push(serializeValue(env[key]));
    }
  }

  const ret = {
    __type: 'function',
    source: f.toString(),
    args,
    values,
  } as SerializedFunction;
  cachePair(f, ret);
  return ret;
}

function wrap<T extends (...args: any[]) => any>(f: T) {
  return function (...args: any[]) {
    try {
      return f(...args);
    } catch (e) {
      throw new Error(`In function ${f.toString()}\n${e.stack}`);
    }
  };
}

export function deserializeFunction<T extends (...args: any[]) => any>(
  f: SerializedFunction,
  noWrap?: boolean
): T {
  if (f.__deserialized) {
    return f.__deserialized as T;
  }
  let ret;
  const valueMap = f.values.map((v) => deserializeValue(v));
  try {
    if (noWrap) {
      ret = new Function(
        'require',
        '__args',
        `const [${f.args.join(',')}] = __args;
  return ${f.source}`
      )(require, valueMap);
    } else {
      ret = new Function(
        'require',
        '__wrap',
        '__args',
        `const [${f.args.join(',')}] = __args;
return __wrap(${f.source})`
      )(require, wrap, valueMap);
    }
    cachePair(ret, f);
    return ret;
  } catch (e) {
    throw new Error(
      'Error while deserializing function ' + f.source + '\n' + e.stack
    );
  }
}

export function captureEnv<T extends (...args: any[]) => any>(
  f: T & {
    __env?: FunctionEnv;
  },
  env: FunctionEnv
) {
  if (!f.__env) {
    Object.defineProperty(f, '__env', {
      value: env,
      enumerable: false,
      configurable: false,
    });
  }
  return f;
}
