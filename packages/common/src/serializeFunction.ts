export interface SerializedFunction {
  __type: 'function';
  source: string;
  args: string[];
  values: any[];
}

export type FunctionEnv = { [key: string]: any };

class RequireModule {
  moduleName: string;
  constructor(moduleName: string) {
    this.moduleName = moduleName;
  }
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
    if (v.constructor === RequireModule) {
      return {
        __type: 'require',
        moduleName: v.moduleName,
      };
    }
    if (Array.isArray(v)) {
      return v.map(serializeValue);
    }
    if (v instanceof RegExp || v instanceof Buffer) {
      return v;
    }
    if (v.constructor !== Object) {
      throw new Error(`Cannot pass a ${v.constructor.name} object`);
    }
    v = serializeObject(v);
    if (v.__type) {
      // handle a native object with __type field. This is a rare case.
      return {
        __type: 'object',
        value: v,
      };
    }
    return v;
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
    if (Array.isArray(v)) {
      return Object.freeze(v.map(deserializeValue));
    }
    if (v.__type) {
      switch (v.__type) {
        case 'object':
          return Object.freeze(deserializeObject(v.value));
        case 'function':
          return deserializeFunction(v, true);
        case 'require':
          return require(v.moduleName);
      }
    }
    return Object.freeze(deserializeObject(v));
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

  return {
    __type: 'function',
    source: f.toString(),
    args,
    values,
  } as SerializedFunction;
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
    Object.defineProperty(ret, '__serialized', {
      value: f,
      enumerable: false,
      configurable: false,
    });
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
