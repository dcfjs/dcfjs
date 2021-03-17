export interface SerializedFunction {
    __type: 'function';
    source: string;
    args: string[];
    values: any[];
}
export declare type FunctionEnv = {
    [key: string]: any;
};
declare class RequireModule {
    moduleName: string;
    constructor(moduleName: string);
}
export declare function requireModule(v: string): RequireModule;
export declare function serializeFunction<T extends (...args: any[]) => any>(f: T & {
    __env?: FunctionEnv;
    __serialized?: SerializedFunction;
}, env?: FunctionEnv): SerializedFunction;
export declare function deserializeFunction<T extends (...args: any[]) => any>(f: SerializedFunction, noWrap?: boolean): T;
export declare function captureEnv<T extends (...args: any[]) => any>(f: T & {
    __env?: FunctionEnv;
}, env: FunctionEnv): T & {
    __env?: FunctionEnv | undefined;
};
export {};
