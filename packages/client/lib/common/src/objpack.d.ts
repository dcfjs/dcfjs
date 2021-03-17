/// <reference types="node" />
declare const encode: (obj: unknown) => Buffer, decode: (buf: any) => unknown;
export { encode, decode };
