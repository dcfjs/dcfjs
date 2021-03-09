import BufferList = require('bl');

export { BufferList };
export type Encoder<T extends unknown = unknown> = (
  obj: T
) => Buffer | BufferList;
export type EncodeTypeChecker<T extends unknown = unknown> = (v: T) => boolean;

export type EncodingType<T extends unknown = unknown> = {
  check: EncodeTypeChecker<T>;
  encode: Encoder<T>;
};

export interface EncodeOptions {
  disableTimestampEncoding?: boolean;
  preferMap?: boolean;
  sortKeys?: boolean;
  forceFloat64?: boolean;
  compatibilityMode?: boolean;
  protoAction?: 'error' | 'remove';
}

export type Decoder<T extends unknown = unknown> = (buf: Buffer) => T;

export type DecodeOptions = EncodeOptions;
