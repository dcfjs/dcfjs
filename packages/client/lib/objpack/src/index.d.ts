/// <reference types="node" />
/// <reference types="bl" />
import { EncodeTypeChecker, Decoder } from './type.d';
import { BufferList, EncodeOptions, Encoder } from './type';
import { IncompleteBufferError } from './helpers';
export default function msgpack(options?: EncodeOptions): {
    encode: (obj: unknown) => Buffer;
    decode: (buf: Buffer | BufferList) => unknown;
    register: <T>(type: number, constructor: Function, encode: Encoder<T>, decode: Decoder<T>) => void;
    registerEncoder: (check: EncodeTypeChecker, encode: Encoder) => void;
    registerDecoder: (type: number, decode: Decoder) => void;
    buffer: boolean;
    type: string;
    IncompleteBufferError: typeof IncompleteBufferError;
};
