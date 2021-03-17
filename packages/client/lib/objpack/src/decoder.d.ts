/// <reference types="node" />
import { Decoder, DecodeOptions, BufferList } from './type';
export declare function buildDecode(decodingTypes: Map<number, Decoder>, options: DecodeOptions): (buf: Buffer | BufferList) => unknown;
