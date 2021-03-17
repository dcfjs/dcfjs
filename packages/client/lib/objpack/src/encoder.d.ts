/// <reference types="node" />
import { EncodingType, EncodeOptions } from './type';
export declare function buildEncode(encodingTypes: EncodingType[], options: EncodeOptions): (obj: unknown) => Buffer;
