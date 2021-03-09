import { EncodeTypeChecker, Decoder } from './type.d';
import { BufferList, EncodeOptions, Encoder, EncodingType } from './type';
import { IncompleteBufferError } from './helpers';
import { DateCodec } from './codecs';
import { buildEncode } from './encoder';
import { buildDecode } from './decoder';

const assert = require('assert');

export default function msgpack(options?: EncodeOptions) {
  const encodingTypes: EncodingType[] = [];
  const decodingTypes = new Map();

  options = options || {
    forceFloat64: false,
    compatibilityMode: false,
    // if true, skips encoding Dates using the msgpack
    // timestamp ext format (-1)
    disableTimestampEncoding: false,
    preferMap: false,
    // options.protoAction: 'error' (default) / 'remove' / 'ignore'
    protoAction: 'error',
  };

  decodingTypes.set(DateCodec.type, DateCodec.decode);
  if (!options.disableTimestampEncoding) {
    encodingTypes.push(DateCodec as EncodingType);
  }

  function registerEncoder(check: EncodeTypeChecker, encode: Encoder) {
    assert(check, 'must have an encode function');
    assert(encode, 'must have an encode function');

    encodingTypes.push({ check, encode });
  }

  function registerDecoder(type: number, decode: Decoder) {
    assert(type >= 0, 'must have a non-negative type');
    assert(decode, 'must have a decode function');
    decodingTypes.set(type, decode);
  }

  function register<T>(
    type: number,
    constructor: Function,
    encode: Encoder<T>,
    decode: Decoder<T>
  ) {
    assert(constructor, 'must have a constructor');
    assert(encode, 'must have an encode function');
    assert(type >= 0, 'must have a non-negative type');
    assert(decode, 'must have a decode function');

    function check(obj: unknown) {
      return obj instanceof constructor;
    }

    function reEncode(obj: T) {
      const buf = new BufferList();
      const header = Buffer.allocUnsafe(1);

      header.writeInt8(type, 0);

      buf.append(header);
      buf.append(encode(obj));

      return buf;
    }

    registerEncoder(check, reEncode as Encoder);
    registerDecoder(type, decode);
  }

  return {
    encode: buildEncode(encodingTypes, options),
    decode: buildDecode(decodingTypes, options),
    register,
    registerEncoder,
    registerDecoder,
    // needed for levelup support
    buffer: true,
    type: 'msgpack5',
    IncompleteBufferError,
  };
}
