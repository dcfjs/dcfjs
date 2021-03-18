import { IncompleteBufferError } from './helpers';
import { Decoder, DecodeOptions, BufferList } from './type';

const SIZES: { [key: number]: number } = {
  0xc4: 2,
  0xc5: 3,
  0xc6: 5,
  0xc7: 3,
  0xc8: 4,
  0xc9: 6,
  0xca: 5,
  0xcb: 9,
  0xcc: 2,
  0xcd: 3,
  0xce: 5,
  0xcf: 9,
  0xd0: 2,
  0xd1: 3,
  0xd2: 5,
  0xd3: 9,
  0xd4: 3,
  0xd5: 4,
  0xd6: 6,
  0xd7: 10,
  0xd8: 18,
  0xd9: 2,
  0xda: 3,
  0xdb: 5,
  0xde: 3,
  0xdc: 3,
  0xdd: 5,
};

function isValidDataSize(
  dataLength: number,
  bufLength: number,
  headerLength: number
) {
  return bufLength >= headerLength + dataLength;
}

export function buildDecode(
  decodingTypes: Map<number, Decoder>,
  options: DecodeOptions
) {
  return decode;

  function decode(buf: Buffer | BufferList) {
    // TODO: Make it into ensureBl handler ?
    if (!(buf instanceof BufferList)) {
      buf = new BufferList().append(buf);
    }

    const result = tryDecode(buf as BufferList, 0);
    // Handle worst case ASAP and keep code flat
    if (!result) throw new IncompleteBufferError();

    (buf as BufferList).consume(result[1]);
    return result[0];
  }

  function tryDecode(
    buf: BufferList,
    initialOffset = 0
  ): [unknown, number] | null {
    if (buf.length <= initialOffset) return null;

    const bufLength = buf.length - initialOffset;
    let offset = initialOffset;

    const first = buf.readUInt8(offset);
    offset += 1;

    const size = SIZES[first] || -1;
    if (bufLength < size) return null;

    const inRange = (start: number, end: number) =>
      first >= start && first <= end;

    if (first < 0x80) return [first, 1]; // 7-bits positive ints
    if ((first & 0xf0) === 0x80) {
      const length = first & 0x0f;
      const headerSize = offset - initialOffset;
      // we have a map with less than 15 elements
      return decodeMap(buf, offset, length, headerSize, options);
    }
    if ((first & 0xf0) === 0x90) {
      const length = first & 0x0f;
      const headerSize = offset - initialOffset;
      // we have an array with less than 15 elements
      return decodeArray(buf, offset, length, headerSize);
    }

    if ((first & 0xe0) === 0xa0) {
      // fixstr up to 31 bytes
      const length = first & 0x1f;
      if (!isValidDataSize(length, bufLength, 1)) return null;
      const result = buf.toString('utf8', offset, offset + length);
      return [result, length + 1];
    }
    if (inRange(0xc0, 0xc3)) return decodeConstants(first);
    if (inRange(0xc4, 0xc6)) {
      const length = (buf as any).readUIntBE(offset, size - 1);
      offset += size - 1;

      if (!isValidDataSize(length, bufLength, size)) return null;
      const result = buf.slice(offset, offset + length);
      return [result, size + length];
    }
    if (inRange(0xc7, 0xc9)) {
      const length = (buf as any).readUIntBE(offset, size - 2);
      offset += size - 2;

      const type = buf.readInt8(offset);
      offset += 1;

      if (!isValidDataSize(length, bufLength, size)) return null;
      return decodeExt(buf, offset, type, length, size);
    }
    if (inRange(0xca, 0xcb)) return decodeFloat(buf, offset, size - 1);
    if (inRange(0xcc, 0xcf)) return decodeUnsignedInt(buf, offset, size - 1);
    if (inRange(0xd0, 0xd3)) return decodeSigned(buf, offset, size - 1);
    if (inRange(0xd4, 0xd8)) {
      const type = buf.readInt8(offset); // Signed
      offset += 1;
      return decodeExt(buf, offset, type, size - 2, 2);
    }

    if (inRange(0xd9, 0xdb)) {
      const length = (buf as any).readUIntBE(offset, size - 1) as number;
      offset += size - 1;

      if (!isValidDataSize(length, bufLength, size)) return null;
      const result = buf.toString('utf8', offset, offset + length);
      return [result, size + length];
    }
    if (inRange(0xdc, 0xdd)) {
      const length = (buf as any).readUIntBE(offset, size - 1) as number;
      offset += size - 1;
      return decodeArray(buf, offset, length, size);
    }
    if (inRange(0xde, 0xdf)) {
      let length;
      switch (first) {
        case 0xde:
          // maps up to 2^16 elements - 2 bytes
          length = buf.readUInt16BE(offset);
          offset += 2;
          return decodeMap(buf, offset, length, 3, options);

        case 0xdf:
          length = buf.readUInt32BE(offset);
          offset += 4;
          return decodeMap(buf, offset, length, 5, options);
      }
    }
    if (first >= 0xe0) return [first - 0x100, 1]; // 5 bits negative ints

    throw new Error('not implemented yet');
  }

  function decodeArray(
    buf: BufferList,
    initialOffset: number,
    length: number,
    headerLength: number
  ): [unknown[], number] | null {
    let offset = initialOffset;
    const result = [];
    let i = 0;

    while (i++ < length) {
      const decodeResult = tryDecode(buf, offset);
      if (!decodeResult) return null;

      result.push(decodeResult[0]);
      offset += decodeResult[1];
    }
    return [result, headerLength + offset - initialOffset];
  }

  function decodeMap(
    buf: BufferList,
    offset: number,
    length: number,
    headerLength: number,
    options: DecodeOptions
  ): [object, number] | null {
    const _temp = decodeArray(buf, offset, 2 * length, headerLength);
    if (!_temp) return null;
    const [result, consumedBytes] = _temp;

    let isPlainObject = !options.preferMap;

    if (isPlainObject) {
      for (let i = 0; i < 2 * length; i += 2) {
        if (typeof result[i] !== 'string') {
          isPlainObject = false;
          break;
        }
      }
    }

    if (isPlainObject) {
      const object: Record<string, unknown> = {};
      for (let i = 0; i < 2 * length; i += 2) {
        const key = result[i] as string;
        const val = result[i + 1];

        if (key === '__proto__') {
          if (options.protoAction === 'error') {
            throw new SyntaxError(
              'Object contains forbidden prototype property'
            );
          }

          if (options.protoAction === 'remove') {
            continue;
          }
        }

        object[key] = val;
      }
      return [object, consumedBytes];
    } else {
      const mapping = new Map();
      for (let i = 0; i < 2 * length; i += 2) {
        const key = result[i];
        const val = result[i + 1];
        mapping.set(key, val);
      }
      return [mapping, consumedBytes];
    }
  }

  function readInt64BE(buf: Buffer, offset: number): number {
    var negate = (buf[offset] & 0x80) == 0x80;

    if (negate) {
      let carry = 1;
      for (let i = offset + 7; i >= offset; i--) {
        const v = (buf[i] ^ 0xff) + carry;
        buf[i] = v & 0xff;
        carry = v >> 8;
      }
    }

    const hi = buf.readUInt32BE(offset + 0);
    const lo = buf.readUInt32BE(offset + 4);
    return (hi * 4294967296 + lo) * (negate ? -1 : +1);
  }

  function decodeUnsignedInt(
    buf: BufferList,
    offset: number,
    size: number
  ): [number, number] {
    const maxOffset = offset + size;
    let result = 0;
    while (offset < maxOffset) {
      result += buf.readUInt8(offset++) * Math.pow(256, maxOffset - offset);
    }
    return [result, size + 1];
  }

  function decodeConstants(first: number): [unknown, number] | null {
    if (first === 0xc0) return [null, 1];
    if (first === 0xc1) return [undefined, 1];
    if (first === 0xc2) return [false, 1];
    if (first === 0xc3) return [true, 1];
    return null;
  }

  function decodeSigned(
    buf: BufferList,
    offset: number,
    size: number
  ): [number, number] {
    let result = 0;
    if (size === 1) result = buf.readInt8(offset);
    if (size === 2) result = buf.readInt16BE(offset);
    if (size === 4) result = buf.readInt32BE(offset);
    if (size === 8) result = readInt64BE(buf.slice(offset, offset + 8), 0);
    return [result, size + 1];
  }

  function decodeFloat(
    buf: BufferList,
    offset: number,
    size: number
  ): [number, number] {
    let result = NaN;
    if (size === 4) result = buf.readFloatBE(offset);
    if (size === 8) result = buf.readDoubleBE(offset);
    return [result, size + 1];
  }

  function decodeExt(
    buf: BufferList,
    offset: number,
    type: number,
    size: number,
    headerSize: number
  ): [unknown, number] {
    const toDecode = buf.slice(offset, offset + size);

    const decode = decodingTypes.get(type);
    if (!decode) throw new Error('unable to find ext type ' + type);

    const value = decode(toDecode);
    return [value, headerSize + size];
  }
}
