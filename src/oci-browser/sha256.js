const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
]);

const W = new Uint32Array(64);

function rightRotate(value, amount) {
  return (value >>> amount) | (value << (32 - amount));
}

function toUint8Array(data) {
  if (data instanceof Uint8Array) return data;
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (typeof data === 'string') return new TextEncoder().encode(data);
  throw new TypeError('Sha256.update() expects a Uint8Array, ArrayBuffer, DataView, or string.');
}

export class Sha256 {
  constructor() {
    this._state = new Uint32Array([
      0x6a09e667,
      0xbb67ae85,
      0x3c6ef372,
      0xa54ff53a,
      0x510e527f,
      0x9b05688c,
      0x1f83d9ab,
      0x5be0cd19
    ]);
    this._buffer = new Uint8Array(64);
    this._bufferLength = 0;
    this._bytesHashedLow = 0;
    this._bytesHashedHigh = 0;
    this._finished = false;
  }

  update(data) {
    if (this._finished) {
      throw new Error('Sha256 digest was already finalized.');
    }

    const bytes = toUint8Array(data);
    let offset = 0;
    let remaining = bytes.byteLength;

    this._bytesHashedLow = (this._bytesHashedLow + remaining) >>> 0;
    if (this._bytesHashedLow < remaining) {
      this._bytesHashedHigh = (this._bytesHashedHigh + 1) >>> 0;
    }
    this._bytesHashedHigh = (this._bytesHashedHigh + Math.floor(remaining / 0x100000000)) >>> 0;

    if (this._bufferLength > 0) {
      const needed = 64 - this._bufferLength;
      const toCopy = Math.min(needed, remaining);
      this._buffer.set(bytes.subarray(offset, offset + toCopy), this._bufferLength);
      this._bufferLength += toCopy;
      offset += toCopy;
      remaining -= toCopy;
      if (this._bufferLength === 64) {
        this._transform(this._buffer);
        this._bufferLength = 0;
      }
    }

    while (remaining >= 64) {
      this._transform(bytes.subarray(offset, offset + 64));
      offset += 64;
      remaining -= 64;
    }

    if (remaining > 0) {
      this._buffer.set(bytes.subarray(offset, offset + remaining), 0);
      this._bufferLength = remaining;
    }

    return this;
  }

  digest() {
    if (!this._finished) {
      this._finish();
    }

    const out = new Uint8Array(32);
    for (let index = 0; index < 8; index += 1) {
      const value = this._state[index];
      out[index * 4] = (value >>> 24) & 0xff;
      out[index * 4 + 1] = (value >>> 16) & 0xff;
      out[index * 4 + 2] = (value >>> 8) & 0xff;
      out[index * 4 + 3] = value & 0xff;
    }
    return out;
  }

  hex() {
    return Array.from(this.digest(), (value) => value.toString(16).padStart(2, '0')).join('');
  }

  _finish() {
    const buffer = this._buffer;
    let length = this._bufferLength;
    buffer[length] = 0x80;
    length += 1;

    if (length > 56) {
      buffer.fill(0, length, 64);
      this._transform(buffer);
      buffer.fill(0);
      length = 0;
    }

    buffer.fill(0, length, 56);

    const bitsLow = (this._bytesHashedLow << 3) >>> 0;
    const bitsHigh = ((this._bytesHashedHigh << 3) | (this._bytesHashedLow >>> 29)) >>> 0;

    buffer[56] = (bitsHigh >>> 24) & 0xff;
    buffer[57] = (bitsHigh >>> 16) & 0xff;
    buffer[58] = (bitsHigh >>> 8) & 0xff;
    buffer[59] = bitsHigh & 0xff;
    buffer[60] = (bitsLow >>> 24) & 0xff;
    buffer[61] = (bitsLow >>> 16) & 0xff;
    buffer[62] = (bitsLow >>> 8) & 0xff;
    buffer[63] = bitsLow & 0xff;

    this._transform(buffer);
    this._finished = true;
  }

  _transform(chunk) {
    for (let index = 0; index < 16; index += 1) {
      const offset = index * 4;
      W[index] =
        ((chunk[offset] << 24) | (chunk[offset + 1] << 16) | (chunk[offset + 2] << 8) | chunk[offset + 3]) >>> 0;
    }

    for (let index = 16; index < 64; index += 1) {
      const s0 = rightRotate(W[index - 15], 7) ^ rightRotate(W[index - 15], 18) ^ (W[index - 15] >>> 3);
      const s1 = rightRotate(W[index - 2], 17) ^ rightRotate(W[index - 2], 19) ^ (W[index - 2] >>> 10);
      W[index] = (W[index - 16] + s0 + W[index - 7] + s1) >>> 0;
    }

    let a = this._state[0];
    let b = this._state[1];
    let c = this._state[2];
    let d = this._state[3];
    let e = this._state[4];
    let f = this._state[5];
    let g = this._state[6];
    let h = this._state[7];

    for (let index = 0; index < 64; index += 1) {
      const s1 = rightRotate(e, 6) ^ rightRotate(e, 11) ^ rightRotate(e, 25);
      const choice = (e & f) ^ (~e & g);
      const temp1 = (h + s1 + choice + K[index] + W[index]) >>> 0;
      const s0 = rightRotate(a, 2) ^ rightRotate(a, 13) ^ rightRotate(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (s0 + majority) >>> 0;

      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }

    this._state[0] = (this._state[0] + a) >>> 0;
    this._state[1] = (this._state[1] + b) >>> 0;
    this._state[2] = (this._state[2] + c) >>> 0;
    this._state[3] = (this._state[3] + d) >>> 0;
    this._state[4] = (this._state[4] + e) >>> 0;
    this._state[5] = (this._state[5] + f) >>> 0;
    this._state[6] = (this._state[6] + g) >>> 0;
    this._state[7] = (this._state[7] + h) >>> 0;
  }
}
