import { normalizeFsPath } from './shared.js';

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function concatChunks(chunks, totalLength) {
  const merged = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return merged;
}

function isZeroBlock(block) {
  for (let index = 0; index < block.byteLength; index += 1) {
    if (block[index] !== 0) return false;
  }
  return true;
}

function parseTarString(bytes) {
  let end = bytes.byteLength;
  for (let index = 0; index < bytes.byteLength; index += 1) {
    if (bytes[index] === 0) {
      end = index;
      break;
    }
  }
  return textDecoder.decode(bytes.subarray(0, end)).trim();
}

function parseTarNumber(bytes) {
  if (!bytes || bytes.byteLength === 0) return 0;

  if (bytes[0] & 0x80) {
    let value = 0n;
    const negative = bytes[0] === 0xff;
    for (const byte of bytes) {
      value = (value << 8n) | BigInt(byte);
    }
    if (negative) {
      const bits = BigInt(bytes.byteLength * 8);
      value -= 1n << bits;
    } else {
      value &= ~(1n << BigInt(bytes.byteLength * 8 - 1));
    }
    return Number(value);
  }

  const raw = parseTarString(bytes).replace(/\0/g, '').trim();
  if (!raw) return 0;
  return Number.parseInt(raw.replace(/\s+$/g, ''), 8) || 0;
}

function normalizeTarPath(rawPath, typeflag) {
  const cleaned = String(rawPath || '').replace(/\0/g, '').replace(/^\.\/+/, '').replace(/^\/+/, '');
  if (!cleaned || cleaned === '.') return '/';
  const withoutTrailingSlash = typeflag === '5' ? cleaned.replace(/\/+$/, '') : cleaned;
  return normalizeFsPath(withoutTrailingSlash);
}

function parsePax(bodyBytes) {
  const records = {};
  const text = textDecoder.decode(bodyBytes);
  let cursor = 0;
  while (cursor < text.length) {
    const spaceIndex = text.indexOf(' ', cursor);
    if (spaceIndex < 0) break;
    const length = Number.parseInt(text.slice(cursor, spaceIndex), 10);
    if (!Number.isFinite(length) || length <= 0) break;
    const record = text.slice(spaceIndex + 1, cursor + length - 1);
    const equalsIndex = record.indexOf('=');
    if (equalsIndex > 0) {
      const key = record.slice(0, equalsIndex);
      const value = record.slice(equalsIndex + 1);
      records[key] = value;
    }
    cursor += length;
  }
  return records;
}

function mergePax(globalPax, localPax) {
  if (!globalPax && !localPax) return null;
  return {
    ...(globalPax || {}),
    ...(localPax || {})
  };
}

async function readExactly(reader, state, bytesToRead, captureLimit = 0) {
  const chunks = [];
  let collected = 0;
  let remaining = bytesToRead;

  while (remaining > 0) {
    if (state.buffer.byteLength === 0) {
      const next = await reader.read();
      if (next.done) {
        throw new Error('Unexpected end of tar stream.');
      }
      state.buffer = next.value;
      state.offset = 0;
    }

    const available = state.buffer.byteLength - state.offset;
    const take = Math.min(available, remaining);
    if (captureLimit > collected) {
      const captureCount = Math.min(take, captureLimit - collected);
      chunks.push(state.buffer.subarray(state.offset, state.offset + captureCount));
      collected += captureCount;
    }

    state.offset += take;
    remaining -= take;
    state.absoluteOffset += take;

    if (state.offset >= state.buffer.byteLength) {
      state.buffer = new Uint8Array(0);
      state.offset = 0;
    }
  }

  return captureLimit > 0 ? concatChunks(chunks, collected) : null;
}

function buildHeaderDescriptor(header, globalPax, localPax, longName, longLink) {
  const pax = mergePax(globalPax, localPax);
  const name = pax?.path || longName || parseTarString(header.subarray(0, 100));
  const prefix = parseTarString(header.subarray(345, 500));
  const rawPath = prefix && !name.startsWith(prefix) ? `${prefix}/${name}` : name;
  const typeflag = String.fromCharCode(header[156] || 48);
  const linkname = pax?.linkpath || longLink || parseTarString(header.subarray(157, 257));
  const xattrs = {};
  if (pax) {
    for (const [key, value] of Object.entries(pax)) {
      if (key.startsWith('SCHILY.xattr.')) {
        xattrs[key.slice('SCHILY.xattr.'.length)] = value;
      }
    }
  }

  return {
    path: normalizeTarPath(rawPath, typeflag),
    rawPath,
    typeflag,
    mode: parseTarNumber(header.subarray(100, 108)),
    uid: parseTarNumber(header.subarray(108, 116)),
    gid: parseTarNumber(header.subarray(116, 124)),
    size: parseTarNumber(header.subarray(124, 136)),
    mtime: parseTarNumber(header.subarray(136, 148)),
    linkname,
    uname: pax?.uname || parseTarString(header.subarray(265, 297)),
    gname: pax?.gname || parseTarString(header.subarray(297, 329)),
    devmajor: parseTarNumber(header.subarray(329, 337)),
    devminor: parseTarNumber(header.subarray(337, 345)),
    xattrs,
    pax
  };
}

export async function scanTarStream(stream, options = {}) {
  const reader = stream.getReader();
  const state = {
    buffer: new Uint8Array(0),
    offset: 0,
    absoluteOffset: 0
  };
  const captureLimit = Number(options.captureLimit || 0);
  let globalPax = null;
  let nextLocalPax = null;
  let longName = null;
  let longLink = null;
  let captureResult = null;

  while (true) {
    if (options.signal?.aborted) {
      throw new DOMException('The tar scan was aborted.', 'AbortError');
    }

    const headerOffset = state.absoluteOffset;
    const header = await readExactly(reader, state, 512, 512);
    if (isZeroBlock(header)) {
      break;
    }

    const descriptor = buildHeaderDescriptor(header, globalPax, nextLocalPax, longName, longLink);
    nextLocalPax = null;
    longName = null;
    longLink = null;

    const bodyOffset = state.absoluteOffset;
    const shouldCapture = captureResult === null && descriptor.path === normalizeFsPath(options.capturePath || '');

    if (descriptor.typeflag === 'x' || descriptor.typeflag === 'g') {
      const body = await readExactly(reader, state, descriptor.size, descriptor.size);
      const padBytes = (512 - (descriptor.size % 512)) % 512;
      if (padBytes) {
        await readExactly(reader, state, padBytes);
      }
      const records = parsePax(body);
      if (descriptor.typeflag === 'g') {
        globalPax = {
          ...(globalPax || {}),
          ...records
        };
      } else {
        nextLocalPax = records;
      }
      continue;
    }

    if (descriptor.typeflag === 'L' || descriptor.typeflag === 'K') {
      const body = await readExactly(reader, state, descriptor.size, descriptor.size);
      const padBytes = (512 - (descriptor.size % 512)) % 512;
      if (padBytes) {
        await readExactly(reader, state, padBytes);
      }
      const value = textDecoder.decode(body).replace(/\0+$/, '');
      if (descriptor.typeflag === 'L') {
        longName = value;
      } else {
        longLink = value;
      }
      continue;
    }

    const entry = {
      ...descriptor,
      headerOffset,
      bodyOffset,
      type: tarTypeToEntryType(descriptor.typeflag),
      capture: null
    };

    const captureBytes =
      shouldCapture && entry.type === 'file'
        ? await readExactly(reader, state, descriptor.size, Math.min(captureLimit, descriptor.size))
        : await readExactly(reader, state, descriptor.size);

    const padBytes = (512 - (descriptor.size % 512)) % 512;
    if (padBytes) {
      await readExactly(reader, state, padBytes);
    }

    if (shouldCapture && entry.type === 'file') {
      captureResult = {
        entry,
        bytes: captureBytes || new Uint8Array(0),
        truncated: descriptor.size > captureLimit
      };
    }

    if (typeof options.onEntry === 'function') {
      await options.onEntry(entry);
    }
  }

  await reader.releaseLock?.();
  return captureResult;
}

function tarTypeToEntryType(typeflag) {
  switch (typeflag) {
    case '0':
    case '\0':
    case '7':
      return 'file';
    case '1':
      return 'hardlink';
    case '2':
      return 'symlink';
    case '3':
      return 'char';
    case '4':
      return 'block';
    case '5':
      return 'dir';
    case '6':
      return 'fifo';
    default:
      return 'other';
  }
}

function splitNamePrefix(name) {
  const bytes = textEncoder.encode(name);
  if (bytes.byteLength <= 100) {
    return { name, prefix: '' };
  }

  const parts = name.split('/');
  while (parts.length > 1) {
    const entryName = parts.pop();
    const prefix = parts.join('/');
    const entryBytes = textEncoder.encode(entryName);
    const prefixBytes = textEncoder.encode(prefix);
    if (entryBytes.byteLength <= 100 && prefixBytes.byteLength <= 155) {
      return { name: entryName, prefix };
    }
  }

  throw new Error(`Tar path is too long for the USTAR header: ${name}`);
}

function writeString(target, offset, length, value) {
  const encoded = textEncoder.encode(String(value || ''));
  target.set(encoded.subarray(0, length), offset);
}

function writeOctal(target, offset, length, value) {
  const raw = Math.max(0, Number(value || 0)).toString(8);
  const padded = raw.padStart(length - 1, '0');
  writeString(target, offset, length - 1, padded);
  target[offset + length - 1] = 0;
}

function createTarHeader(name, size, options = {}) {
  const { name: entryName, prefix } = splitNamePrefix(name);
  const header = new Uint8Array(512);

  writeString(header, 0, 100, entryName);
  writeOctal(header, 100, 8, options.mode ?? (options.typeflag === '5' ? 0o755 : 0o644));
  writeOctal(header, 108, 8, options.uid ?? 0);
  writeOctal(header, 116, 8, options.gid ?? 0);
  writeOctal(header, 124, 12, size);
  writeOctal(header, 136, 12, options.mtime ?? Math.floor(Date.now() / 1000));
  header.fill(0x20, 148, 156);
  writeString(header, 156, 1, options.typeflag || '0');
  writeString(header, 157, 100, options.linkname || '');
  writeString(header, 257, 6, 'ustar');
  writeString(header, 263, 2, '00');
  writeString(header, 265, 32, options.uname || 'root');
  writeString(header, 297, 32, options.gname || 'root');
  writeOctal(header, 329, 8, options.devmajor ?? 0);
  writeOctal(header, 337, 8, options.devminor ?? 0);
  writeString(header, 345, 155, prefix);

  let checksum = 0;
  for (const byte of header) {
    checksum += byte;
  }
  writeOctal(header, 148, 8, checksum);
  return header;
}

async function copyStream(reader, writer, onChunk) {
  while (true) {
    const next = await reader.read();
    if (next.done) break;
    await writer.write(next.value);
    if (typeof onChunk === 'function') {
      onChunk(next.value.byteLength);
    }
  }
}

export class TarWriter {
  constructor(writable) {
    this.writable = writable;
    this.bytesWritten = 0;
  }

  async writeBytes(name, bytes, options = {}) {
    const data = bytes instanceof Uint8Array ? bytes : textEncoder.encode(String(bytes || ''));
    await this.writable.write(createTarHeader(name, data.byteLength, options));
    await this.writable.write(data);
    this.bytesWritten += 512 + data.byteLength;
    await this._writePadding(data.byteLength);
  }

  async writeFile(name, file, options = {}, onProgress) {
    await this.writable.write(createTarHeader(name, file.size, options));
    this.bytesWritten += 512;
    const reader = file.stream().getReader();
    await copyStream(reader, this.writable, (length) => {
      this.bytesWritten += length;
      if (typeof onProgress === 'function') {
        onProgress(length);
      }
    });
    await this._writePadding(file.size);
  }

  async finish() {
    const trailer = new Uint8Array(1024);
    await this.writable.write(trailer);
    this.bytesWritten += trailer.byteLength;
    await this.writable.close();
  }

  async _writePadding(size) {
    const padding = (512 - (size % 512)) % 512;
    if (padding > 0) {
      await this.writable.write(new Uint8Array(padding));
      this.bytesWritten += padding;
    }
  }
}
