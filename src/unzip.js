/**
 * Minimal ZIP reader for console imports (the counterpart of src/zip.js).
 *
 * It reads the archive through its central directory, so entries written with
 * data descriptors (as ZipWriter does) are supported, and it handles ZIP64
 * sizes and offsets. Extraction streams one entry at a time to a temp file, so
 * an archive never has to fit in memory. Only the two methods our exports use
 * are accepted: stored (0) and deflate (8). Encrypted and multi-disk archives
 * are refused.
 */
import fs from 'fs';
import fsp from 'fs/promises';
import zlib from 'zlib';
import { Transform, Writable } from 'stream';
import { pipeline } from 'stream/promises';
import { crc32 } from './zip.js';

const U16_MAX = 0xffff;
const U32_MAX = 0xffffffff;
const SIG_EOCD = 0x06054b50;
const SIG_ZIP64_EOCD = 0x06064b50;
const SIG_ZIP64_LOCATOR = 0x07064b50;
const SIG_CENTRAL = 0x02014b50;
const SIG_LOCAL = 0x04034b50;
const ZIP64_EXTRA_ID = 0x0001;
const MAX_CENTRAL_DIRECTORY_BYTES = 64 * 1024 * 1024;
const MAX_ENTRIES = 100000;

function zipError(message) {
    return Object.assign(new Error(`ZIP archive: ${message}`), { statusCode: 400 });
}

function readU64(buf, offset) {
    const value = buf.readBigUInt64LE(offset);
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw zipError('a size is too large to read');
    return Number(value);
}

async function readExactly(fh, buf, offset, length, position) {
    let done = 0;
    while (done < length) {
        const { bytesRead } = await fh.read(buf, offset + done, length - done, position + done);
        if (!bytesRead) throw zipError('the file ended in the middle of the central directory');
        done += bytesRead;
    }
}

/** Entries in an archive, in central-directory order. Directories included. */
export async function listZipEntries(filePath) {
    const fh = await fsp.open(filePath, 'r');
    try {
        const { size } = await fh.stat();
        if (size < 22) throw zipError('not a ZIP archive');
        const tailLen = Math.min(size, 22 + U16_MAX);
        const tail = Buffer.alloc(tailLen);
        await readExactly(fh, tail, 0, tailLen, size - tailLen);

        let eocd = -1;
        for (let i = tail.length - 22; i >= 0; i--) {
            if (tail.readUInt32LE(i) === SIG_EOCD) { eocd = i; break; }
        }
        if (eocd < 0) throw zipError('end of central directory not found');

        let count = tail.readUInt16LE(eocd + 10);
        let cdSize = tail.readUInt32LE(eocd + 12);
        let cdOffset = tail.readUInt32LE(eocd + 16);

        if (eocd >= 20 && tail.readUInt32LE(eocd - 20) === SIG_ZIP64_LOCATOR) {
            const recOffset = readU64(tail, eocd - 20 + 8);
            if (recOffset + 56 > size) throw zipError('the ZIP64 end record is out of range');
            const rec = Buffer.alloc(56);
            await readExactly(fh, rec, 0, 56, recOffset);
            if (rec.readUInt32LE(0) !== SIG_ZIP64_EOCD) throw zipError('bad ZIP64 end record');
            count = readU64(rec, 32);
            cdSize = readU64(rec, 40);
            cdOffset = readU64(rec, 48);
        } else if (count === U16_MAX || cdSize === U32_MAX || cdOffset === U32_MAX) {
            throw zipError('the ZIP64 end record is missing');
        }

        if (count > MAX_ENTRIES) throw zipError(`more than ${MAX_ENTRIES} entries`);
        if (cdSize > MAX_CENTRAL_DIRECTORY_BYTES) throw zipError('the central directory is too large');
        if (cdOffset + cdSize > size) throw zipError('the central directory is out of range');
        if (cdSize === 0) return [];

        const cd = Buffer.alloc(cdSize);
        await readExactly(fh, cd, 0, cdSize, cdOffset);
        const entries = [];
        let p = 0;
        for (let n = 0; n < count; n++) {
            if (p + 46 > cd.length || cd.readUInt32LE(p) !== SIG_CENTRAL) {
                throw zipError(`bad central directory header #${n + 1}`);
            }
            const flags = cd.readUInt16LE(p + 8);
            const method = cd.readUInt16LE(p + 10);
            const crc = cd.readUInt32LE(p + 16);
            let csize = cd.readUInt32LE(p + 20);
            let usize = cd.readUInt32LE(p + 24);
            const nameLen = cd.readUInt16LE(p + 28);
            const extraLen = cd.readUInt16LE(p + 30);
            const commentLen = cd.readUInt16LE(p + 32);
            let disk = cd.readUInt16LE(p + 34);
            let offset = cd.readUInt32LE(p + 42);
            const nameEnd = p + 46 + nameLen;
            const extraEnd = nameEnd + extraLen;
            if (extraEnd + commentLen > cd.length) throw zipError(`central directory header #${n + 1} is truncated`);
            const name = cd.toString('utf8', p + 46, nameEnd);
            if (!name) throw zipError(`central directory header #${n + 1} has no name`);

            let q = nameEnd;
            while (q + 4 <= extraEnd) {
                const id = cd.readUInt16LE(q);
                const len = cd.readUInt16LE(q + 2);
                if (id === ZIP64_EXTRA_ID) {
                    let r = q + 4;
                    if (usize === U32_MAX) { usize = readU64(cd, r); r += 8; }
                    if (csize === U32_MAX) { csize = readU64(cd, r); r += 8; }
                    if (offset === U32_MAX) { offset = readU64(cd, r); r += 8; }
                    if (disk === U16_MAX) { disk = cd.readUInt32LE(r); r += 4; }
                }
                q += 4 + len;
            }
            if (flags & 0x0001) throw zipError(`entry "${name}" is encrypted`);
            if (method !== 0 && method !== 8) throw zipError(`entry "${name}" uses unsupported compression method ${method}`);
            if (disk !== 0) throw zipError('multi-disk archives are not supported');
            if (offset + csize > size) throw zipError(`entry "${name}" is out of range`);

            entries.push({ name, flags, method, crc, csize, usize, offset });
            p = extraEnd + commentLen;
        }
        return entries;
    } finally {
        await fh.close();
    }
}

/** Where an entry's bytes start, from its local header. */
async function dataStart(filePath, entry) {
    const fh = await fsp.open(filePath, 'r');
    try {
        const { size } = await fh.stat();
        if (entry.offset + 30 > size) throw zipError(`entry "${entry.name}" is out of range`);
        const header = Buffer.alloc(30);
        await readExactly(fh, header, 0, 30, entry.offset);
        if (header.readUInt32LE(0) !== SIG_LOCAL) throw zipError(`bad local header for "${entry.name}"`);
        const start = entry.offset + 30 + header.readUInt16LE(26) + header.readUInt16LE(28);
        if (start + entry.csize > size) throw zipError(`entry "${entry.name}" is out of range`);
        return start;
    } finally {
        await fh.close();
    }
}

/** Streams for an entry's compressed bytes and, for deflate, its inflater. */
async function entryStreams(filePath, entry) {
    const start = await dataStart(filePath, entry);
    const streams = [fs.createReadStream(filePath, { start, end: start + entry.csize - 1 })];
    if (entry.method === 8) streams.push(zlib.createInflateRaw());
    return streams;
}

/**
 * Extract one entry to `destPath`, streaming and verifying its CRC-32 and
 * size. `maxBytes` caps the uncompressed output (a zip-bomb guard); throws
 * with statusCode 400 on any problem. Returns the number of bytes written.
 */
export async function extractZipEntry(filePath, entry, destPath, { maxBytes = Infinity } = {}) {
    if (entry.usize > maxBytes) throw zipError(`entry "${entry.name}" is larger than the remaining import limit`);
    if (entry.csize === 0) {
        if (entry.usize !== 0 || entry.crc !== 0) throw zipError(`entry "${entry.name}" failed its CRC check`);
        await fsp.writeFile(destPath, '');
        return 0;
    }
    const streams = await entryStreams(filePath, entry);
    let crc = 0;
    let size = 0;
    const meter = new Transform({
        transform(chunk, enc, cb) {
            size += chunk.length;
            if (size > maxBytes) return cb(zipError(`entry "${entry.name}" expands beyond the remaining import limit`));
            crc = crc32(chunk, crc);
            cb(null, chunk);
        },
    });
    streams.push(meter, fs.createWriteStream(destPath));
    try {
        await pipeline(...streams);
    } catch (err) {
        await fsp.unlink(destPath).catch(() => {});
        if (err.statusCode) throw err;
        throw zipError(`could not extract "${entry.name}": ${err.message}`);
    }
    if (size !== entry.usize) {
        await fsp.unlink(destPath).catch(() => {});
        throw zipError(`entry "${entry.name}" has the wrong size`);
    }
    if ((crc >>> 0) !== (entry.crc >>> 0)) {
        await fsp.unlink(destPath).catch(() => {});
        throw zipError(`entry "${entry.name}" failed its CRC check`);
    }
    return size;
}

/** Read one entry into memory (for small metadata such as manifest.json). */
export async function readZipEntry(filePath, entry, { maxBytes = 4 * 1024 * 1024 } = {}) {
    if (entry.usize > maxBytes) throw zipError(`entry "${entry.name}" is too large to read`);
    if (entry.csize === 0) {
        if (entry.usize !== 0 || entry.crc !== 0) throw zipError(`entry "${entry.name}" failed its CRC check`);
        return Buffer.alloc(0);
    }
    const streams = await entryStreams(filePath, entry);
    const chunks = [];
    let crc = 0;
    let size = 0;
    streams.push(new Writable({
        write(chunk, enc, cb) {
            size += chunk.length;
            if (size > maxBytes) return cb(zipError(`entry "${entry.name}" is too large to read`));
            crc = crc32(chunk, crc);
            chunks.push(Buffer.from(chunk));
            cb();
        },
    }));
    try {
        await pipeline(...streams);
    } catch (err) {
        if (err.statusCode) throw err;
        throw zipError(`could not read "${entry.name}": ${err.message}`);
    }
    if (size !== entry.usize || (crc >>> 0) !== (entry.crc >>> 0)) throw zipError(`entry "${entry.name}" failed its CRC check`);
    return Buffer.concat(chunks);
}
