/**
 * Minimal streaming ZIP writer (no dependencies), used by multi-table exports.
 *
 * Entries are written one after another straight to a sink, so an archive of
 * any size needs no temp file and no buffering beyond one chunk. Sizes and
 * CRC-32 are not known until an entry ends, so each entry uses a data
 * descriptor (general-purpose flag bit 3) and the central directory at the end
 * carries the authoritative values, as Go's archive/zip does. ZIP64 records are
 * added only when an entry, offset or entry count exceeds the 32/16-bit limits,
 * so ordinary archives stay readable by every unzip tool.
 *
 *   const zip = new ZipWriter(chunk => write(res, chunk));
 *   const entry = await zip.entry('a.csv', { compress: true });
 *   await entry.write(buffer); ...; await entry.close();
 *   await zip.finish();
 *
 * `sink` is an async function that resolves when the chunk may be followed by
 * the next one (backpressure). Names are stored as UTF-8 (flag bit 11).
 */
import zlib from 'zlib';

const U32_MAX = 0xffffffff;
const U16_MAX = 0xffff;
const FLAGS = 0x0008 | 0x0800; // data descriptor, UTF-8 names
const METHOD_STORE = 0;
const METHOD_DEFLATE = 8;
const ZIP64_EXTRA_ID = 0x0001;

// zlib.crc32 needs Node >= 22.2; keep a table fallback for older runtimes.
let crcTable = null;
function crc32(buf, prev = 0) {
    if (typeof zlib.crc32 === 'function') return zlib.crc32(buf, prev);
    if (!crcTable) {
        crcTable = new Uint32Array(256);
        for (let n = 0; n < 256; n++) {
            let c = n;
            for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
            crcTable[n] = c >>> 0;
        }
    }
    let c = (prev ^ U32_MAX) >>> 0;
    for (let i = 0; i < buf.length; i++) c = crcTable[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
    return (c ^ U32_MAX) >>> 0;
}

function dosDateTime(d) {
    const year = Math.max(1980, d.getFullYear());
    return {
        time: (d.getHours() << 11) | (d.getMinutes() << 5) | Math.floor(d.getSeconds() / 2),
        date: ((year - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate(),
    };
}

export class ZipWriter {
    constructor(sink) {
        this.sink = sink;
        this.offset = 0;
        this.entries = [];
        this.open = null;
        this.finished = false;
    }

    async #emit(buf) {
        this.offset += buf.length;
        await this.sink(buf);
    }

    /** Start an entry. Only one entry can be open at a time. */
    async entry(name, { compress = true, date = new Date() } = {}) {
        if (this.open) throw new Error('ZipWriter: close the previous entry first');
        if (this.finished) throw new Error('ZipWriter: archive already finished');
        const nameBuf = Buffer.from(name, 'utf8');
        if (nameBuf.length > U16_MAX) throw new Error('ZipWriter: entry name too long');
        const { time, date: dosDate } = dosDateTime(date);
        const method = compress ? METHOD_DEFLATE : METHOD_STORE;
        const rec = { nameBuf, method, time, date: dosDate, offset: this.offset, crc: 0, csize: 0, usize: 0 };

        const header = Buffer.alloc(30);
        header.writeUInt32LE(0x04034b50, 0);
        header.writeUInt16LE(20, 4);             // version needed
        header.writeUInt16LE(FLAGS, 6);
        header.writeUInt16LE(method, 8);
        header.writeUInt16LE(time, 10);
        header.writeUInt16LE(dosDate, 12);
        // crc, sizes: 0 here, real values in the data descriptor
        header.writeUInt16LE(nameBuf.length, 26);
        header.writeUInt16LE(0, 28);             // extra length
        await this.#emit(Buffer.concat([header, nameBuf]));

        // Deflate output is drained by a consumer task so the writer can apply
        // backpressure on the deflater and the deflater on the sink.
        let deflate = null;
        let drained = null;
        if (compress) {
            deflate = zlib.createDeflateRaw({ level: 6 });
            drained = (async () => {
                for await (const out of deflate) {
                    rec.csize += out.length;
                    await this.#emit(out);
                }
            })();
            drained.catch(() => {}); // surfaced by close()
        }

        const entry = {
            write: async (chunk) => {
                if (!chunk || chunk.length === 0) return;
                const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, 'utf8');
                rec.crc = crc32(buf, rec.crc);
                rec.usize += buf.length;
                if (!deflate) {
                    rec.csize += buf.length;
                    await this.#emit(buf);
                } else if (!deflate.write(buf)) {
                    await new Promise((resolve, reject) => {
                        const done = (err) => { deflate.off('drain', done); deflate.off('error', done); err ? reject(err) : resolve(); };
                        deflate.once('drain', done);
                        deflate.once('error', done);
                    });
                }
            },
            close: async () => {
                if (deflate) {
                    deflate.end();
                    await drained;
                }
                const zip64 = rec.csize >= U32_MAX || rec.usize >= U32_MAX;
                const desc = Buffer.alloc(zip64 ? 24 : 16);
                desc.writeUInt32LE(0x08074b50, 0);
                desc.writeUInt32LE(rec.crc >>> 0, 4);
                if (zip64) {
                    desc.writeBigUInt64LE(BigInt(rec.csize), 8);
                    desc.writeBigUInt64LE(BigInt(rec.usize), 16);
                } else {
                    desc.writeUInt32LE(rec.csize, 8);
                    desc.writeUInt32LE(rec.usize, 12);
                }
                await this.#emit(desc);
                this.entries.push(rec);
                this.open = null;
            },
            abort: () => { if (deflate) deflate.destroy(); this.open = null; },
        };
        this.open = entry;
        return entry;
    }

    /** Add a whole entry from a Buffer or string. */
    async add(name, data, opts) {
        const e = await this.entry(name, opts);
        await e.write(data);
        await e.close();
    }

    /** Write the central directory and end records. */
    async finish() {
        if (this.open) throw new Error('ZipWriter: close the open entry first');
        this.finished = true;
        const cdStart = this.offset;
        for (const e of this.entries) {
            const extra = [];
            if (e.usize >= U32_MAX) extra.push(BigInt(e.usize));
            if (e.csize >= U32_MAX) extra.push(BigInt(e.csize));
            if (e.offset >= U32_MAX) extra.push(BigInt(e.offset));
            const extraBuf = Buffer.alloc(extra.length ? 4 + 8 * extra.length : 0);
            if (extra.length) {
                extraBuf.writeUInt16LE(ZIP64_EXTRA_ID, 0);
                extraBuf.writeUInt16LE(8 * extra.length, 2);
                extra.forEach((v, i) => extraBuf.writeBigUInt64LE(v, 4 + 8 * i));
            }
            const version = extra.length ? 45 : 20;
            const h = Buffer.alloc(46);
            h.writeUInt32LE(0x02014b50, 0);
            h.writeUInt16LE((3 << 8) | version, 4); // made by: UNIX
            h.writeUInt16LE(version, 6);
            h.writeUInt16LE(FLAGS, 8);
            h.writeUInt16LE(e.method, 10);
            h.writeUInt16LE(e.time, 12);
            h.writeUInt16LE(e.date, 14);
            h.writeUInt32LE(e.crc >>> 0, 16);
            h.writeUInt32LE(Math.min(e.csize, U32_MAX), 20);
            h.writeUInt32LE(Math.min(e.usize, U32_MAX), 24);
            h.writeUInt16LE(e.nameBuf.length, 28);
            h.writeUInt16LE(extraBuf.length, 30);
            // comment length, disk, internal attributes: 0
            h.writeUInt32LE((0o100644 << 16) >>> 0, 38); // regular file, rw-r--r--
            h.writeUInt32LE(Math.min(e.offset, U32_MAX), 42);
            await this.#emit(Buffer.concat([h, e.nameBuf, extraBuf]));
        }
        const cdSize = this.offset - cdStart;
        const count = this.entries.length;
        if (count >= U16_MAX || cdSize >= U32_MAX || cdStart >= U32_MAX) {
            const eocd64Offset = this.offset;
            const r = Buffer.alloc(56);
            r.writeUInt32LE(0x06064b50, 0);
            r.writeBigUInt64LE(44n, 4);   // size of the rest of this record
            r.writeUInt16LE((3 << 8) | 45, 12);
            r.writeUInt16LE(45, 14);
            // disk numbers: 0
            r.writeBigUInt64LE(BigInt(count), 24);
            r.writeBigUInt64LE(BigInt(count), 32);
            r.writeBigUInt64LE(BigInt(cdSize), 40);
            r.writeBigUInt64LE(BigInt(cdStart), 48);
            const loc = Buffer.alloc(20);
            loc.writeUInt32LE(0x07064b50, 0);
            loc.writeBigUInt64LE(BigInt(eocd64Offset), 8);
            loc.writeUInt32LE(1, 16);     // total disks
            await this.#emit(Buffer.concat([r, loc]));
        }
        const end = Buffer.alloc(22);
        end.writeUInt32LE(0x06054b50, 0);
        end.writeUInt16LE(Math.min(count, U16_MAX), 8);
        end.writeUInt16LE(Math.min(count, U16_MAX), 10);
        end.writeUInt32LE(Math.min(cdSize, U32_MAX), 12);
        end.writeUInt32LE(Math.min(cdStart, U32_MAX), 16);
        await this.#emit(end);
    }
}
