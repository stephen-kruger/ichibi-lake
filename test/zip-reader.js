/**
 * Test helper: read a whole ZIP archive from a Buffer via its central
 * directory (ZIP64 aware), inflate each entry and verify its CRC-32.
 * Returns [{ name, method, data }]. Throws on any structural problem.
 */
import zlib from 'zlib';

export function readZip(buf) {
    let eocd = -1;
    for (let i = buf.length - 22; i >= Math.max(0, buf.length - 22 - 65535); i--) {
        if (buf.readUInt32LE(i) === 0x06054b50) { eocd = i; break; }
    }
    if (eocd < 0) throw new Error('zip: end of central directory not found');
    let count = buf.readUInt16LE(eocd + 10);
    let cdOffset = buf.readUInt32LE(eocd + 16);
    if (eocd >= 20 && buf.readUInt32LE(eocd - 20) === 0x07064b50) {
        const rec = Number(buf.readBigUInt64LE(eocd - 20 + 8));
        if (buf.readUInt32LE(rec) !== 0x06064b50) throw new Error('zip: bad ZIP64 end record');
        count = Number(buf.readBigUInt64LE(rec + 32));
        cdOffset = Number(buf.readBigUInt64LE(rec + 48));
    }
    const out = [];
    let p = cdOffset;
    for (let n = 0; n < count; n++) {
        if (buf.readUInt32LE(p) !== 0x02014b50) throw new Error(`zip: bad central header #${n}`);
        const method = buf.readUInt16LE(p + 10);
        const crc = buf.readUInt32LE(p + 16);
        let csize = buf.readUInt32LE(p + 20);
        let usize = buf.readUInt32LE(p + 24);
        const nameLen = buf.readUInt16LE(p + 28);
        const extraLen = buf.readUInt16LE(p + 30);
        const commentLen = buf.readUInt16LE(p + 32);
        let offset = buf.readUInt32LE(p + 42);
        const name = buf.toString('utf8', p + 46, p + 46 + nameLen);
        // ZIP64 extended information: present fields follow the 0xFFFFFFFF markers in order.
        let e = p + 46 + nameLen;
        const extraEnd = e + extraLen;
        while (e + 4 <= extraEnd) {
            const id = buf.readUInt16LE(e);
            const len = buf.readUInt16LE(e + 2);
            if (id === 0x0001) {
                let q = e + 4;
                if (usize === 0xffffffff) { usize = Number(buf.readBigUInt64LE(q)); q += 8; }
                if (csize === 0xffffffff) { csize = Number(buf.readBigUInt64LE(q)); q += 8; }
                if (offset === 0xffffffff) { offset = Number(buf.readBigUInt64LE(q)); q += 8; }
            }
            e += 4 + len;
        }
        if (buf.readUInt32LE(offset) !== 0x04034b50) throw new Error(`zip: bad local header for ${name}`);
        const dataStart = offset + 30 + buf.readUInt16LE(offset + 26) + buf.readUInt16LE(offset + 28);
        const raw = buf.subarray(dataStart, dataStart + csize);
        const data = method === 8 ? zlib.inflateRawSync(raw) : method === 0 ? Buffer.from(raw) : null;
        if (!data) throw new Error(`zip: unsupported method ${method} for ${name}`);
        if (data.length !== usize) throw new Error(`zip: size mismatch for ${name}`);
        if ((zlib.crc32(data) >>> 0) !== crc) throw new Error(`zip: CRC mismatch for ${name}`);
        out.push({ name, method, data });
        p = extraEnd + commentLen;
    }
    return out;
}
