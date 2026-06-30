import { DuckDBInstance } from '@duckdb/node-api';

const inst = await DuckDBInstance.create(':memory:');
const conn = await inst.connect();

await conn.run(`CREATE TABLE t (
    event_id VARCHAR, person_id VARCHAR, identification_confidence VARCHAR,
    created VARCHAR, event_type VARCHAR
)`);

const rows = [
    ['a1', '102020058', '0.7985730419225642', '2024-01-01', 'A'],
    ['b2', '201234567', '0.9123456789012345', '2024-01-02', 'B'],
    ['c3', null, null, '2024-01-03', 'C'],
];

for (const r of rows) {
    await conn.run('INSERT INTO t VALUES ($1,$2,$3,$4,$5)', r, []);
}

// Simulate what executeUserSql does: run sql, get result, call _readRows
async function _readRows(result) {
    const columns = result.columnNames();
    const rowSet = await result.getRows();
    const results = [];
    if (!rowSet) return results;
    const convertValue = (val) => {
        if (val === null || val === undefined) return null;
        if (typeof val === 'object' && val.constructor.name === 'DuckDBBlobValue' && val.bytes) {
            return Buffer.from(val.bytes).toString('base64');
        }
        if (val instanceof Uint8Array || Buffer.isBuffer(val)) {
            return Buffer.from(val).toString('base64');
        }
        return val;
    };
    if (typeof rowSet[Symbol.asyncIterator] === 'function' || typeof rowSet[Symbol.iterator] === 'function') {
        for await (const row of rowSet) {
            const obj = {};
            columns.forEach((col, idx) => { obj[col] = convertValue(row[idx]); });
            results.push(obj);
        }
    } else if (typeof rowSet.next === 'function') {
        let entry = rowSet.next();
        while (entry && !entry.done) {
            const row = entry.value;
            const obj = {};
            columns.forEach((col, idx) => { obj[col] = convertValue(row[idx]); });
            results.push(obj);
            entry = rowSet.next();
        }
    }
    return results;
}

async function test(label, sql) {
    const result = await conn.run(sql);
    const rows = await _readRows(result);
    console.log(`\n--- ${label} ---`);
    for (const r of rows) {
        console.log(`  event_id=${JSON.stringify(r.event_id)} person_id=${JSON.stringify(r.person_id)} conf=${JSON.stringify(r.identification_confidence)}`);
    }
}

await test('No ORDER BY', 'SELECT event_id, person_id, identification_confidence FROM t');
await test('ORDER BY event_id', 'SELECT event_id, person_id, identification_confidence FROM t ORDER BY event_id');
await test('ORDER BY created', 'SELECT event_id, person_id, identification_confidence FROM t ORDER BY created');
await test('ORDER BY person_id', 'SELECT event_id, person_id, identification_confidence FROM t ORDER BY person_id');

// Now try with different read patterns
async function test2(label, sql) {
    const result = await conn.run(sql);
    const columns = result.columnNames();
    
    // Method 1: getColumns + reconstruct
    const colValues = await result.getColumns();
    const rows1 = [];
    for (let ri = 0; ri < colValues[0].length; ri++) {
        const obj = {};
        columns.forEach((col, ci) => { obj[col] = colValues[ci][ri]; });
        rows1.push(obj);
    }
    console.log(`\n--- ${label} (getColumns) ---`);
    for (const r of rows1) {
        console.log(`  event_id=${JSON.stringify(r.event_id)} person_id=${JSON.stringify(r.person_id)} conf=${JSON.stringify(r.identification_confidence)}`);
    }
    
    // Method 2: getRowObjects
    const result2 = await conn.run(sql);
    const rows2 = await result2.getRowObjects();
    console.log(`\n--- ${label} (getRowObjects) ---`);
    for (const r of rows2) {
        console.log(`  event_id=${JSON.stringify(r.event_id)} person_id=${JSON.stringify(r.person_id)} conf=${JSON.stringify(r.identification_confidence)}`);
    }
    
    // Method 3: getColumnsObject
    const result3 = await conn.run(sql);
    const colObj = await result3.getColumnsObject();
    const rows3 = [];
    const firstCol = Object.values(colObj)[0];
    if (firstCol) {
        for (let ri = 0; ri < firstCol.length; ri++) {
            const obj = {};
            for (const col of columns) {
                obj[col] = colObj[col][ri];
            }
            rows3.push(obj);
        }
    }
    console.log(`\n--- ${label} (getColumnsObject) ---`);
    for (const r of rows3) {
        console.log(`  event_id=${JSON.stringify(r.event_id)} person_id=${JSON.stringify(r.person_id)} conf=${JSON.stringify(r.identification_confidence)}`);
    }
}

await test2('ORDER BY event_id', 'SELECT event_id, person_id, identification_confidence FROM t ORDER BY event_id');
await test2('ORDER BY created', 'SELECT event_id, person_id, identification_confidence FROM t ORDER BY created');

console.log('\nDone');
