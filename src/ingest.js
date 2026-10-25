/**
 * Batch ingestion shared by POST /upload/:tableName, POST /kafka-sink and the
 * Kafka consumer: create the table on first write, evolve its schema
 * (src/schema-evolution.js) and write the batch in one of three modes:
 *
 *   append  (default) INSERT every row.
 *   upsert  Rows replace existing rows with the same key; rows with a new key
 *           are inserted. Within the batch the last occurrence of a key wins
 *           (or the row with the highest `orderBy` value). A replaced row
 *           keeps its `created` value; `updated` is set to now.
 *   dedupe  Only rows whose key does not exist yet are inserted. Within the
 *           batch the first occurrence of a key wins (or the highest
 *           `orderBy` value).
 *
 * Keys are the table's declared key columns (src/table-keys.js). A request may
 * name them (`keys`) when the table has none yet; they are then declared.
 *
 * Concurrency
 * -----------
 * The keyed modes run one MERGE INTO per batch in a transaction on one
 * connection. DuckLake commits optimistically and detects conflicts per
 * table: two concurrent MERGEs into the same table (even with disjoint keys,
 * even when neither matches an existing row) cannot both commit; the loser
 * gets a "Transaction conflict" and is rolled back. That batch is retried
 * (INGEST_CONFLICT_RETRIES, default 10) and, if it keeps losing, the caller
 * gets a 409 and nothing of the batch is written. A plain
 * INSERT ... WHERE NOT EXISTS would NOT conflict with another one and could
 * insert the same new key twice, which is why dedupe also uses MERGE.
 * Plain appends never conflict with each other, so a mode=append write that
 * races with an upsert can still add a row with an existing key.
 */
import fs from 'fs/promises';
import path from 'path';
import { randomUUID } from 'crypto';
import { withConnection, invalidateColumnCache, VARCHAR } from './db.js';
import { resolveTypeConflict, formatSchemaDefinition } from './schema-evolution.js';
import { retryOnConflict, isTransactionConflict } from './conflict.js';
import { getTableKeys, setTableKeys, normaliseKeyList, assertKeysMatchColumns } from './table-keys.js';

export const INGEST_MODES = ['append', 'upsert', 'dedupe'];

// Position of each row in the batch, written into the spooled file for the
// keyed modes so "last/first occurrence wins" does not depend on scan order.
const SEQ_COL = '__ichibi_seq';
const READ_OPTIONS = 'sample_size=-1';
const SAFE_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
// `created` / `updated` are maintained by the keyed modes when the table has
// them with one of these types.
const TIMESTAMPISH_RE = /^(TIMESTAMP|DATE|VARCHAR)/i;

const conflictRetries = () => Math.max(1, parseInt(process.env.INGEST_CONFLICT_RETRIES || '10', 10) || 10);

function httpError(statusCode, message) {
    return Object.assign(new Error(message), { statusCode });
}

function q(name) {
    return `"${String(name).replace(/"/g, '""')}"`;
}

/** Validate a `mode` value (undefined -> 'append'). Throws 400. */
export function parseIngestMode(raw) {
    const mode = raw === undefined || raw === null || raw === '' ? 'append' : String(raw).toLowerCase();
    if (!INGEST_MODES.includes(mode)) {
        throw httpError(400, `Invalid mode "${raw}". Expected one of: ${INGEST_MODES.join(', ')}`);
    }
    return mode;
}

async function rowsOf(conn, sql, params, types) {
    const result = params ? await conn.run(sql, params, types) : await conn.run(sql);
    const names = result.columnNames();
    return (await result.getRows()).map(r => Object.fromEntries(names.map((n, i) => [n, r[i]])));
}

async function tableColumns(conn, tableName) {
    const rows = await rowsOf(conn,
        `SELECT column_name, data_type FROM information_schema.columns
         WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position`,
        [tableName], [VARCHAR]);
    return new Map(rows.map(r => [r.column_name, String(r.data_type)]));
}

/**
 * Add the batch's new columns to an existing table and widen STRUCT columns
 * whose shape changed. Returns the incoming columns that can be written
 * (columns with an incompatible type are skipped for this batch).
 */
async function evolveSchema(conn, tableName, sourceSql) {
    const existingCols = await tableColumns(conn, tableName);
    const incoming = (await rowsOf(conn, `DESCRIBE ${sourceSql}`))
        .map(r => ({ name: r.column_name, type: String(r.column_type) }))
        .filter(c => c.name !== SEQ_COL);

    const writable = [];
    let schemaChanged = false;
    for (const col of incoming) {
        const existingType = existingCols.get(col.name);
        if (existingType) {
            const resolution = resolveTypeConflict(existingType, col.type);
            if (resolution.action === 'skip') {
                console.warn(`[Schema Evolution] Skipping column "${col.name}" in ${tableName} for this batch: ${resolution.reason}`);
                continue;
            }
            if (resolution.action === 'widen') {
                console.log(`[Schema Evolution] Widening column "${col.name}" in ${tableName} from ${existingType} to ${resolution.newType}`);
                try {
                    await conn.run(`ALTER TABLE ${q(tableName)} ALTER COLUMN ${q(col.name)} SET DATA TYPE ${resolution.newType}`);
                    schemaChanged = true;
                } catch (alterErr) {
                    console.warn(`[Schema Evolution] Skipping column "${col.name}" in ${tableName} for this batch: ALTER failed (${alterErr.message || alterErr})`);
                    continue;
                }
            }
        } else {
            console.log(`[Schema Evolution] Adding column ${col.name} (${col.type}) to ${tableName}`);
            try {
                await conn.run(`ALTER TABLE ${q(tableName)} ADD COLUMN ${q(col.name)} ${col.type}`);
                schemaChanged = true;
            } catch (addErr) {
                console.warn(`[Schema Evolution] Skipping new column "${col.name}" in ${tableName} for this batch: ADD COLUMN failed (${addErr.message || addErr})`);
                continue;
            }
        }
        writable.push(col.name);
    }

    if (schemaChanged) {
        invalidateColumnCache(tableName);
        const schemaRows = await rowsOf(conn,
            `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position`,
            [tableName], [VARCHAR]);
        console.log(`[Schema Evolution] New schema for ${tableName}:\n${formatSchemaDefinition(tableName, schemaRows)}`);
    }
    return { incoming: new Set(incoming.map(c => c.name)), writable };
}

/** SELECT that keeps one row per key (see the module comment for which). */
function dedupSql(sourceSql, keys, mode, orderBy) {
    const order = [];
    if (orderBy) order.push(`${q(orderBy)} DESC NULLS LAST`);
    order.push(`${q(SEQ_COL)} ${mode === 'upsert' ? 'DESC' : 'ASC'}`);
    return `SELECT * FROM (${sourceSql}) QUALIFY row_number() OVER (PARTITION BY ${keys.map(q).join(', ')} ORDER BY ${order.join(', ')}) = 1`;
}

function nowFor(type) {
    return /^VARCHAR/i.test(type) ? 'CAST(now() AS VARCHAR)' : `CAST(now() AS ${type})`;
}

/** The MERGE INTO statement for a keyed batch. */
function buildMergeSql(tableName, mode, keys, tableCols, writable, sourceSql) {
    const writableSet = new Set(writable);
    const on = keys.map(k => `t.${q(k)} = CAST(s.${q(k)} AS ${tableCols.get(k)})`).join(' AND ');
    const createdType = tableCols.get('created');
    const updatedType = tableCols.get('updated');
    const manageCreated = createdType && TIMESTAMPISH_RE.test(createdType);
    const manageUpdated = updatedType && TIMESTAMPISH_RE.test(updatedType);

    // INSERT: every writable column, plus created/updated when managed.
    const insertCols = [];
    const insertVals = [];
    for (const col of writable) {
        if (col === 'created' && manageCreated) {
            insertCols.push(col); insertVals.push(`coalesce(s.${q(col)}, ${nowFor(createdType)})`);
        } else if (col === 'updated' && manageUpdated) {
            insertCols.push(col); insertVals.push(nowFor(updatedType));
        } else {
            insertCols.push(col); insertVals.push(`s.${q(col)}`);
        }
    }
    if (manageCreated && !writableSet.has('created')) { insertCols.push('created'); insertVals.push(nowFor(createdType)); }
    if (manageUpdated && !writableSet.has('updated')) { insertCols.push('updated'); insertVals.push(nowFor(updatedType)); }
    const insert = `WHEN NOT MATCHED THEN INSERT (${insertCols.map(q).join(', ')}) VALUES (${insertVals.join(', ')})`;

    if (mode === 'dedupe') {
        return `MERGE INTO ${q(tableName)} AS t USING (${sourceSql}) AS s ON ${on} ${insert}`;
    }

    // upsert replaces the whole row: columns the batch does not carry become
    // NULL, except `created` (kept) and `updated` (now).
    const sets = [];
    for (const [col, type] of tableCols) {
        if (keys.includes(col)) continue;
        if (col === 'created' && manageCreated) {
            sets.push(`${q(col)} = coalesce(t.${q(col)}, ${writableSet.has(col) ? `s.${q(col)}` : nowFor(type)})`);
        } else if (col === 'updated' && manageUpdated) {
            sets.push(`${q(col)} = ${nowFor(type)}`);
        } else if (writableSet.has(col)) {
            sets.push(`${q(col)} = s.${q(col)}`);
        } else {
            sets.push(`${q(col)} = NULL`);
        }
    }
    const matched = sets.length ? `WHEN MATCHED THEN UPDATE SET ${sets.join(', ')}` : 'WHEN MATCHED THEN DO NOTHING';
    return `MERGE INTO ${q(tableName)} AS t USING (${sourceSql}) AS s ON ${on} ${matched} ${insert}`;
}

/** Reject a batch whose key columns are missing or NULL. */
async function assertBatchKeys(conn, keys, incoming, writable, sourceSql, orderBy) {
    for (const k of keys) {
        if (!incoming.has(k)) throw httpError(400, `Key column "${k}" is missing from the batch`);
        if (!writable.includes(k)) throw httpError(400, `Key column "${k}" has a type in this batch that cannot be written to the table`);
    }
    if (orderBy && !incoming.has(orderBy)) throw httpError(400, `orderBy column "${orderBy}" is missing from the batch`);
    const nullCount = await rowsOf(conn,
        `SELECT count(*) AS n FROM (${sourceSql}) WHERE ${keys.map(k => `${q(k)} IS NULL`).join(' OR ')}`);
    const n = Number(nullCount[0]?.n || 0);
    if (n > 0) throw httpError(400, `${n} row(s) have a NULL value in key column(s) ${keys.join(', ')}`);
}

/**
 * Ingest `rows` (an array of plain objects) into `tableName`.
 *
 * options:
 *   mode              'append' | 'upsert' | 'dedupe'
 *   keys              key columns to declare when the table has none yet
 *                     (array or comma-separated string); must match the
 *                     declared keys when it has them
 *   keysAreDefault    true: `keys` only apply when the table has none
 *                     declared (no 409 on a mismatch)
 *   orderBy           keyed modes: within the batch keep the row with the
 *                     highest value of this column
 *   onCreated(conn)   called after this call created the table (ACL setup)
 *   beforeExisting()  called before writing to a table that already existed
 *                     (write-access check); may throw
 *   tempPrefix        spool file name prefix
 *
 * Returns { table, mode, created, received, inserted, updated, skipped,
 * duplicates, keys }. Errors carry a statusCode (400 bad batch, 409 conflict
 * retries exhausted / key mismatch).
 */
export async function ingestRows(tableName, rows, {
    mode = 'append', keys: requestedKeys = null, keysAreDefault = false, orderBy = null,
    onCreated = null, beforeExisting = null, tempPrefix = 'ingest', updatedBy = null,
} = {}) {
    mode = parseIngestMode(mode);
    const keyed = mode !== 'append';
    const wantKeys = requestedKeys != null && requestedKeys !== '' ? normaliseKeyList(requestedKeys) : null;
    if (wantKeys && wantKeys.length === 0) throw httpError(400, 'keys must name at least one column');
    if (orderBy != null && orderBy !== '') {
        if (!keyed) throw httpError(400, 'orderBy is only valid with mode=upsert or mode=dedupe');
        if (typeof orderBy !== 'string' || !SAFE_IDENTIFIER_RE.test(orderBy)) throw httpError(400, `Invalid orderBy column: ${JSON.stringify(orderBy)}`);
    } else {
        orderBy = null;
    }

    const filePath = path.join(process.cwd(), `${tempPrefix}_${randomUUID()}.json`);
    const result = { table: tableName, mode, created: false, received: rows.length, inserted: 0, updated: 0, skipped: 0, duplicates: 0, keys: [] };
    try {
        // Write incrementally to avoid holding a second copy of the batch.
        const fh = await fs.open(filePath, 'w');
        try {
            let i = 0;
            for (const row of rows) {
                const out = keyed && row && typeof row === 'object' && !Array.isArray(row) ? { ...row, [SEQ_COL]: i } : row;
                await fh.write(JSON.stringify(out) + '\n');
                i++;
            }
        } finally {
            await fh.close();
        }
        const escapedPath = filePath.replace(/'/g, "''");
        const sourceSql = `SELECT * FROM read_json_auto('${escapedPath}', ${READ_OPTIONS})`;

        await withConnection(async (conn) => {
            const exists = (await tableColumns(conn, tableName)).size > 0;
            let writeExisting = exists;

            if (!exists) {
                try {
                    if (keyed) {
                        const keys = wantKeys || await getTableKeys(tableName, conn);
                        if (!keys.length) {
                            throw httpError(400, `mode=${mode} needs key columns: table "${tableName}" does not exist yet, so pass ?key=<column>[,<column>...] to declare them`);
                        }
                        const incomingTypes = new Map((await rowsOf(conn, `DESCRIBE ${sourceSql}`)).map(r => [r.column_name, String(r.column_type)]));
                        await assertBatchKeys(conn, keys, incomingTypes, [...incomingTypes.keys()], sourceSql, orderBy);
                        assertKeysMatchColumns(tableName, keys, incomingTypes);
                        await conn.run(`CREATE TABLE ${q(tableName)} AS SELECT * EXCLUDE (${q(SEQ_COL)}) FROM (${dedupSql(sourceSql, keys, mode, orderBy)})`);
                        result.keys = keys;
                        const n = await rowsOf(conn, `SELECT count(*) AS n FROM ${q(tableName)}`);
                        result.inserted = Number(n[0]?.n || 0);
                        result.duplicates = rows.length - result.inserted;
                    } else {
                        await conn.run(`CREATE TABLE ${q(tableName)} AS ${sourceSql}`);
                        result.inserted = rows.length;
                    }
                    result.created = true;
                    invalidateColumnCache(tableName);
                } catch (createErr) {
                    // A concurrent write created the table first: write into it
                    // like any existing table (including the access check).
                    if (/already exists/i.test(createErr.message || '')) {
                        invalidateColumnCache(tableName);
                        writeExisting = true;
                    } else {
                        throw createErr;
                    }
                }
                if (result.created) {
                    if (onCreated) await onCreated(conn);
                    if (keyed) await setTableKeys(tableName, result.keys, updatedBy);
                }
            }

            if (!writeExisting) return;
            if (beforeExisting) await beforeExisting();

            let keys = [];
            if (keyed) {
                keys = await getTableKeys(tableName, conn);
                if (keys.length && wantKeys && !keysAreDefault && wantKeys.join(',') !== keys.join(',')) {
                    throw httpError(409, `Table "${tableName}" is keyed on (${keys.join(', ')}), not (${wantKeys.join(', ')}). Change the keys with PUT /tables/${tableName}/keys.`);
                }
                if (!keys.length && !wantKeys) {
                    throw httpError(400, `Table "${tableName}" has no key columns. Declare them with PUT /tables/${tableName}/keys or pass ?key=<column>[,<column>...]`);
                }
            }

            const { incoming, writable } = await evolveSchema(conn, tableName, sourceSql);

            if (!keyed) {
                await conn.run(`INSERT INTO ${q(tableName)} BY NAME SELECT ${writable.map(q).join(', ')} FROM (${sourceSql})`);
                result.inserted = rows.length;
                return;
            }

            const tableCols = await tableColumns(conn, tableName);
            if (!keys.length) {
                // First keyed write to a table without keys: declare them.
                assertKeysMatchColumns(tableName, wantKeys, tableCols);
                await setTableKeys(tableName, wantKeys, updatedBy);
                keys = wantKeys;
            } else {
                assertKeysMatchColumns(tableName, keys, tableCols);
            }
            result.keys = keys;
            await assertBatchKeys(conn, keys, incoming, writable, sourceSql, orderBy);

            const dedup = dedupSql(sourceSql, keys, mode, orderBy);
            const keyMatch = keys.map(k => `t.${q(k)} = CAST(s.${q(k)} AS ${tableCols.get(k)})`).join(' AND ');
            const mergeSql = buildMergeSql(tableName, mode, keys, tableCols, writable, dedup);

            await retryOnConflict(async () => {
                await conn.run('BEGIN TRANSACTION');
                try {
                    const counts = await rowsOf(conn,
                        `SELECT count(*) AS total, count(*) FILTER (WHERE EXISTS (SELECT 1 FROM ${q(tableName)} t WHERE ${keyMatch})) AS matched FROM (${dedup}) s`);
                    await conn.run(mergeSql);
                    await conn.run('COMMIT');
                    const total = Number(counts[0].total);
                    const matched = Number(counts[0].matched);
                    result.duplicates = rows.length - total;
                    result.inserted = total - matched;
                    if (mode === 'upsert') result.updated = matched; else result.skipped = matched;
                } catch (err) {
                    await conn.run('ROLLBACK').catch(() => {});
                    throw err;
                }
            }, { attempts: conflictRetries(), label: `${mode} ${tableName}` });
        });
    } catch (err) {
        if (isTransactionConflict(err)) {
            throw httpError(409, `Write conflict on table "${tableName}": concurrent writes kept winning after ${conflictRetries()} attempts. Nothing from this batch was written; retry the request.`);
        }
        throw err;
    } finally {
        await fs.unlink(filePath).catch(() => {});
    }
    return result;
}

/** One-line summary for responses and logs. */
export function describeIngest(r) {
    if (r.mode === 'append') return `Inserted ${r.inserted} records into ${r.table}`;
    const dup = r.duplicates ? `, ${r.duplicates} duplicate key(s) in the batch collapsed` : '';
    if (r.mode === 'upsert') return `Upserted ${r.received} records into ${r.table}: ${r.inserted} inserted, ${r.updated} updated${dup}`;
    return `Deduplicated ${r.received} records into ${r.table}: ${r.inserted} inserted, ${r.skipped} skipped (key exists)${dup}`;
}
