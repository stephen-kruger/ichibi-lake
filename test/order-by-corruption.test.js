/**
 * Regression test for the DuckLake inline-statistics ORDER BY corruption.
 *
 * The corruption is a DuckDB engine bug (see src/db.js): truncated min/max
 * string statistics on DuckLake inline rows mislead the statistics_propagation
 * and compressed_materialization optimizers under blocking operators, scrambling
 * VARCHAR columns. The fix disables both optimizers instance-wide.
 *
 * The bug only manifests on the specific DuckDB/DuckLake version the gateway
 * pins (and against the live Postgres-backed inline encoding), so a from-scratch
 * reproduction is not portable across DuckDB versions. Instead, the primary
 * guard below asserts -- through the *real* production helper in src/db.js --
 * that the corruption-triggering optimizers are actually disabled. If the fix is
 * removed or weakened, this test fails. A second block exercises the genuine
 * DuckLake inline path under the ORDER BY suite to guard the symptom directly on
 * affected versions.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import { DISABLED_OPTIMIZERS, applyOptimizerWorkaround } from '../src/db.js';

let passed = 0, failed = 0;

async function test(name, fn) {
    try {
        await fn();
        passed++;
        console.log(`  \u2713 ${name}`);
    } catch (err) {
        failed++;
        console.error(`  \u2717 ${name}`);
        console.error(`    ${err.message}`);
    }
}

async function scalar(conn, sql) {
    const result = await conn.run(sql);
    const rowSet = await result.getRows();
    return rowSet[0][0];
}

async function attachDuckLake(dir) {
    const meta = path.join(dir, 'meta.ducklake');
    const data = path.join(dir, 'data');
    await fs.mkdir(data, { recursive: true });
    const instance = await DuckDBInstance.create(':memory:', { allow_unsigned_extensions: 'true' });
    const conn = await instance.connect();
    await conn.run('INSTALL ducklake; LOAD ducklake;');
    // DATA_INLINING_ROW_LIMIT keeps the small rowset in the metadata catalog
    // (the inline path that triggers the bug) rather than flushing to Parquet.
    await conn.run(`ATTACH 'ducklake:${meta}' AS ducklake (DATA_PATH '${data}', DATA_INLINING_ROW_LIMIT 1000);`);
    await conn.run('USE ducklake;');
    return { instance, conn };
}

async function run() {
    // ---- Primary guard: the fix is applied via the production code path -----
    // This is the assertion that fails if we did not have the fix in src/db.js.
    console.log('\n\u25b8 order-by-corruption: optimizer workaround (production code path)');

    await test('DISABLED_OPTIMIZERS lists both corruption-triggering optimizers', () => {
        assert.ok(DISABLED_OPTIMIZERS.includes('statistics_propagation'),
            `DISABLED_OPTIMIZERS must include statistics_propagation (got '${DISABLED_OPTIMIZERS}')`);
        assert.ok(DISABLED_OPTIMIZERS.includes('compressed_materialization'),
            `DISABLED_OPTIMIZERS must include compressed_materialization (got '${DISABLED_OPTIMIZERS}')`);
    });

    await test('applyOptimizerWorkaround disables both optimizers on a real connection', async () => {
        const instance = await DuckDBInstance.create(':memory:');
        const conn = await instance.connect();
        await applyOptimizerWorkaround(conn, 'test');
        const setting = String(await scalar(conn, `SELECT current_setting('disabled_optimizers')`));
        assert.ok(setting.includes('statistics_propagation'),
            `disabled_optimizers must include statistics_propagation (got '${setting}')`);
        assert.ok(setting.includes('compressed_materialization'),
            `disabled_optimizers must include compressed_materialization (got '${setting}')`);
    });

    // ---- Symptom guard: exercise the genuine DuckLake inline ORDER BY path ---
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'orderby-'));
    let dl;
    try {
        dl = await attachDuckLake(dir);
        await runInlineOrderBySuite(dl.conn);
    } finally {
        await fs.rm(dir, { recursive: true, force: true });
    }

    console.log('\n\u2500\u2500\u2500 order-by-corruption tests \u2500\u2500\u2500');
    console.log(`  passed: ${passed}`);
    console.log(`  failed: ${failed}`);
    process.exit(failed === 0 ? 0 : 1);
}

async function runInlineOrderBySuite(conn) {
    await conn.run(`
        CREATE TABLE sardal_dmuh_events (
            event_id                VARCHAR,
            event_type              VARCHAR,
            person_id               VARCHAR,
            identification_confidence VARCHAR,
            created                 VARCHAR,
            source_system           VARCHAR,
            destination_system      VARCHAR,
            payload                 VARCHAR
        )
    `);

    const rows = [
        ['uuid-0001-aaaa-4a7a-9f3a', 'REGISTRATION', '102020058',
         '0.7985730419225642', '{"$date":"2024-01-15T10:30:00Z"}',
         'system_a', 'system_b', '{"key":"val1"}'],
        ['uuid-0002-bbbb-5b8b-0g4b', 'UPDATE',       '204060079',
         '0.9123456789012345', '{"$date":"2024-02-20T14:45:00Z"}',
         'system_b', 'system_c', '{"key":"val2"}'],
        ['uuid-0003-cccc-6c9c-1h5c', 'DELETE',       '308050061',
         '0.6543210987654321', '{"$date":"2024-03-25T08:15:00Z"}',
         'system_a', 'system_d', '{"key":"val3"}'],
        ['uuid-0004-dddd-7d0d-2i6d', 'LOGIN',        '401020032',
         '0.5000000000000000', '{"$date":"2024-04-10T16:00:00Z"}',
         'system_c', 'system_a', '{"key":"val4"}'],
        ['uuid-0005-eeee-8e1e-3j7e', 'LOGOUT',       '509030077',
         '0.1234567890123456', '{"$date":"2024-05-05T22:30:00Z"}',
         'system_d', 'system_b', '{"key":"val5"}'],
    ];

    const insertSql = `INSERT INTO sardal_dmuh_events VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`;
    for (const r of rows) {
        await conn.run(insertSql, r, []);
    }

    // Apply the production workaround on this DuckLake instance. Without the
    // fix in src/db.js this is a no-op and the ORDER BY queries below scramble
    // person_id / identification_confidence on affected DuckDB versions.
    await applyOptimizerWorkaround(conn, 'test');

    async function queryAndCheck(sql, description) {
        await test(description, async () => {
            const result = await conn.run(sql);
            const names = result.columnNames();
            const rowSet = await result.getRows();
            const fetched = [];
            for (const row of rowSet) {
                const obj = {};
                names.forEach((col, i) => { obj[col] = row[i]; });
                fetched.push(obj);
            }
            assert.ok(fetched.length > 0, 'expected at least one row');

            for (const row of fetched) {
                const eventId = row.event_id;
                const pid = row.person_id;
                const conf = row.identification_confidence;
                const expected = rows.find(r => r[0] === eventId);
                if (expected) {
                    assert.equal(pid, expected[2],
                        `person_id mismatch for ${eventId}: got '${pid}', expected '${expected[2]}'`);
                    assert.equal(conf, expected[3],
                        `identification_confidence mismatch for ${eventId}: got '${conf}', expected '${expected[3]}'`);
                }
                const corrupted = pid === '9' && conf === 'A';
                assert.ok(!corrupted,
                    `ORDER BY corruption detected for ${eventId}: person_id='${pid}', identification_confidence='${conf}'`);
            }
        });
    }

    console.log('\n\u25b8 order-by-corruption: DuckLake inline ORDER BY integrity');
    await queryAndCheck(
        `SELECT event_id, person_id, identification_confidence FROM sardal_dmuh_events`,
        'SELECT without ORDER BY returns correct person_id and identification_confidence'
    );
    await queryAndCheck(
        `SELECT event_id, person_id, identification_confidence FROM sardal_dmuh_events ORDER BY event_id`,
        'ORDER BY event_id does not corrupt person_id / identification_confidence'
    );
    await queryAndCheck(
        `SELECT event_id, person_id, identification_confidence FROM sardal_dmuh_events ORDER BY created`,
        'ORDER BY created does not corrupt person_id / identification_confidence'
    );
    await queryAndCheck(
        `SELECT event_id, person_id, identification_confidence FROM sardal_dmuh_events ORDER BY person_id`,
        'ORDER BY person_id does not corrupt person_id / identification_confidence'
    );
    await queryAndCheck(
        `SELECT event_id, person_id, identification_confidence FROM sardal_dmuh_events ORDER BY event_type, event_id`,
        'ORDER BY event_type, event_id does not corrupt person_id / identification_confidence'
    );
    await queryAndCheck(
        `SELECT event_id, person_id, identification_confidence FROM sardal_dmuh_events WHERE event_type = 'REGISTRATION' ORDER BY event_id`,
        'WHERE + ORDER BY event_id does not corrupt person_id / identification_confidence'
    );
    await queryAndCheck(
        `SELECT event_id, person_id, identification_confidence FROM sardal_dmuh_events ORDER BY event_id, event_type, person_id, identification_confidence, created, source_system, destination_system, payload`,
        'ORDER BY all columns does not corrupt person_id / identification_confidence'
    );
}

run().catch(err => {
    console.error('Unhandled error:', err);
    process.exit(1);
});
