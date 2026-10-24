/**
 * Unit tests for src/sql-guard.js: the read-only + no-introspection guard
 * applied to every user-supplied SQL endpoint.
 */
import assert from 'node:assert/strict';
import { assertReadOnlySql } from '../src/sql-guard.js';

let passed = 0, failed = 0;
async function test(name, fn) {
    try { await fn(); passed++; console.log(`  ✓ ${name}`); }
    catch (err) { failed++; console.error(`  ✗ ${name}\n    ${err.message}`); }
}

const allowed = (sql) => assert.doesNotThrow(() => assertReadOnlySql(sql), sql);
const blocked = (sql, re = /not allowed/) => {
    assert.throws(() => assertReadOnlySql(sql), (err) => err.statusCode === 403 && re.test(err.message), sql);
};

async function run() {
    console.log('\n▸ sql-guard: read-only enforcement');
    await test('plain reads are allowed', () => {
        allowed('SELECT * FROM events WHERE id = 1');
        allowed('WITH t AS (SELECT 1 AS n) SELECT n FROM t');
        allowed("SELECT count(*) FROM events WHERE note = 'please DROP me'");
        allowed('SELECT "update", "set" FROM t');
        allowed('FROM GRAPH_TABLE (g MATCH (a:p)-[e:k]->(b:p) COLUMNS (a.id))');
    });
    await test('write, DDL and admin statements are rejected', () => {
        for (const sql of ['DROP TABLE t', 'insert into t values (1)', 'UPDATE t SET a = 1', 'DELETE FROM t',
            'ALTER TABLE t ADD COLUMN x INT', "ATTACH 'x.db'", "COPY t TO 'x.csv'", 'PRAGMA version', 'SET threads = 1',
            'RESET threads', 'CALL ducklake_expire_snapshots()', 'INSTALL httpfs', 'LOAD httpfs', 'USE memory',
            'SELECT 1; DROP TABLE t']) blocked(sql, /Read-only/);
    });
    await test('keywords hidden in comments do not bypass, and do not block', () => {
        blocked('SELECT 1 /* harmless */; DELETE FROM t');
        allowed('SELECT 1 -- DROP TABLE t');
    });

    console.log('\n▸ sql-guard: introspection functions that leak connection details');
    await test('connection-string and secret introspection is rejected', () => {
        for (const sql of [
            'SELECT * FROM duckdb_databases()',
            'SELECT path FROM pragma_database_list',
            "SELECT * FROM duckdb_settings() WHERE name = 'allowed_paths'",
            "SELECT current_setting('allowed_directories')",
            'SELECT * FROM duckdb_secrets()',
            "SELECT * FROM which_secret('s3://x', 's3')",
            'SELECT * FROM main.duckdb_databases()',
        ]) blocked(sql, /Function/);
    });
    await test('quoting or casing the function name does not bypass', () => {
        blocked('SELECT * FROM "duckdb_databases"()', /Function/);
        blocked('SELECT * FROM "pragma_database_list"', /Function/);
        blocked('SELECT CURRENT_SETTING(\'x\')', /Function/);
    });
    await test('the same words inside string literals are allowed', () => {
        allowed("SELECT 'duckdb_databases() is blocked' AS note");
        allowed("SELECT * FROM t WHERE name = 'current_setting'");
    });
}

run().then(() => {
    console.log('\n─── sql-guard unit tests ───');
    console.log(`  passed: ${passed}`);
    console.log(`  failed: ${failed}`);
    if (failed > 0) process.exit(1);
}).catch((err) => { console.error(err); process.exit(1); });
