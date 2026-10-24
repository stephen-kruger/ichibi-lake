/**
 * Regression test for DuckDB 2.0's per-session SQL/PGQ grammar opt-in.
 *
 * Since DuckDB 2.0, DuckPGQ's syntax (CREATE/DROP PROPERTY GRAPH, GRAPH_TABLE)
 * only parses on a connection that has run
 * `SET active_grammar_extensions = ['duckpgq']`. The gateway cannot predict
 * whether user SQL uses PGQ, so src/db.js re-applies the setting every time a
 * connection is handed out (enableDuckPgqGrammar). These tests exercise that
 * real helper and pin down the DuckDB behaviours the design relies on.
 *
 * Needs the duckpgq community extension (downloaded on first run); the suite
 * is skipped with a warning if it cannot be installed.
 */
import assert from 'node:assert/strict';
import { DuckDBInstance } from '@duckdb/node-api';
import { enableDuckPgqGrammar } from '../src/db.js';

let passed = 0, failed = 0;

async function test(name, fn) {
    try {
        await fn();
        passed++;
        console.log(`  ✓ ${name}`);
    } catch (err) {
        failed++;
        console.error(`  ✗ ${name}`);
        console.error(`    ${err.message}`);
    }
}

const CREATE_GRAPH = `CREATE PROPERTY GRAPH g VERTEX TABLES (p) EDGE TABLES (
    k SOURCE KEY (src) REFERENCES p (id) DESTINATION KEY (dst) REFERENCES p (id) LABEL knows)`;
const GRAPH_QUERY = `FROM GRAPH_TABLE (g MATCH (a:p)-[e:knows]->(b:p) COLUMNS (a.id AS s, b.id AS d))`;

async function rows(conn, sql) {
    const result = await conn.runAndReadAll(sql);
    return result.getRows();
}

async function run() {
    console.log('\n▸ pgq-grammar: SQL/PGQ grammar is enabled on every connection');

    const inst = await DuckDBInstance.create(':memory:', { allow_community_extensions: 'true' });
    const setup = await inst.connect();
    try {
        await setup.run('INSTALL duckpgq FROM community; LOAD duckpgq;');
    } catch (err) {
        console.warn(`  ! skipped: duckpgq extension unavailable (${err.message.split('\n')[0]})`);
        return;
    }
    await setup.run(`CREATE TABLE p(id INT PRIMARY KEY); CREATE TABLE k(src INT, dst INT);
                     INSERT INTO p VALUES (1), (2); INSERT INTO k VALUES (1, 2);`);

    await test('a fresh connection cannot parse PGQ without the grammar opt-in', async () => {
        const conn = await inst.connect();
        await conn.run('LOAD duckpgq;');
        await assert.rejects(conn.run(CREATE_GRAPH), /syntax error/i);
    });

    await test('prepending the SET into the same SQL string is NOT sufficient', async () => {
        // DuckDB parses the whole string before executing the SET, which is
        // why the gateway runs the SET as a separate statement.
        const conn = await inst.connect();
        await conn.run('LOAD duckpgq;');
        await assert.rejects(
            conn.run(`SET active_grammar_extensions = ['duckpgq']; ${CREATE_GRAPH}`),
            /syntax error/i,
        );
    });

    await test('enableDuckPgqGrammar on a fresh connection enables PGQ', async () => {
        const conn = await inst.connect();
        await enableDuckPgqGrammar(conn);
        await conn.run(CREATE_GRAPH);
        assert.deepEqual(await rows(conn, GRAPH_QUERY), [[1, 2]]);
    });

    await test('enableDuckPgqGrammar restores PGQ after the session setting is RESET', async () => {
        // Models a pooled connection whose session state was cleared between
        // checkouts: the grammar must be re-applied on hand-out.
        const conn = await inst.connect();
        await enableDuckPgqGrammar(conn);
        await conn.run('RESET active_grammar_extensions');
        await assert.rejects(conn.run(GRAPH_QUERY), /syntax error/i);
        await enableDuckPgqGrammar(conn);
        assert.deepEqual(await rows(conn, GRAPH_QUERY), [[1, 2]]);
    });

    await test('DROP PROPERTY GRAPH parses with the grammar enabled', async () => {
        const conn = await inst.connect();
        await enableDuckPgqGrammar(conn);
        await conn.run('DROP PROPERTY GRAPH g');
        await assert.rejects(conn.run(GRAPH_QUERY), /g/);
    });

    await test('plain SQL is unaffected by the grammar opt-in', async () => {
        const conn = await inst.connect();
        await enableDuckPgqGrammar(conn);
        assert.deepEqual(await rows(conn, 'SELECT count(*)::INT FROM p'), [[2]]);
    });

    await test('enableDuckPgqGrammar is a no-op when DISABLE_DUCKPGQ=1', async () => {
        const conn = await inst.connect();
        const prev = process.env.DISABLE_DUCKPGQ;
        process.env.DISABLE_DUCKPGQ = '1';
        try {
            await enableDuckPgqGrammar(conn);
        } finally {
            if (prev === undefined) delete process.env.DISABLE_DUCKPGQ; else process.env.DISABLE_DUCKPGQ = prev;
        }
        const loaded = await rows(conn, "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'duckpgq'");
        // LOAD is instance-wide, but the session grammar must not have been set.
        assert.equal(loaded.length, 1);
        await assert.rejects(conn.run(CREATE_GRAPH), /syntax error/i);
    });
}

run()
    .then(() => {
        console.log('\n─── pgq-grammar tests ───');
        console.log(`  passed: ${passed}`);
        console.log(`  failed: ${failed}`);
        if (failed > 0) process.exit(1);
    })
    .catch(err => {
        console.error('Unhandled error:', err);
        process.exit(1);
    });
