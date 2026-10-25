/**
 * Unit tests for src/sql-tables.js: the table-extraction walker behind
 * table-scoped SQL for non-superusers. Statements are parsed by a local
 * in-memory DuckDB with json_serialize_sql, exactly as the gateway does on its
 * primary instance, so these tests also pin the AST shapes of the DuckDB
 * version in package.json.
 */
import assert from 'node:assert/strict';
import { DuckDBInstance } from '@duckdb/node-api';
import { extractTableReferences, analyzeUserSql } from '../src/sql-tables.js';

let passed = 0, failed = 0;
async function test(name, fn) {
    try { await fn(); passed++; console.log(`  ✓ ${name}`); }
    catch (err) { failed++; console.error(`  ✗ ${name}\n    ${err.message}`); }
}

const instance = await DuckDBInstance.create(':memory:');
const conn = await instance.connect();

async function parse(sql) {
    const reader = await conn.runAndReadAll('SELECT json_serialize_sql(?::VARCHAR) AS ast', [sql]);
    return JSON.parse(reader.getRows()[0][0]);
}

// Sorted, lower-cased table names the policy lets through.
async function tablesOf(sql) {
    return analyzeUserSql(await parse(sql)).map(t => t.toLowerCase()).sort();
}

async function expectTables(sql, expected) {
    assert.deepEqual(await tablesOf(sql), [...expected].sort(), sql);
}

async function expectRejected(sql, status, re) {
    let error = null;
    try { analyzeUserSql(await parse(sql)); } catch (err) { error = err; }
    assert.ok(error, `expected rejection: ${sql}`);
    assert.equal(error.statusCode, status, `${sql}: ${error.message}`);
    if (re) assert.match(error.message, re, sql);
}

async function run() {
    console.log('\n▸ sql-tables: table extraction');

    await test('single table, FROM-first and no FROM', async () => {
        await expectTables('SELECT a FROM events', ['events']);
        await expectTables('FROM events', ['events']);
        await expectTables('SELECT 1 AS one', []);
        await expectTables('VALUES (1), (2)', []);
    });

    await test('joins of every kind', async () => {
        await expectTables('SELECT * FROM a JOIN b ON a.id = b.id LEFT JOIN c USING (id)', ['a', 'b', 'c']);
        await expectTables('SELECT * FROM a, b', ['a', 'b']);
        await expectTables('SELECT * FROM a NATURAL JOIN b', ['a', 'b']);
        await expectTables('SELECT * FROM a CROSS JOIN b', ['a', 'b']);
        await expectTables('SELECT * FROM a POSITIONAL JOIN b', ['a', 'b']);
        await expectTables('SELECT * FROM a ASOF JOIN b USING (ts)', ['a', 'b']);
        await expectTables('SELECT * FROM a SEMI JOIN b USING (id)', ['a', 'b']);
        await expectTables('SELECT * FROM a ANTI JOIN b USING (id)', ['a', 'b']);
        await expectTables('SELECT * FROM a JOIN b ON a.id = (SELECT max(id) FROM c)', ['a', 'b', 'c']);
    });

    await test('nested subqueries in every clause', async () => {
        await expectTables('SELECT * FROM (SELECT * FROM (SELECT * FROM deep) d1) d2', ['deep']);
        await expectTables(
            'SELECT * FROM a WHERE x IN (SELECT x FROM b WHERE y IN (SELECT y FROM c WHERE z IN (SELECT z FROM d)))',
            ['a', 'b', 'c', 'd']);
        await expectTables('SELECT (SELECT max(v) FROM s) FROM t ORDER BY (SELECT 1 FROM o) LIMIT (SELECT 1 FROM l)',
            ['s', 't', 'o', 'l']);
        await expectTables('SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u)', ['t', 'u']);
        await expectTables('SELECT * FROM t WHERE a = ANY (SELECT a FROM u)', ['t', 'u']);
        await expectTables('SELECT * FROM t WHERE a > ALL (SELECT a FROM u)', ['t', 'u']);
        await expectTables('SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > (SELECT count(*) FROM h)', ['t', 'h']);
        await expectTables('SELECT * FROM t QUALIFY row_number() OVER () = (SELECT 1 FROM q)', ['t', 'q']);
        await expectTables('SELECT * FROM t, LATERAL (SELECT * FROM u WHERE u.a = t.a)', ['t', 'u']);
        await expectTables('SELECT (FROM s)', ['s']);
        await expectTables('SELECT * FROM (VALUES ((SELECT 1 FROM v))) x', ['v']);
        await expectTables('SELECT sum(a) OVER w FROM t WINDOW w AS (ORDER BY (SELECT max(a) FROM w1))', ['t', 'w1']);
        await expectTables('SELECT count(*) FILTER (WHERE a > (SELECT 1 FROM f)) FROM t', ['t', 'f']);
        await expectTables('SELECT list_transform([1], x -> x + (SELECT 1 FROM lam)) FROM t', ['t', 'lam']);
        await expectTables('SELECT CASE WHEN a THEN (SELECT 1 FROM c1) ELSE (SELECT 2 FROM c2) END FROM t', ['t', 'c1', 'c2']);
        await expectTables('SELECT * FROM t AT (VERSION => (SELECT 1 FROM ver))', ['t', 'ver']);
    });

    await test('set operations', async () => {
        await expectTables('SELECT a FROM t UNION SELECT a FROM u', ['t', 'u']);
        await expectTables('SELECT a FROM t UNION ALL SELECT a FROM u UNION ALL SELECT a FROM v', ['t', 'u', 'v']);
        await expectTables('SELECT a FROM t INTERSECT SELECT a FROM u EXCEPT SELECT a FROM v', ['t', 'u', 'v']);
        await expectTables('SELECT a FROM t UNION BY NAME SELECT a FROM u', ['t', 'u']);
    });

    await test('CTEs are resolved, not reported as tables', async () => {
        await expectTables('WITH x AS (SELECT * FROM a) SELECT * FROM x JOIN b USING (id)', ['a', 'b']);
        await expectTables('WITH x AS (SELECT * FROM a), y AS (SELECT * FROM x) SELECT * FROM y', ['a']);
        await expectTables('WITH x AS (SELECT * FROM a) SELECT * FROM (SELECT * FROM x)', ['a']);
        await expectTables('WITH x AS (SELECT * FROM a) SELECT (SELECT max(v) FROM x)', ['a']);
        await expectTables('WITH x AS MATERIALIZED (SELECT * FROM a) SELECT * FROM x', ['a']);
        await expectTables('WITH "X" AS (SELECT * FROM a) SELECT * FROM x', ['a']);
        await expectTables('WITH RECURSIVE r AS (SELECT 1 n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r', []);
    });

    await test('a CTE shadowing a table name hides it only where DuckDB does', async () => {
        // The CTE named like a real table shadows it in the main query...
        await expectTables('WITH secret AS (SELECT * FROM pub) SELECT * FROM secret', ['pub']);
        // ...but not inside its own body: this reads the real table.
        await expectTables('WITH secret AS (SELECT * FROM secret) SELECT * FROM secret', ['secret']);
        await expectTables('WITH RECURSIVE secret AS (SELECT * FROM secret) SELECT * FROM secret', ['secret']);
        // A CTE body cannot see later CTEs either.
        await expectTables('WITH a AS (SELECT * FROM b), b AS (SELECT * FROM pub) SELECT * FROM a', ['b', 'pub']);
        // A recursive CTE's anchor reads the real table; the recursive term the CTE.
        await expectTables(
            'WITH RECURSIVE secret AS (SELECT a FROM secret UNION ALL SELECT a + 1 FROM secret WHERE a < 0) SELECT * FROM secret',
            ['secret']);
        // A CTE defined in a subquery is not visible outside it.
        await expectTables('SELECT * FROM (WITH s AS (SELECT 1 a) SELECT * FROM s), s', ['s']);
        // A CTE in one branch of a set operation does not cover the other.
        await expectTables('SELECT * FROM t UNION ALL (WITH t AS (SELECT 1 a) SELECT * FROM t)', ['t']);
        // An inner CTE shadows an outer table of the same name.
        await expectTables('SELECT * FROM (WITH pub AS (SELECT 7 a) SELECT * FROM pub)', []);
        // Qualified names never refer to CTEs.
        await expectTables('WITH secret AS (SELECT 1 a) SELECT * FROM main.secret', ['secret']);
    });

    await test('quoted identifiers, schema-qualified names and comments', async () => {
        await expectTables('SELECT * FROM "My Table"', ['my table']);
        await expectTables('SELECT * FROM "weird""name"', ['weird"name']);
        await expectTables('SELECT * FROM main.events', ['events']);
        await expectTables('SELECT * FROM ducklake.events', ['events']);
        await expectTables('SELECT * FROM ducklake.main.events', ['events']);
        await expectTables('SELECT * FROM "DuckLake"."MAIN"."Events"', ['events']);
        await expectTables('SELECT * FROM events e, events AS f', ['events']);
        await expectTables('/* FROM hidden */ SELECT * -- FROM other\n FROM events', ['events']);
        await expectTables("SELECT 'FROM secret' AS s FROM events", ['events']);
    });

    await test('other read-only shapes', async () => {
        await expectTables('DESCRIBE events', ['events']);
        await expectTables('SUMMARIZE events', ['events']);
        await expectTables('SELECT * FROM (PIVOT t ON a IN (\'x\') USING sum(b))', ['t']);
        await expectTables('SELECT * FROM t UNPIVOT (v FOR k IN (a, b))', ['t']);
        await expectTables('SELECT * FROM t TABLESAMPLE 10%', ['t']);
        await expectTables('SELECT * FROM t USING SAMPLE reservoir(5 ROWS) REPEATABLE (1)', ['t']);
        await expectTables('SELECT * FROM t AT (VERSION => 3)', ['t']);
        await expectTables('SELECT DISTINCT ON (a) a FROM t ORDER BY a DESC NULLS LAST LIMIT 10 OFFSET 2', ['t']);
        await expectTables('SELECT a FROM t GROUP BY GROUPING SETS ((a), ())', ['t']);
        await expectTables('SELECT * EXCLUDE (a), COLUMNS(\'b.*\'), #1 FROM t', ['t']);
        await expectTables(
            "SELECT x->>'a', a || b, NOT a, -a, a IS NULL, a LIKE 'x%', a::DECIMAL(10,2), TRY_CAST(a AS INT), " +
            "a[1], a BETWEEN 1 AND 2, a COLLATE nocase, coalesce(a, b), a IN (1, 2), {'k': 1}, [1, 2], " +
            "interval 1 day, DATE '2020-01-01' FROM t", ['t']);
    });

    console.log('\n▸ sql-tables: rejections');

    await test('multiple statements are rejected', async () => {
        await expectRejected('SELECT 1; SELECT 2', 400, /one SQL statement/);
        await expectRejected('SELECT * FROM a; SELECT * FROM secret', 400, /one SQL statement/);
    });

    await test('non-SELECT statements are rejected', async () => {
        for (const sql of ['INSERT INTO t VALUES (1)', 'DELETE FROM t', 'CREATE TABLE x (a INT)', 'EXPLAIN SELECT 1',
            'SET threads = 1', 'PIVOT t ON a USING sum(b)', "COPY t TO 'x.csv'", "ATTACH 'x.db'", 'CALL pragma_version()']) {
            await expectRejected(sql, 403, /only a single SELECT/);
        }
    });

    await test('parse errors are rejected', async () => {
        await expectRejected('SELEC 1', 400, /Could not parse/);
        await expectRejected('SELECT * FROM', 400, /Could not parse/);
    });

    await test('table functions are rejected', async () => {
        for (const sql of ["SELECT * FROM read_parquet('/data/x.parquet')", "SELECT * FROM read_csv('/etc/passwd')",
            "SELECT * FROM glob('/data/*')", "SELECT * FROM query('SELECT * FROM secret')", 'SELECT * FROM range(3)',
            "SELECT * FROM read_text('/etc/hosts')", 'SELECT * FROM duckdb_tables()',
            "SELECT * FROM ducklake_snapshots('ducklake')", 'SELECT * FROM unnest([1, 2])',
            'SELECT * FROM t1 JOIN LATERAL unnest(t1.l) ON true',
            "SELECT * FROM t WHERE a IN (SELECT * FROM read_json('x.json'))",
            "WITH x AS (SELECT * FROM read_parquet('x')) SELECT * FROM x"]) {
            await expectRejected(sql, 403, /Table function/);
        }
    });

    await test('other catalogs and schemas are rejected', async () => {
        for (const sql of ['SELECT * FROM memory.main.t', 'SELECT * FROM system.main.t', 'SELECT * FROM temp.main.t',
            'SELECT * FROM information_schema.tables', 'SELECT * FROM pg_catalog.pg_tables',
            'SELECT * FROM __ducklake_metadata_ducklake.public.ducklake_table', 'SELECT * FROM ducklake.other.t',
            'SELECT * FROM other.t', 'SELECT * FROM a.b.c.d', 'SELECT * FROM t WHERE a IN (SELECT a FROM memory.t)']) {
            await expectRejected(sql, 403, /outside ducklake\.main/);
        }
    });

    await test('system tables are reported as not found', async () => {
        await expectRejected('SELECT * FROM _rbac_users', 404, /not found/);
        await expectRejected('SELECT * FROM ducklake.main._table_acls', 404, /not found/);
        await expectRejected('SELECT * FROM t WHERE k IN (SELECT api_key FROM "_rbac_users")', 404, /not found/);
        await expectRejected('WITH x AS (SELECT * FROM _graph_registry) SELECT * FROM x', 404, /not found/);
    });

    await test('catalog listings are rejected', async () => {
        await expectRejected('SHOW TABLES', 403, /SHOW/);
        await expectRejected('SHOW ALL TABLES', 403, /SHOW/);
    });

    await test('unknown AST nodes fail closed', async () => {
        const base = await parse('SELECT * FROM t');
        const mutate = (fn) => { const copy = structuredClone(base); fn(copy.statements[0].node); return copy; };
        assert.throws(() => extractTableReferences(mutate(n => { n.from_table.type = 'MYSTERY_REF'; })),
            (e) => e.statusCode === 400 && /MYSTERY_REF/.test(e.message));
        assert.throws(() => extractTableReferences(mutate(n => { n.select_list[0].class = 'NEW_EXPR'; })),
            (e) => e.statusCode === 400 && /NEW_EXPR/.test(e.message));
        assert.throws(() => extractTableReferences(mutate(n => { n.type = 'CTE_NODE'; })),
            (e) => e.statusCode === 400 && /CTE_NODE/.test(e.message));
        assert.throws(() => extractTableReferences(null), (e) => e.statusCode === 400);
        assert.throws(() => extractTableReferences({ error: false, statements: [{}] }), (e) => e.statusCode === 400);
    });

    console.log(`\nsql-tables: ${passed} passed, ${failed} failed`);
    if (failed > 0) process.exit(1);
}

run().catch(err => { console.error(err); process.exit(1); });
