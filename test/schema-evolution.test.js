/**
 * Unit tests for src/schema-evolution.js.
 *
 * Pure in-process tests — no gateway, no DuckLake required. Verifies the
 * conflict-resolution rules the Kafka consumer relies on, especially the
 * STRUCT-vs-primitive 'skip' path that keeps ingestion alive when DuckLake
 * cannot evolve a column type.
 */

import assert from 'node:assert/strict';
import {
    parseStructType,
    unionStructTypes,
    resolveTypeConflict,
    formatSchemaDefinition,
} from '../src/schema-evolution.js';

let passed = 0, failed = 0;

// Tests run in order; an async test is awaited before the next one starts.
let chain = Promise.resolve();
function test(name, fn) {
    chain = chain.then(async () => {
        try {
            await fn();
            passed++;
            console.log(`  \u2713 ${name}`);
        } catch (err) {
            failed++;
            console.error(`  \u2717 ${name}`);
            console.error(`    ${err.message}`);
        }
    });
}
const section = (title) => { chain = chain.then(() => console.log(title)); };

section('\n\u25b8 schema-evolution: parseStructType');

test('parses a flat STRUCT into a field map', () => {
    const fields = parseStructType('STRUCT(a INTEGER, b VARCHAR)');
    assert.equal(fields.get('a'), 'INTEGER');
    assert.equal(fields.get('b'), 'VARCHAR');
});

test('parses nested STRUCTs without splitting on inner commas', () => {
    const fields = parseStructType('STRUCT(a INTEGER, b STRUCT(x INTEGER, y DOUBLE))');
    assert.equal(fields.get('a'), 'INTEGER');
    assert.equal(fields.get('b'), 'STRUCT(x INTEGER, y DOUBLE)');
});

test('returns null when the input is not a STRUCT', () => {
    assert.equal(parseStructType('VARCHAR'), null);
    assert.equal(parseStructType('INTEGER[]'), null);
});

section('\n\u25b8 schema-evolution: unionStructTypes');

test('adds missing fields from the new struct without dropping existing ones', () => {
    const merged = unionStructTypes(
        'STRUCT(a INTEGER)',
        'STRUCT(a INTEGER, b VARCHAR)'
    );
    const fields = parseStructType(merged);
    assert.equal(fields.get('a'), 'INTEGER');
    assert.equal(fields.get('b'), 'VARCHAR');
});

test('recursively merges nested STRUCT fields', () => {
    const merged = unionStructTypes(
        'STRUCT(p STRUCT(x INTEGER))',
        'STRUCT(p STRUCT(x INTEGER, y DOUBLE))'
    );
    const fields = parseStructType(merged);
    // Field names are always quoted in built types (same type to DuckDB).
    assert.equal(fields.get('p'), 'STRUCT("x" INTEGER, "y" DOUBLE)');
});

test('field names with quotes, commas, parentheses and spaces round-trip', () => {
    // As DuckDB prints them: names quoted, embedded quotes doubled.
    const fields = parseStructType('STRUCT("x""y" BIGINT, "a,b" VARCHAR, "temp (C)" DOUBLE, "sp ace" STRUCT("in,ner" INTEGER))');
    assert.deepEqual([...fields.keys()], ['x"y', 'a,b', 'temp (C)', 'sp ace']);
    assert.equal(fields.get('temp (C)'), 'DOUBLE');
    assert.equal(fields.get('sp ace'), 'STRUCT("in,ner" INTEGER)');
});

test('union of structs with awkward field names quotes every name', () => {
    const merged = unionStructTypes('STRUCT("x""y" BIGINT, "order" BIGINT, "1st" BIGINT, "a,b" BIGINT)', 'STRUCT("z" BIGINT)');
    assert.equal(merged, 'STRUCT("x""y" BIGINT, "order" BIGINT, "1st" BIGINT, "a,b" BIGINT, "z" BIGINT)');
    assert.deepEqual([...parseStructType(merged).keys()], ['x"y', 'order', '1st', 'a,b', 'z']);
});

test('built STRUCT types with awkward names are valid DuckDB types', async () => {
    const { DuckDBInstance } = await import('@duckdb/node-api');
    const conn = await (await DuckDBInstance.create(':memory:')).connect();
    const merged = unionStructTypes('STRUCT("x""y" BIGINT, "order" BIGINT, "1st" BIGINT, "a,b" BIGINT, "temp (C)" BIGINT)', 'STRUCT("new key" BIGINT)');
    const r = await conn.runAndReadAll(`SELECT typeof(NULL::${merged})`);
    assert.equal(parseStructType(r.getRows()[0][0]).size, 6);
    conn.closeSync();
});

section('\n\u25b8 schema-evolution: resolveTypeConflict');

test('returns insert when the two types are identical', () => {
    const r = resolveTypeConflict('INTEGER', 'INTEGER');
    assert.equal(r.action, 'insert');
});

test('returns insert when both types are primitives (DuckDB casts itself)', () => {
    const r = resolveTypeConflict('INTEGER', 'BIGINT');
    assert.equal(r.action, 'insert');
});

test('returns skip when existing is STRUCT and new is a primitive (DuckLake cannot evolve)', () => {
    const r = resolveTypeConflict('STRUCT(a INTEGER, b VARCHAR)', 'VARCHAR');
    assert.equal(r.action, 'skip');
    assert.ok(typeof r.reason === 'string' && r.reason.length > 0);
});

test('returns skip when existing is a primitive and new is STRUCT (symmetric case)', () => {
    const r = resolveTypeConflict('VARCHAR', 'STRUCT(a INTEGER)');
    assert.equal(r.action, 'skip');
});

test('returns insert when the new STRUCT is a subset of the existing STRUCT', () => {
    const r = resolveTypeConflict(
        'STRUCT(a INTEGER, b VARCHAR)',
        'STRUCT(a INTEGER)'
    );
    assert.equal(r.action, 'insert');
});

test('returns widen when the new STRUCT has extra fields', () => {
    const r = resolveTypeConflict(
        'STRUCT(a INTEGER)',
        'STRUCT(a INTEGER, b VARCHAR)'
    );
    assert.equal(r.action, 'widen');
    const fields = parseStructType(r.newType);
    assert.equal(fields.get('a'), 'INTEGER');
    assert.equal(fields.get('b'), 'VARCHAR');
});

test('does not emit widen action with VARCHAR fallback for STRUCT-vs-primitive', () => {
    // Regression: previously returned { action: 'widen', newType: 'VARCHAR' },
    // which DuckLake rejects with "Type evolution is not supported".
    const r = resolveTypeConflict('STRUCT(v BIGINT, upload_type VARCHAR)', 'VARCHAR');
    assert.notEqual(r.action, 'widen');
    assert.equal(r.action, 'skip');
});

section('\n\u25b8 schema-evolution: formatSchemaDefinition');

test('renders a CREATE TABLE-like definition from column metadata', () => {
    const out = formatSchemaDefinition('t', [
        { column_name: 'id',   data_type: 'INTEGER' },
        { column_name: 'name', data_type: 'VARCHAR' },
    ]);
    assert.ok(out.includes('CREATE TABLE "t"'));
    assert.ok(out.includes('"id" INTEGER'));
    assert.ok(out.includes('"name" VARCHAR'));
});

chain.then(() => {
    console.log('\n\u2500\u2500\u2500 schema-evolution unit tests \u2500\u2500\u2500');
    console.log(`  passed: ${passed}`);
    console.log(`  failed: ${failed}`);
    process.exit(failed === 0 ? 0 : 1);
});
