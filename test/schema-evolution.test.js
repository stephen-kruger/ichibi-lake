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

function test(name, fn) {
    try {
        fn();
        passed++;
        console.log(`  \u2713 ${name}`);
    } catch (err) {
        failed++;
        console.error(`  \u2717 ${name}`);
        console.error(`    ${err.message}`);
    }
}

console.log('\n\u25b8 schema-evolution: parseStructType');

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

console.log('\n\u25b8 schema-evolution: unionStructTypes');

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
    assert.equal(fields.get('p'), 'STRUCT(x INTEGER, y DOUBLE)');
});

console.log('\n\u25b8 schema-evolution: resolveTypeConflict');

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

console.log('\n\u25b8 schema-evolution: formatSchemaDefinition');

test('renders a CREATE TABLE-like definition from column metadata', () => {
    const out = formatSchemaDefinition('t', [
        { column_name: 'id',   data_type: 'INTEGER' },
        { column_name: 'name', data_type: 'VARCHAR' },
    ]);
    assert.ok(out.includes('CREATE TABLE "t"'));
    assert.ok(out.includes('"id" INTEGER'));
    assert.ok(out.includes('"name" VARCHAR'));
});

console.log('\n\u2500\u2500\u2500 schema-evolution unit tests \u2500\u2500\u2500');
console.log(`  passed: ${passed}`);
console.log(`  failed: ${failed}`);
process.exit(failed === 0 ? 0 : 1);
