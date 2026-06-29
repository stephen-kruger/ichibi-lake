/**
 * Unit tests for src/graphs.js.
 *
 * Pure in-process tests — no gateway, no DuckLake required. Verifies the
 * identifier validation and CREATE PROPERTY GRAPH DDL generation that both the
 * HTTP layer and the durable registry depend on.
 */

import assert from 'node:assert/strict';
import { isSafeIdentifier, buildCreatePropertyGraphSql } from '../src/graphs.js';

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

console.log('\n\u25b8 graphs: isSafeIdentifier');

test('accepts simple identifiers', () => {
    assert.equal(isSafeIdentifier('users'), true);
    assert.equal(isSafeIdentifier('_my_graph'), true);
    assert.equal(isSafeIdentifier('Edge123'), true);
});

test('rejects names with spaces, punctuation or leading digits', () => {
    assert.equal(isSafeIdentifier('bad name'), false);
    assert.equal(isSafeIdentifier('drop;table'), false);
    assert.equal(isSafeIdentifier('1users'), false);
    assert.equal(isSafeIdentifier(''), false);
    assert.equal(isSafeIdentifier(null), false);
    assert.equal(isSafeIdentifier(42), false);
});

console.log('\n\u25b8 graphs: buildCreatePropertyGraphSql validation');

test('throws on missing or invalid name', () => {
    assert.throws(() => buildCreatePropertyGraphSql({ name: 'bad name', vertexTables: ['u'], edgeTables: [] }), /name/);
    assert.throws(() => buildCreatePropertyGraphSql({ vertexTables: ['u'], edgeTables: [] }), /name/);
});

test('throws when vertexTables is empty or not an array', () => {
    assert.throws(() => buildCreatePropertyGraphSql({ name: 'g', vertexTables: [], edgeTables: [] }), /vertexTables/);
    assert.throws(() => buildCreatePropertyGraphSql({ name: 'g', vertexTables: 'u', edgeTables: [] }), /vertexTables/);
});

test('throws when edgeTables is not an array', () => {
    assert.throws(() => buildCreatePropertyGraphSql({ name: 'g', vertexTables: ['u'], edgeTables: 'e' }), /edgeTables/);
});

test('rejects an unsafe vertex table name', () => {
    assert.throws(() => buildCreatePropertyGraphSql({ name: 'g', vertexTables: ['bad name'], edgeTables: [] }), /vertex table/);
});

console.log('\n\u25b8 graphs: buildCreatePropertyGraphSql DDL output');

test('builds a vertex-only graph from string vertex tables', () => {
    const sql = buildCreatePropertyGraphSql({ name: 'g', vertexTables: ['users'], edgeTables: [] });
    assert.equal(sql, 'CREATE PROPERTY GRAPH g VERTEX TABLES (users)');
});

test('omits KEY when a vertex has no label (DuckPGQ requires LABEL with KEY)', () => {
    const sql = buildCreatePropertyGraphSql({
        name: 'g',
        vertexTables: [{ name: 'users', key: 'id' }],
        edgeTables: [],
    });
    assert.equal(sql, 'CREATE PROPERTY GRAPH g VERTEX TABLES (users)');
});

test('emits KEY (...) LABEL ... when both are supplied', () => {
    const sql = buildCreatePropertyGraphSql({
        name: 'g',
        vertexTables: [{ name: 'users', key: 'id', label: 'Person' }],
        edgeTables: [],
    });
    assert.equal(sql, 'CREATE PROPERTY GRAPH g VERTEX TABLES (users KEY (id) LABEL Person)');
});

test('builds an edge table with SOURCE/DESTINATION key references', () => {
    const sql = buildCreatePropertyGraphSql({
        name: 'social',
        vertexTables: ['users'],
        edgeTables: [{
            name: 'follows',
            source: 'users',
            destination: 'users',
            sourceKey: 'follower_id',
            sourceRef: 'id',
            destinationKey: 'followed_id',
            destinationRef: 'id',
        }],
    });
    assert.equal(
        sql,
        'CREATE PROPERTY GRAPH social VERTEX TABLES (users) EDGE TABLES ' +
        '(follows SOURCE KEY (follower_id) REFERENCES users (id) ' +
        'DESTINATION KEY (followed_id) REFERENCES users (id))'
    );
});

test('builds an edge table without explicit keys (auto-discovery)', () => {
    const sql = buildCreatePropertyGraphSql({
        name: 'g',
        vertexTables: ['a', 'b'],
        edgeTables: [{ name: 'e', source: 'a', destination: 'b' }],
    });
    assert.equal(
        sql,
        'CREATE PROPERTY GRAPH g VERTEX TABLES (a, b) EDGE TABLES (e SOURCE a DESTINATION b)'
    );
});

test('rejects an unsafe edge source identifier', () => {
    assert.throws(() => buildCreatePropertyGraphSql({
        name: 'g',
        vertexTables: ['a'],
        edgeTables: [{ name: 'e', source: 'bad source', destination: 'a' }],
    }), /edge source/);
});

console.log('\n\u2500\u2500\u2500 graphs unit tests \u2500\u2500\u2500');
console.log(`  passed: ${passed}`);
console.log(`  failed: ${failed}`);
process.exit(failed === 0 ? 0 : 1);
