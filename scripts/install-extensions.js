#!/usr/bin/env node
/**
 * Pre-install the DuckDB extensions the gateway loads at startup, using the
 * same @duckdb/node-api build as the gateway (so the extension version always
 * matches). Run at Docker image build time: see docker/Dockerfile for why
 * startup must never download extensions.
 */
import { DuckDBInstance } from '@duckdb/node-api';

const EXTENSIONS = [
    'INSTALL ducklake;',
    'INSTALL postgres;',
    'INSTALL duckpgq FROM community;',
];

const instance = await DuckDBInstance.create(':memory:');
const conn = await instance.connect();
for (const sql of EXTENSIONS) {
    await conn.run(sql);
    console.log(`[install-extensions] ${sql}`);
}
const result = await conn.runAndReadAll(
    `SELECT extension_name, extension_version, install_path FROM duckdb_extensions() WHERE installed AND extension_name IN ('ducklake', 'postgres_scanner', 'duckpgq')`
);
for (const [name, version, path] of result.getRows()) console.log(`[install-extensions] ${name} ${version} -> ${path}`);
if (result.getRows().length !== EXTENSIONS.length) {
    console.error('[install-extensions] Not every extension reports as installed.');
    process.exit(1);
}
process.exit(0);
