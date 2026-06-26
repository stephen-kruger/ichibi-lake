/**
 * Integration tests for the SQL/PGQ Graph Query endpoints (DuckPGQ).
 *
 * Exercises the three examples from graphs.md:
 *   1. Define a Property Graph (`users` + `follows` edge on users).
 *   2. Basic MATCH pattern projection.
 *   3. ANY SHORTEST path with a path_length column.
 */

import { API_KEY, BASE_URL } from './_env.js';

// Use unique table/graph names so reruns do not collide across test runs.
const suffix = Date.now();
const USERS_TABLE = `graph_users_${suffix}`;
const FOLLOWS_TABLE = `graph_follows_${suffix}`;
const GRAPH_NAME = `my_graph_${suffix}`;

async function post(path, body, extraHeaders = {}) {
    const res = await fetch(`${BASE_URL}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY, ...extraHeaders },
        body: JSON.stringify(body),
    });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch (_) { json = { raw: text }; }
    return { status: res.status, ok: res.ok, body: json };
}

async function del(path) {
    const res = await fetch(`${BASE_URL}${path}`, {
        method: 'DELETE',
        headers: { 'x-api-key': API_KEY },
    });
    return { status: res.status, ok: res.ok };
}

async function get(path) {
    const res = await fetch(`${BASE_URL}${path}`, {
        headers: { 'x-api-key': API_KEY },
    });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch (_) { json = { raw: text }; }
    return { status: res.status, ok: res.ok, body: json };
}

async function runTests() {
    console.log(`API_KEY: ${API_KEY}`);
    console.log(`BASE_URL: ${BASE_URL}`);
    console.log('--- Starting DuckPGQ Graph Gateway Integration Tests ---\n');

    try {
        // 0. Preflight: ensure DuckPGQ is actually loaded. If not, skip with a
        // clear message rather than producing confusing downstream failures.
        console.log('[Preflight] Checking DuckPGQ extension availability...');
        const pgqCheck = await post('/query', {
            sql: "SELECT loaded, installed FROM duckdb_extensions() WHERE extension_name = 'duckpgq'"
        });
        const row = pgqCheck.ok && Array.isArray(pgqCheck.body.rows) ? pgqCheck.body.rows[0] : null;
        const loaded = !!row && (row.loaded === true || row.loaded === 'true');
        if (!loaded) {
            if (!row) {
                console.warn('SKIP: DuckPGQ is not installed on the server (duckdb_extensions() returned no row).');
                console.warn('      The gateway attempts `FORCE INSTALL duckpgq FROM community` at startup; check the');
                console.warn('      server logs. On Linux/arm64 the community CDN may not yet publish a build that');
                console.warn('      matches the running DuckDB version.');
            } else {
                console.warn('SKIP: DuckPGQ is installed but not loaded. Extension row:', JSON.stringify(row));
            }
            return;
        }
        console.log('OK: DuckPGQ is loaded.\n');

        // 1. Seed vertex table `users`
        console.log(`[Setup] Seeding ${USERS_TABLE} (vertices) and ${FOLLOWS_TABLE} (edges)...`);
        const usersUpload = await post(`/upload/${USERS_TABLE}`, [
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
            { id: 3, name: 'Carol' },
        ]);
        if (!usersUpload.ok) throw new Error(`users upload failed: ${JSON.stringify(usersUpload.body)}`);

        const followsUpload = await post(`/upload/${FOLLOWS_TABLE}`, [
            // Alice -> Bob -> Carol
            { src_id: 1, dst_id: 2 },
            { src_id: 2, dst_id: 3 },
        ]);
        if (!followsUpload.ok) throw new Error(`follows upload failed: ${JSON.stringify(followsUpload.body)}`);
        // Allow DuckLake a brief moment to commit before DuckPGQ binds to them.
        await new Promise((r) => setTimeout(r, 500));
        console.log('OK: Seed data inserted.\n');

        // 2. Example from graphs.md section 2: define a Property Graph.
        console.log('[Test] POST /graphs  — define property graph');
        const createRes = await post('/graphs', {
            name: GRAPH_NAME,
            vertexTables: [USERS_TABLE],
            edgeTables: [
                {
                    name: FOLLOWS_TABLE,
                    source: USERS_TABLE,
                    destination: USERS_TABLE,
                    sourceKey: 'src_id',
                    sourceRef: 'id',
                    destinationKey: 'dst_id',
                    destinationRef: 'id',
                },
            ],
        });
        if (!createRes.ok) throw new Error(`FAIL: graph creation returned ${createRes.status}: ${JSON.stringify(createRes.body)}`);
        console.log(`OK: Property graph ${GRAPH_NAME} created. SQL: ${createRes.body.sql}\n`);

        // 3. GET /graphs — listing
        console.log('[Test] GET /graphs  — list includes our graph');
        const listRes = await get('/graphs');
        if (!listRes.ok) throw new Error(`FAIL: graph list returned ${listRes.status}: ${JSON.stringify(listRes.body)}`);
        const names = (listRes.body.graphs || []).map((g) => (g.name || '').toLowerCase());
        if (!names.includes(GRAPH_NAME.toLowerCase())) {
            throw new Error(`FAIL: new graph ${GRAPH_NAME} missing from listing. Got: ${JSON.stringify(names)}`);
        }
        console.log('OK: Graph appears in list.\n');

        // 4. Example from graphs.md section 3: basic MATCH query.
        console.log('[Test] POST /graphs/:name/query  — basic MATCH');
        const matchRes = await post(`/graphs/${GRAPH_NAME}/query`, {
            match: `(a:${USERS_TABLE})-[f:${FOLLOWS_TABLE}]->(b:${USERS_TABLE})`,
            columns: 'a.name AS follower, b.name AS followed',
        });
        if (!matchRes.ok) throw new Error(`FAIL: MATCH query returned ${matchRes.status}: ${JSON.stringify(matchRes.body)}`);
        const edges = matchRes.body.rows || [];
        if (edges.length !== 2) {
            throw new Error(`FAIL: expected 2 edges (Alice->Bob, Bob->Carol), got ${edges.length}: ${JSON.stringify(edges)}`);
        }
        const pairs = new Set(edges.map((r) => `${r.follower}->${r.followed}`));
        if (!pairs.has('Alice->Bob') || !pairs.has('Bob->Carol')) {
            throw new Error(`FAIL: unexpected edge set: ${JSON.stringify([...pairs])}`);
        }
        console.log(`OK: MATCH returned ${edges.length} edges including Alice->Bob and Bob->Carol.\n`);

        // 5. Example from graphs.md section 4: shortest path.
        console.log('[Test] POST /graphs/:name/query  — ANY SHORTEST');
        const shortestRes = await post(`/graphs/${GRAPH_NAME}/query`, {
            match: `p = ANY SHORTEST (a:${USERS_TABLE})-[f:${FOLLOWS_TABLE}]->+ (b:${USERS_TABLE})`,
            where: "a.name = 'Alice'",
            columns: 'path_length(p) AS distance',
        });
        if (!shortestRes.ok) throw new Error(`FAIL: SHORTEST query returned ${shortestRes.status}: ${JSON.stringify(shortestRes.body)}`);
        const rows = shortestRes.body.rows || [];
        if (rows.length === 0) throw new Error('FAIL: SHORTEST returned zero rows');
        const distances = rows.map((r) => Number(r.distance)).sort((a, b) => a - b);
        // From Alice: distance 1 to Bob, distance 2 to Carol.
        if (!distances.includes(1) || !distances.includes(2)) {
            throw new Error(`FAIL: expected distances to include 1 and 2, got ${JSON.stringify(distances)}`);
        }
        console.log(`OK: SHORTEST paths from Alice have distances ${JSON.stringify(distances)}.\n`);

        // 6. Validation: ensure malformed bodies are rejected.
        console.log('[Test] POST /graphs  — rejects invalid name');
        const badCreate = await post('/graphs', { name: 'bad name!', vertexTables: ['x'], edgeTables: [] });
        if (badCreate.status !== 400) {
            throw new Error(`FAIL: expected 400 on invalid graph name, got ${badCreate.status}`);
        }
        console.log('OK: invalid graph name rejected with 400.\n');

        console.log('[Test] POST /graphs/:name/query  — requires match+columns or graphTable');
        const badQuery = await post(`/graphs/${GRAPH_NAME}/query`, { match: '(a)-[]->(b)' });
        if (badQuery.status !== 400) {
            throw new Error(`FAIL: expected 400 on incomplete query body, got ${badQuery.status}`);
        }
        console.log('OK: incomplete query body rejected with 400.\n');

        // 7. Raw graphTable mode still works.
        console.log('[Test] POST /graphs/:name/query  — raw graphTable mode');
        const rawRes = await post(`/graphs/${GRAPH_NAME}/query`, {
            graphTable: `MATCH (a:${USERS_TABLE})-[f:${FOLLOWS_TABLE}]->(b:${USERS_TABLE}) COLUMNS (a.name AS follower, b.name AS followed)`,
        });
        if (!rawRes.ok) throw new Error(`FAIL: raw MATCH returned ${rawRes.status}: ${JSON.stringify(rawRes.body)}`);
        if ((rawRes.body.rows || []).length !== 2) {
            throw new Error(`FAIL: raw MATCH expected 2 rows, got ${(rawRes.body.rows || []).length}`);
        }
        console.log('OK: raw graphTable mode produced the expected rows.\n');

        // 8. Cleanup: drop graph.
        console.log(`[Teardown] DELETE /graphs/${GRAPH_NAME}`);
        const dropRes = await del(`/graphs/${GRAPH_NAME}`);
        if (!dropRes.ok) throw new Error(`FAIL: drop returned ${dropRes.status}`);
        console.log('OK: graph dropped.\n');

        console.log('--- ALL GRAPH INTEGRATION TESTS PASSED ---');
    } catch (err) {
        console.error('\n!!! GRAPH TEST SUITE FAILED !!!');
        console.error(err.message);
        process.exit(1);
    }
}

runTests();
