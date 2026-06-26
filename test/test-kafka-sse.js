import http from 'http';
import { API_KEY, BASE_URL } from './_env.js';

const url = new URL(BASE_URL);

function sseSubscribe(topicName, onEvent, opts = {}) {
    const { since = '1h' } = opts;
    return new Promise((resolve, reject) => {
        const qs = `?since=${encodeURIComponent(since)}`;
        const req = http.get(
            `${BASE_URL}/kafka-subscribe/${encodeURIComponent(topicName)}${qs}`,
            { headers: { 'x-api-key': API_KEY } },
            (res) => {
                if (res.statusCode !== 200) {
                    reject(new Error(`SSE subscribe failed: ${res.statusCode}`));
                    return;
                }
                let buffer = '';
                let currentEvent = null;
                let currentData = [];

                function dispatch() {
                    if (currentData.length > 0) {
                        onEvent(currentEvent || 'message', currentData.join('\n'));
                    }
                    currentEvent = null;
                    currentData = [];
                }

                res.on('data', (chunk) => {
                    buffer += chunk.toString();
                    const parts = buffer.split('\n');
                    buffer = parts.pop();
                    for (const line of parts) {
                        if (line === '') { dispatch(); continue; }
                        if (line.startsWith('event: ')) {
                            currentEvent = line.slice(7).trim();
                        } else if (line.startsWith('data: ')) {
                            currentData.push(line.slice(6));
                        } else if (line.startsWith('data:')) {
                            currentData.push(line.slice(5));
                        }
                    }
                });

                res.on('end', () => {
                    dispatch();
                    resolve();
                });

                res.on('error', reject);
            }
        );
        req.on('error', reject);
    });
}

async function uploadData(tableName, data) {
    const res = await fetch(`${BASE_URL}/upload/${tableName}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
        body: JSON.stringify(data),
    });
    if (!res.ok) throw new Error(`Upload failed: ${res.status} ${res.statusText} - ${await res.text()}`);
    return res.json();
}

function wait(ms) {
    return new Promise(r => setTimeout(r, ms));
}

function collectSSE(events, eventType, timeoutMs = 15000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${eventType} event`)), timeoutMs);
        function check() {
            const idx = events.findIndex(e => e.event === eventType);
            if (idx !== -1) {
                clearTimeout(timer);
                resolve(events[idx].data);
            } else {
                setTimeout(check, 100);
            }
        }
        check();
    });
}

function filler(n) {
    return 'x'.repeat(n);
}

async function testKafkaSSE() {
    console.log('--- Kafka SSE Integration Tests ---\n');

    const suffix = Date.now();
    const tableName = `kafka_sse_test_${suffix}`;
    const topicName = tableName;

    try {
        // === Test 1: Existing DuckDB data published on SSE subscribe ===
        console.log('[Test 1] DuckDB data dump via Kafka on SSE subscribe');

        const testRows = [
            { id: 1, name: 'Alice', role: 'admin' },
            { id: 2, name: 'Bob', role: 'user' },
            { id: 3, name: 'Charlie', role: 'moderator' },
        ];

        console.log(`  Uploading ${testRows.length} rows to table "${tableName}"...`);
        await uploadData(tableName, testRows);
        await wait(1000);

        const eventLog = [];
        const dumpPromise = collectSSE(eventLog, 'dump-complete');

        sseSubscribe(topicName, (evt, data) => {
            eventLog.push({ event: evt, data });
        }).catch(() => {});

        const dumpData = await dumpPromise;
        const dumpInfo = JSON.parse(dumpData);
        if (dumpInfo.rowsPublished !== testRows.length) {
            throw new Error(`Expected ${testRows.length} dumped rows, got ${dumpInfo.rowsPublished}`);
        }

        await wait(5000);

        const messageEvents = eventLog.filter(e => e.event === 'message');
        const values = messageEvents.map(m => {
            try { return JSON.parse(JSON.parse(m.data).value); } catch (e) { return m.data; }
        });

        for (const expected of testRows) {
            if (!values.find(r => String(r.id) === String(expected.id) && r.name === expected.name)) {
                throw new Error(`Row {id:${expected.id}, name:${expected.name}} not found in SSE stream`);
            }
        }
        console.log(`  OK: All ${testRows.length} rows received via SSE\n`);

        // === Test 2: Size-based batch flush (cumulative bytes > KAFKA_MAX_BATCH_BYTES) ===
        console.log('[Test 2] Size-based batch flush for many medium-sized rows');

        const table2 = `kafka_sse_size_${suffix}`;
        const topic2 = table2;

        // 50 rows * ~25KB each ≈ 1.25 MB total, well above the default
        // KAFKA_MAX_BATCH_BYTES of 950000 — exercises the byte-aware flush.
        const mediumRows = [];
        for (let i = 0; i < 50; i++) {
            mediumRows.push({ id: i, payload: filler(25_000) });
        }

        console.log(`  Uploading ${mediumRows.length} rows (~25KB each) to "${table2}"...`);
        await uploadData(table2, mediumRows);
        await wait(1000);

        const eventLog2 = [];
        const dumpPromise2 = collectSSE(eventLog2, 'dump-complete', 30000);

        sseSubscribe(topic2, (evt, data) => {
            eventLog2.push({ event: evt, data });
        }).catch(() => {});

        const dumpInfo2 = JSON.parse(await dumpPromise2);
        if (dumpInfo2.rowsPublished !== mediumRows.length) {
            throw new Error(`Expected ${mediumRows.length} rowsPublished, got ${dumpInfo2.rowsPublished}`);
        }
        if (dumpInfo2.rowsSkipped !== undefined && dumpInfo2.rowsSkipped !== 0) {
            throw new Error(`Expected 0 rowsSkipped, got ${dumpInfo2.rowsSkipped}`);
        }
        console.log(`  OK: dump-complete reports rowsPublished=${dumpInfo2.rowsPublished}, rowsSkipped=${dumpInfo2.rowsSkipped ?? 0}\n`);

        // === Test 3: Large row payload passes through without issue ===
        console.log('[Test 3] Large row passes through SSE dump without issue');

        const table3 = `kafka_sse_oversize_${suffix}`;
        const topic3 = table3;

        const allRows = [
            { id: 1, payload: 'small' },
            { id: 2, payload: 'small' },
            { id: 3, payload: 'small' },
            { id: 999, payload: filler(1_100_000) }, // large payload is fine — SSE path has no per-message limit
        ];

        console.log(`  Uploading ${allRows.length} rows (including 1 ~1.1MB row) to "${table3}"...`);
        await uploadData(table3, allRows);
        await wait(1000);

        const eventLog3 = [];
        const dumpPromise3 = collectSSE(eventLog3, 'dump-complete', 30000);

        sseSubscribe(topic3, (evt, data) => {
            eventLog3.push({ event: evt, data });
        }).catch(() => {});

        const dumpInfo3 = JSON.parse(await dumpPromise3);
        if (dumpInfo3.rowsPublished !== allRows.length) {
            throw new Error(`Expected ${allRows.length} rowsPublished, got ${dumpInfo3.rowsPublished}`);
        }
        console.log(`  OK: dump-complete reports rowsPublished=${dumpInfo3.rowsPublished} (SSE path has no per-message size limit)\n`);

        console.log('--- ALL KAFKA SSE TESTS PASSED ---\n');
        process.exit(0);

    } catch (err) {
        console.error('\n!!! TEST FAILED !!!');
        console.error(err.message);
        process.exit(1);
    }
}

testKafkaSSE();