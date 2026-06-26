import http from 'http';
import { API_KEY, BASE_URL } from './_env.js';

// --- Helpers ---

function sseSubscribe(topicName, onEvent, opts = {}) {
    return new Promise((resolve, reject) => {
        const { since = null, sinceColumn = null, timeoutMs = 60000 } = opts;
        const params = [];
        if (since) params.push(`since=${encodeURIComponent(since)}`);
        if (sinceColumn) params.push(`sinceColumn=${encodeURIComponent(sinceColumn)}`);
        const qs = params.length ? '?' + params.join('&') : '';
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
        req.on('socket', (socket) => {
            socket.setTimeout(timeoutMs);
            socket.on('timeout', () => {
                req.destroy();
                reject(new Error('SSE connection timed out'));
            });
        });
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

function collectSSE(events, eventType, timeoutMs = 30000) {
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

// --- Test ---

async function runTest() {
    console.log('--- Kafka Since-Filtered Dump Test ---\n');
    const suffix = Date.now();
    const now = Date.now();

    // Create events in three distinct time windows relative to "now":
    //   Window A: 2 hours ago  (-120m) — 5 events
    //   Window B: 30 min ago    (-30m)  — 5 events
    //   Window C: 1 min ago     (-1m)   — 5 events
    // Total: 15 events.
    const WINDOWS = [
        { label: '2h ago', offsetMs: -120 * 60000, count: 5 },
        { label: '30m ago', offsetMs: -30 * 60000, count: 5 },
        { label: '1m ago',  offsetMs: -1 * 60000,  count: 5 },
    ];

    function buildRows(offsetMs, count, startId, colName) {
        const rows = [];
        for (let i = 0; i < count; i++) {
            rows.push({
                id: startId + i,
                event_name: `event_${startId + i}`,
                [colName]: Math.floor((now + offsetMs) / 1000),
                sequence: startId + i,
            });
        }
        return rows;
    }

    // ========================================================================
    // Scenario 1: No since — all events dumped
    // ========================================================================
    const table1 = `no_since_${suffix}`;
    const topic1 = table1;
    console.log('[Scenario 1] No ?since — expect ALL 15 events');
    console.log(`  Table: ${table1}`);

    let allRows = [];
    let id = 1;
    for (const w of WINDOWS) {
        const rows = buildRows(w.offsetMs, w.count, id, 'updated');
        allRows = allRows.concat(rows);
        id += w.count;
    }

    console.log(`  Uploading ${allRows.length} rows across 3 time windows...`);
    await uploadData(table1, allRows);
    await wait(1000);

    const events1 = [];
    const dumpPromise1 = collectSSE(events1, 'dump-complete', 60000);
    sseSubscribe(topic1, (evt, data) => {
        events1.push({ event: evt, data });
    }).catch(() => {});

    const dumpData1 = JSON.parse(await dumpPromise1);
    await wait(5000);

    const msgCount1 = events1.filter(e => e.event === 'message').length;
    const passed1 = dumpData1.rowsPublished === 15 && msgCount1 === 15;
    console.log(`  dump-complete: rowsPublished=${dumpData1.rowsPublished}, messages=${msgCount1} ${passed1 ? '✓' : '✗'}\n`);

    // ========================================================================
    // Scenario 2: since=5m — only Window C (5 events)
    // ========================================================================
    const table2 = `since_5m_${suffix}`;
    const topic2 = table2;
    console.log('[Scenario 2] ?since=5m — expect only Window C (5 events)');
    console.log(`  Table: ${table2}`);

    allRows = [];
    id = 100;
    for (const w of WINDOWS) {
        const rows = buildRows(w.offsetMs, w.count, id, 'updated');
        allRows = allRows.concat(rows);
        id += w.count;
    }

    console.log(`  Uploading ${allRows.length} rows...`);
    await uploadData(table2, allRows);
    await wait(1000);

    const events2 = [];
    const dumpPromise2 = collectSSE(events2, 'dump-complete', 60000);
    sseSubscribe(topic2, (evt, data) => {
        events2.push({ event: evt, data });
    }, { since: '5m' }).catch(() => {});

    const dumpData2 = JSON.parse(await dumpPromise2);
    await wait(5000);

    const msgCount2 = events2.filter(e => e.event === 'message').length;
    const passed2 = dumpData2.rowsPublished === 5 && msgCount2 === 5;
    console.log(`  dump-complete: rowsPublished=${dumpData2.rowsPublished}, messages=${msgCount2} ${passed2 ? '✓' : '✗'}\n`);

    // ========================================================================
    // Scenario 3: since=1h — Windows B + C (10 events)
    // ========================================================================
    const table3 = `since_1h_${suffix}`;
    const topic3 = table3;
    console.log('[Scenario 3] ?since=1h — expect Windows B + C (10 events)');
    console.log(`  Table: ${table3}`);

    allRows = [];
    id = 200;
    for (const w of WINDOWS) {
        const rows = buildRows(w.offsetMs, w.count, id, 'updated');
        allRows = allRows.concat(rows);
        id += w.count;
    }

    console.log(`  Uploading ${allRows.length} rows...`);
    await uploadData(table3, allRows);
    await wait(1000);

    const events3 = [];
    const dumpPromise3 = collectSSE(events3, 'dump-complete', 60000);
    sseSubscribe(topic3, (evt, data) => {
        events3.push({ event: evt, data });
    }, { since: '1h' }).catch(() => {});

    const dumpData3 = JSON.parse(await dumpPromise3);
    await wait(5000);

    const msgCount3 = events3.filter(e => e.event === 'message').length;
    const passed3 = dumpData3.rowsPublished === 10 && msgCount3 === 10;
    console.log(`  dump-complete: rowsPublished=${dumpData3.rowsPublished}, messages=${msgCount3} ${passed3 ? '✓' : '✗'}\n`);

    // ========================================================================
    // Scenario 4: since=1d — all 3 windows (15 events)
    // ========================================================================
    const table4 = `since_1d_${suffix}`;
    const topic4 = table4;
    console.log('[Scenario 4] ?since=1d — expect ALL 15 events');
    console.log(`  Table: ${table4}`);

    allRows = [];
    id = 300;
    for (const w of WINDOWS) {
        const rows = buildRows(w.offsetMs, w.count, id, 'updated');
        allRows = allRows.concat(rows);
        id += w.count;
    }

    console.log(`  Uploading ${allRows.length} rows...`);
    await uploadData(table4, allRows);
    await wait(1000);

    const events4 = [];
    const dumpPromise4 = collectSSE(events4, 'dump-complete', 60000);
    sseSubscribe(topic4, (evt, data) => {
        events4.push({ event: evt, data });
    }, { since: '1d' }).catch(() => {});

    const dumpData4 = JSON.parse(await dumpPromise4);
    await wait(5000);

    const msgCount4 = events4.filter(e => e.event === 'message').length;
    const passed4 = dumpData4.rowsPublished === 15 && msgCount4 === 15;
    console.log(`  dump-complete: rowsPublished=${dumpData4.rowsPublished}, messages=${msgCount4} ${passed4 ? '✓' : '✗'}\n`);

    // ========================================================================
    // Scenario 5: since=1h with explicit sinceColumn override
    // ========================================================================
    const table5 = `since_explicit_col_${suffix}`;
    const topic5 = table5;
    console.log('[Scenario 5] ?since=1h&sinceColumn=custom_ts — expect Windows B + C (10 events)');
    console.log(`  Table: ${table5}`);

    allRows = [];
    id = 400;
    for (const w of WINDOWS) {
        const rows = buildRows(w.offsetMs, w.count, id, 'custom_ts');
        allRows = allRows.concat(rows);
        id += w.count;
    }

    console.log(`  Uploading ${allRows.length} rows with 'custom_ts' column...`);
    await uploadData(table5, allRows);
    await wait(1000);

    const events5 = [];
    const dumpPromise5 = collectSSE(events5, 'dump-complete', 60000);
    sseSubscribe(topic5, (evt, data) => {
        events5.push({ event: evt, data });
    }, { since: '1h', sinceColumn: 'custom_ts' }).catch(() => {});

    const dumpData5 = JSON.parse(await dumpPromise5);
    await wait(5000);

    const msgCount5 = events5.filter(e => e.event === 'message').length;
    const passed5 = dumpData5.rowsPublished === 10 && msgCount5 === 10;
    console.log(`  dump-complete: rowsPublished=${dumpData5.rowsPublished}, messages=${msgCount5} ${passed5 ? '✓' : '✗'}\n`);

    // ========================================================================
    // Summary
    // ========================================================================
    console.log('--- Summary ---');
    console.log(`Scenario 1 (no since):          ${msgCount1}/15 received ${passed1 ? 'PASS' : 'FAIL'}`);
    console.log(`Scenario 2 (since=5m):          ${msgCount2}/5 received  ${passed2 ? 'PASS' : 'FAIL'}`);
    console.log(`Scenario 3 (since=1h):          ${msgCount3}/10 received ${passed3 ? 'PASS' : 'FAIL'}`);
    console.log(`Scenario 4 (since=1d):          ${msgCount4}/15 received ${passed4 ? 'PASS' : 'FAIL'}`);
    console.log(`Scenario 5 (since=1h, explicit): ${msgCount5}/10 received ${passed5 ? 'PASS' : 'FAIL'}`);

    const allPassed = passed1 && passed2 && passed3 && passed4 && passed5;
    if (allPassed) {
        console.log('\n--- ALL SINCE TESTS PASSED ---');
        process.exit(0);
    } else {
        console.log('\n--- SOME TESTS FAILED ---');
        process.exit(1);
    }
}

runTest().catch((err) => {
    console.error('\n!!! TEST FAILED !!!');
    console.error(err.message);
    console.error(err.stack);
    process.exit(1);
});
