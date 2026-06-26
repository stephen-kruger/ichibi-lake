// testing web service - unit tests
import { API_KEY, BASE_URL } from './_env.js';

async function test() {
    console.log('Testing upload...');
    const uploadRes = await fetch(`${BASE_URL}/upload`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
        body: JSON.stringify([{ id: 1, name: "duck" }, { id: 2, name: "lake" }])
    });

    if (!uploadRes.ok) throw new Error(`Upload failed: ${uploadRes.status} ${uploadRes.statusText}`);
    console.log('Upload Response:', await uploadRes.json());

    console.log('Testing kafka sink...');
    // Testing wrapped payload with X-Kafka-Topic header
    const kafkaSinkRes = await fetch(`${BASE_URL}/kafka-sink`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-Kafka-Topic': 'kafka_events_table',
            'x-api-key': API_KEY
        },
        body: JSON.stringify([
            { key: "msg1", value: { event: "login", user: "duck" } },
            { key: "msg2", value: { event: "logout", user: "lake" } }
        ])
    });

    if (!kafkaSinkRes.ok) throw new Error(`Kafka sink failed: ${kafkaSinkRes.status} ${kafkaSinkRes.statusText}`);
    console.log('Kafka Sink Response:', await kafkaSinkRes.json());

    console.log('Testing query...');
    const queryRes = await fetch(`${BASE_URL}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
        body: JSON.stringify({ sql: "SELECT * FROM ichibi_table" })
    });

    if (!queryRes.ok) throw new Error(`Query failed: ${queryRes.status} ${queryRes.statusText}`);
    console.log('Query Response (ichibi_table):', await queryRes.json());

    const queryKafkaRes = await fetch(`${BASE_URL}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
        body: JSON.stringify({ sql: "SELECT * FROM kafka_events_table" })
    });

    if (!queryKafkaRes.ok) throw new Error(`Query kafka table failed: ${queryKafkaRes.status} ${queryKafkaRes.statusText}`);
    console.log('Query Response (kafka_events_table):', await queryKafkaRes.json());
}

// --- /kafka-sink topic resolution tests ---

const SINK_DEFAULT_TOPIC = 'ichibi_table';

async function postSink(pathAndQuery, headers, body) {
    return fetch(`${BASE_URL}${pathAndQuery}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY, ...headers },
        body: JSON.stringify(body)
    });
}

async function countRows(table, marker) {
    const res = await fetch(`${BASE_URL}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
        body: JSON.stringify({ sql: `SELECT count(*) AS n FROM "${table}" WHERE _kafka_key = '${marker}'` })
    });
    if (!res.ok) return -1;
    const j = await res.json();
    return parseInt(j.rows?.[0]?.n ?? 0);
}

async function tableExists(table) {
    const res = await fetch(`${BASE_URL}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
        body: JSON.stringify({ sql: `SELECT count(*) AS n FROM information_schema.tables WHERE table_name = '${table}'` })
    });
    if (!res.ok) return false;
    const j = await res.json();
    return parseInt(j.rows?.[0]?.n ?? 0) > 0;
}

async function testKafkaSinkTopicResolution() {
    console.log('\n--- /kafka-sink topic resolution tests ---');
    const suffix = Date.now();
    const msg = (k) => [{ key: k, value: { event: 'probe', tag: k } }];

    // 1. Header only
    const tHeader = `ks_header_${suffix}`;
    let r = await postSink('/kafka-sink', { 'X-Kafka-Topic': tHeader }, msg('h1'));
    if (!r.ok) throw new Error(`header-only POST failed: ${r.status}`);
    if (await countRows(tHeader, 'h1') !== 1) throw new Error('FAIL: header-only row missing');
    console.log('OK: header-only routes to header topic');

    // 2. Query param only
    const tQuery = `ks_query_${suffix}`;
    r = await postSink(`/kafka-sink?topic=${tQuery}`, {}, msg('q1'));
    if (!r.ok) throw new Error(`query-only POST failed: ${r.status}`);
    if (await countRows(tQuery, 'q1') !== 1) throw new Error('FAIL: query-only row missing');
    console.log('OK: query-only routes to query topic');

    // 3. Both, agreeing
    const tAgree = `ks_agree_${suffix}`;
    r = await postSink(`/kafka-sink?topic=${tAgree}`, { 'X-Kafka-Topic': tAgree }, msg('a1'));
    if (!r.ok) throw new Error(`agreeing POST failed: ${r.status}`);
    if (await countRows(tAgree, 'a1') !== 1) throw new Error('FAIL: agreeing row missing');
    console.log('OK: both-agreeing routes to that topic');

    // 4. Both, disagreeing -> header wins
    const tHeaderWin = `ks_hwin_${suffix}`;
    const tQueryLose = `ks_qlose_${suffix}`;
    r = await postSink(`/kafka-sink?topic=${tQueryLose}`, { 'X-Kafka-Topic': tHeaderWin }, msg('d1'));
    if (!r.ok) throw new Error(`disagreeing POST failed: ${r.status}`);
    if (await countRows(tHeaderWin, 'd1') !== 1) throw new Error('FAIL: header-wins row missing');
    if (await tableExists(tQueryLose)) throw new Error('FAIL: query topic table was created when header was set');
    console.log('OK: header beats query when they disagree');

    // 5. Neither -> documented default; success message names that default
    const defaultMarker = `def_${suffix}`;
    r = await postSink('/kafka-sink', {}, msg(defaultMarker));
    if (!r.ok) throw new Error(`default POST failed: ${r.status}`);
    const defJson = await r.json();
    if (!defJson.message || !defJson.message.includes(SINK_DEFAULT_TOPIC)) {
        throw new Error(`FAIL: success message does not name default topic "${SINK_DEFAULT_TOPIC}": ${defJson.message}`);
    }
    if (await countRows(SINK_DEFAULT_TOPIC, defaultMarker) !== 1) throw new Error('FAIL: default-topic row missing');
    console.log(`OK: neither -> "${SINK_DEFAULT_TOPIC}" and message confirms it`);

    // 6. Whitespace-only header + valid query -> query is used
    const tWs = `ks_ws_${suffix}`;
    r = await postSink(`/kafka-sink?topic=${tWs}`, { 'X-Kafka-Topic': '   ' }, msg('w1'));
    if (!r.ok) throw new Error(`whitespace-header POST failed: ${r.status}`);
    if (await countRows(tWs, 'w1') !== 1) throw new Error('FAIL: whitespace-header fallthrough row missing');
    console.log('OK: whitespace-only header treated as not provided');

    // 7. Invalid identifier -> 400, no table created, no row written
    const badNames = ['1starts_with_digit', 'has-dash', 'drop;--', 'a'.repeat(64)];
    for (const bad of badNames) {
        r = await postSink(`/kafka-sink?topic=${encodeURIComponent(bad)}`, {}, msg('bad'));
        if (r.status !== 400) throw new Error(`FAIL: expected 400 for invalid name "${bad}", got ${r.status}`);
        if (await tableExists(bad)) throw new Error(`FAIL: invalid name "${bad}" created a table`);
    }
    console.log('OK: invalid identifiers rejected with 400 and no DDL executed');

    // 8. Auto-creation: new valid topic that doesn't yet exist
    const tNew = `ks_new_${suffix}`;
    if (await tableExists(tNew)) throw new Error('Precondition failure: new topic table already exists');
    r = await postSink('/kafka-sink', { 'X-Kafka-Topic': tNew }, msg('n1'));
    if (!r.ok) throw new Error(`auto-create POST failed: ${r.status}`);
    if (!(await tableExists(tNew))) throw new Error('FAIL: new topic table was not auto-created');
    if (await countRows(tNew, 'n1') !== 1) throw new Error('FAIL: auto-created table missing row');
    console.log('OK: previously-unknown valid topic is auto-created');

    // 9. Repeated ?topic= keys -> first non-empty wins
    const tRep1 = `ks_rep1_${suffix}`;
    const tRep2 = `ks_rep2_${suffix}`;
    r = await postSink(`/kafka-sink?topic=&topic=${tRep1}&topic=${tRep2}`, {}, msg('r1'));
    if (!r.ok) throw new Error(`repeated-topic POST failed: ${r.status}`);
    if (await countRows(tRep1, 'r1') !== 1) throw new Error('FAIL: first non-empty repeated ?topic= did not win');
    if (await tableExists(tRep2)) throw new Error('FAIL: later repeated ?topic= value was used instead of first');
    console.log('OK: repeated ?topic= picks first non-empty value');

    console.log('--- /kafka-sink topic resolution tests PASSED ---');
}

test()
    .then(() => testKafkaSinkTopicResolution())
    .catch(err => {
        console.error('Test failed:', err);
        process.exit(1);
    });
