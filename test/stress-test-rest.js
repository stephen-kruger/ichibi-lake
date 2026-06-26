/**
 * Stress test for the RESTful Data Discovery and Access endpoints.
 * Focuses on high-concurrency read operations.
 */
const API_KEY = 'ICHIBI_LAKE_035a2116-c9d0-4603-9f12-da1fb57294d1';
const BASE_URL = process.env.BASE_URL || 'http://ichibi-lake:3000';
const CONCURRENT_REQUESTS = 100;
const TEST_TABLE = 'stress_test_table'; // Uses table created by existing stress-test.js if available

async function performRequest(id) {
    const start = Date.now();
    try {
        // Randomly choose between /tables and /tables/:tableName
        const target = Math.random() > 0.5 ? '/tables' : `/tables/${TEST_TABLE}?limit=10`;
        const res = await fetch(`${BASE_URL}${target}`, {
            headers: { 'x-api-key': API_KEY }
        });

        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        await res.json();

        return { id, duration: Date.now() - start, success: true };
    } catch (err) {
        return { id, duration: Date.now() - start, success: false, error: err.message };
    }
}

async function runStressTest() {
    console.log(`--- Starting REST Read Stress Test ---`);
    console.log(`Concurrent requests: ${CONCURRENT_REQUESTS}\n`);

    const start = Date.now();
    const tasks = [];
    for (let i = 0; i < CONCURRENT_REQUESTS; i++) {
        tasks.push(performRequest(i));
    }

    const results = await Promise.all(tasks);
    const end = Date.now();

    const successful = results.filter(r => r.success);
    const failed = results.filter(r => !r.success);
    const durations = successful.map(r => r.duration);
    const avgDuration = durations.reduce((a, b) => a + b, 0) / durations.length;
    const maxDuration = Math.max(...durations);
    const minDuration = Math.min(...durations);

    console.log(`Results:`);
    console.log(`- Total Time: ${end - start}ms`);
    console.log(`- Success: ${successful.length}`);
    console.log(`- Failed: ${failed.length}`);
    if (successful.length > 0) {
        console.log(`- Avg Latency: ${avgDuration.toFixed(2)}ms`);
        console.log(`- Min Latency: ${minDuration}ms`);
        console.log(`- Max Latency: ${maxDuration}ms`);
    }

    if (failed.length > 0) {
        console.log(`\nFailures:`);
        failed.slice(0, 5).forEach(f => console.log(`  [Req ${f.id}] ${f.error}`));
        if (failed.length > 5) console.log(`  ... and ${failed.length - 5} more`);
        process.exit(1);
    }

    console.log(`\n--- REST Stress Test Completed Successfully ---`);
}

runStressTest();
