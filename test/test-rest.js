/**
 * Integration tests for the RESTful Data Discovery and Access endpoints.
 */
import { API_KEY, ALT_KEY as ALT_API_KEY, BASE_URL } from './_env.js';
const TEST_TABLE = 'rest_test_' + Date.now();
// When docker/.env defines only one API key, ALT_API_KEY collapses to API_KEY
// and the cross-tenant assertions become meaningless.  In that single-key mode
// we still exercise the catalog/filter/sort/pagination paths but skip the
// isolation assertions.
const MULTI_KEY = ALT_API_KEY && ALT_API_KEY !== API_KEY;

async function runTests() {
    console.log('--- Starting REST Gateway Integration Tests ---\n');
    console.log(`MULTI_KEY mode: ${MULTI_KEY ? 'on (two distinct keys)' : 'off (single key in docker/.env)'}\n`);

    try {
        // 0. Setup: Create a test table with data
        console.log(`[Setup] Creating test table: ${TEST_TABLE}...`);

        // Rows for the primary key
        const adminSetupData = [
            { id: 1, name: 'Apple', category: 'fruit', price: 1.5 },
            { id: 2, name: 'Banana', category: 'fruit', price: 0.5 },
            { id: 3, name: 'Carrot', category: 'vegetable', price: 0.8 },
        ];

        // Row for the alt key (only inserted in multi-key mode)
        const userSetupData = [
            { id: 4, name: 'Date', category: 'fruit', price: 2.0 },
        ];

        const upload1 = await fetch(`${BASE_URL}/upload/${TEST_TABLE}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
            body: JSON.stringify(adminSetupData)
        });
        if (!upload1.ok) throw new Error(`Admin setup failed: ${upload1.status}`);

        if (MULTI_KEY) {
            // Grant ALT_API_KEY write access so it can upload rows.
            const aclRes = await fetch(`${BASE_URL}/tables/${TEST_TABLE}/acl`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
                body: JSON.stringify({ ownerKeys: [API_KEY, ALT_API_KEY], readerKeys: [] })
            });
            if (!aclRes.ok) throw new Error(`ACL grant failed: ${aclRes.status} ${await aclRes.text()}`);

            const upload2 = await fetch(`${BASE_URL}/upload/${TEST_TABLE}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'x-api-key': ALT_API_KEY },
                body: JSON.stringify(userSetupData)
            });
            if (!upload2.ok) throw new Error(`User setup failed: ${upload2.status}`);
        }

        console.log(`OK: Test table populated${MULTI_KEY ? ' for both keys.' : '.'}`);

        // Add a small delay to ensure DuckDB metadata is synced
        await new Promise(resolve => setTimeout(resolve, 1000));
        console.log('');

        // 1. Test /tables
        console.log('[Test] GET /tables');
        const tablesRes = await fetch(`${BASE_URL}/tables`, {
            headers: { 'x-api-key': API_KEY }
        });
        const tablesData = await tablesRes.json();
        console.log(`[Diagnostic] Tables found: ${JSON.stringify(tablesData.tables)}`);
        if (!tablesData.tables.includes(TEST_TABLE)) throw new Error('FAIL: Table not found in /tables list');
        console.log('OK: Found test table in catalog.\n');

        // 2. Test /tables/:tableName/schema
        console.log(`[Test] GET /tables/${TEST_TABLE}/schema`);
        const schemaRes = await fetch(`${BASE_URL}/tables/${TEST_TABLE}/schema`, {
            headers: { 'x-api-key': API_KEY }
        });
        const schemaData = await schemaRes.json();
        if (!schemaRes.ok) {
            console.error('Schema request failed:', schemaData);
            throw new Error(`FAIL: Schema API returned ${schemaRes.status}`);
        }
        if (!schemaData.schema) {
            console.error('Unexpected schema response structure:', schemaData);
            throw new Error('FAIL: schema property missing in response');
        }
        const columnNames = schemaData.schema.map(c => c.column_name || c.COLUMN_NAME || c.name);
        console.log('Found columns in schema:', columnNames);
        if (!columnNames.includes('category')) {
            throw new Error(`FAIL: Schema missing expected columns. Found: ${columnNames.join(', ')}`);
        }
        console.log('OK: Schema metadata is correct.\n');

        // 3. Test Filtering
        console.log(`[Test] GET /tables/${TEST_TABLE}?category=fruit`);
        const filterRes = await fetch(`${BASE_URL}/tables/${TEST_TABLE}?category=fruit`, {
            headers: { 'x-api-key': API_KEY }
        });
        const filterData = await filterRes.json();
        const expectedFruits = MULTI_KEY ? 3 : 2;
        if (filterData.rowCount !== expectedFruits) {
            throw new Error(`Expected ${expectedFruits} fruits, got ${filterData.rowCount}`);
        }
        if (!filterData.rows.every(r => r.category === 'fruit')) throw new Error('FAIL: Results contain non-fruit items');
        console.log('OK: Equality filtering works.\n');

        // 4. Test Sorting
        console.log(`[Test] GET /tables/${TEST_TABLE}?sort=-price`);
        const sortRes = await fetch(`${BASE_URL}/tables/${TEST_TABLE}?sort=-price`, {
            headers: { 'x-api-key': API_KEY }
        });
        const sortData = await sortRes.json();
        if (sortData.rows[0].price < sortData.rows[1].price) throw new Error('FAIL: Rows not sorted descending by price');
        console.log('OK: Sorting works.\n');

        // 5. Test Pagination
        console.log(`[Test] GET /tables/${TEST_TABLE}?limit=1&offset=1`);
        const pageRes = await fetch(`${BASE_URL}/tables/${TEST_TABLE}?limit=1&offset=1`, {
            headers: { 'x-api-key': API_KEY }
        });
        const pageData = await pageRes.json();
        if (pageData.rows.length !== 1) throw new Error('FAIL: Pagination limit failed');
        console.log('OK: Pagination works.\n');

        // 6. Test ACL-based Access Control
        if (MULTI_KEY) {
            console.log('[Test] ACL - Access Control Isolation');
            // ALT_API_KEY was granted owner access in setup, so it can read the table.
            const altRes = await fetch(`${BASE_URL}/tables/${TEST_TABLE}`, {
                headers: { 'x-api-key': ALT_API_KEY }
            });
            if (!altRes.ok) throw new Error(`FAIL: ALT_API_KEY expected 200 (owner), got ${altRes.status}`);

            // API_KEY is the table creator and has owner access (sees all 4 rows).
            const ownerRes = await fetch(`${BASE_URL}/tables/${TEST_TABLE}`, {
                headers: { 'x-api-key': API_KEY }
            });
            const ownerData = await ownerRes.json();
            if (!ownerRes.ok) throw new Error(`FAIL: Owner expected 200, got ${ownerRes.status}`);
            if (ownerData.rows.length !== 4) throw new Error(`FAIL: Owner expected 4 rows, got ${ownerData.rows.length}`);

            // A non-existent key should get 401 (auth middleware), not 403.
            const noKeyRes = await fetch(`${BASE_URL}/tables/${TEST_TABLE}`);
            if (noKeyRes.status !== 401) throw new Error(`FAIL: Expected 401 for no key, got ${noKeyRes.status}`);
            console.log('OK: ACL-based access control verified.\n');
        } else {
            console.log('[Test] ACL - Access Control SKIPPED (single-key mode)\n');
        }

        console.log('--- ALL REST INTEGRATION TESTS PASSED ---');

    } catch (err) {
        console.error('\n!!! TEST SUITE FAILED !!!');
        console.error(err.message);
        process.exit(1);
    }
}

runTests();
