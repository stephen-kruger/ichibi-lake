import { API_KEY, BASE_URL } from './_env.js';

async function testBlobs() {
    console.log('--- Testing BLOB Functionality ---');
    const testId = `blob_test_${Date.now()}`;
    const tableName = `blob_storage_test_${Date.now()}`;
    const blobColumn = 'avatar';

    // 1. Upload binary data (with tableName)
    const originalBuffer = Buffer.from('DEADBEEF-BINARY-DATA-PACKET-1234567890', 'utf8');
    console.log(`[Upload] Sending ${originalBuffer.length} bytes to ${tableName}.${blobColumn} for ID ${testId}...`);

    const uploadRes = await fetch(`${BASE_URL}/tables/${tableName}/blobs/${testId}/${blobColumn}?idColumn=id`, {
        method: 'POST',
        headers: {
            'x-api-key': API_KEY,
            'Content-Type': 'application/octet-stream'
        },
        body: originalBuffer
    });

    if (!uploadRes.ok) throw new Error(`Upload failed: ${uploadRes.status} ${await uploadRes.text()}`);
    console.log('OK: Upload with tableName successful.');

    // 2. Download binary data (with tableName)
    console.log(`[Download] Fetching BLOB from ${BASE_URL}/tables/${tableName}/blobs/${testId}/${blobColumn}...`);
    const downloadRes = await fetch(`${BASE_URL}/tables/${tableName}/blobs/${testId}/${blobColumn}?idColumn=id`, {
        headers: { 'x-api-key': API_KEY }
    });

    if (!downloadRes.ok) throw new Error(`Download failed: ${downloadRes.status} ${await downloadRes.text()}`);
    const downloadedBuffer = Buffer.from(await downloadRes.arrayBuffer());

    console.log(`OK: Downloaded ${downloadedBuffer.length} bytes.`);
    if (originalBuffer.equals(downloadedBuffer)) console.log('OK: Data integrity verified.');

    // 3. Test Optional tableName (Default to ichibi_table)
    // First, clear any existing ichibi_table to ensure fresh schema (VARCHAR id)
    console.log(`[Cleanup] Dropping existing 'ichibi_table' for clean test...`);
    await fetch(`${BASE_URL}/query`, {
        method: 'POST',
        headers: { 'x-api-key': API_KEY, 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'DROP TABLE IF EXISTS ichibi_table' })
    });

    const defaultTestId = `default_blob_${Date.now()}`;
    console.log(`[Optional Table] Testing upload to default 'ichibi_table' via /blobs/...`);
    const defaultUploadRes = await fetch(`${BASE_URL}/blobs/${defaultTestId}/${blobColumn}`, {
        method: 'POST',
        headers: {
            'x-api-key': API_KEY,
            'Content-Type': 'application/octet-stream'
        },
        body: originalBuffer
    });

    if (!defaultUploadRes.ok) throw new Error(`Default upload failed: ${defaultUploadRes.status} ${await defaultUploadRes.text()}`);
    console.log('OK: Upload to default table successful.');

    console.log(`[Optional Table] Fetching from default 'ichibi_table' via /blobs/...`);
    const defaultDownloadRes = await fetch(`${BASE_URL}/blobs/${defaultTestId}/${blobColumn}`, {
        headers: { 'x-api-key': API_KEY }
    });
    if (!defaultDownloadRes.ok) throw new Error(`Default download failed: ${defaultDownloadRes.status}`);
    const defaultDownloadedBuffer = Buffer.from(await defaultDownloadRes.arrayBuffer());
    if (originalBuffer.equals(defaultDownloadedBuffer)) console.log('OK: Default table data integrity verified.');

    // 4. Test Generated UUID (idValue omitted)
    console.log(`[Generated UUID] Testing anonymous upload via /blobs/:blobColumn...`);
    const anonUploadRes = await fetch(`${BASE_URL}/blobs/${blobColumn}`, {
        method: 'POST',
        headers: {
            'x-api-key': API_KEY,
            'Content-Type': 'application/octet-stream'
        },
        body: originalBuffer
    });

    if (!anonUploadRes.ok) throw new Error(`Anonymous upload failed: ${anonUploadRes.status} ${await anonUploadRes.text()}`);
    const anonUploadData = await anonUploadRes.json();
    const generatedId = anonUploadData.id;
    console.log(`OK: Anonymous upload successful. Generated ID: ${generatedId}`);

    console.log(`[Generated UUID] Verifying retrieval of generated ID: ${generatedId}...`);
    const anonDownloadRes = await fetch(`${BASE_URL}/blobs/${generatedId}/${blobColumn}`, {
        headers: { 'x-api-key': API_KEY }
    });
    if (!anonDownloadRes.ok) throw new Error(`Anonymous download failed: ${anonDownloadRes.status}`);
    const anonDownloadedBuffer = Buffer.from(await anonDownloadRes.arrayBuffer());
    if (originalBuffer.equals(anonDownloadedBuffer)) console.log('OK: Anonymous data integrity verified.');

    // 5. Test Arbitrary Metadata
    const metaId = `meta_test_${Date.now()}`;
    const category = 'testing';
    const importance = 'high';
    console.log(`[Metadata] Uploading with metadata ?category=${category}&importance=${importance}...`);

    const metaUploadRes = await fetch(`${BASE_URL}/blobs/${metaId}/${blobColumn}?category=${category}&importance=${importance}`, {
        method: 'POST',
        headers: {
            'x-api-key': API_KEY,
            'Content-Type': 'application/octet-stream'
        },
        body: originalBuffer
    });

    if (!metaUploadRes.ok) throw new Error(`Metadata upload failed: ${metaUploadRes.status}`);
    console.log('OK: Metadata upload successful.');

    console.log(`[Metadata] Verifying metadata in table query...`);
    const metaVerifyRes = await fetch(`${BASE_URL}/tables/ichibi_table?id=${metaId}`, {
        headers: { 'x-api-key': API_KEY }
    });
    const metaVerifyData = await metaVerifyRes.json();
    const row = metaVerifyData.rows[0];

    if (row.category === category && row.importance === importance) {
        console.log(`OK: Metadata persisted successfully: category=${row.category}, importance=${row.importance}`);
    } else {
        console.error('FAIL: Metadata mismatch!', row);
        throw new Error('Metadata mismatch');
    }

    // 6. Test Record Updates via PATCH
    console.log(`[Update] Testing atomic metadata update via PATCH for ID ${metaId}...`);
    const newStatus = 'approved';
    const newTag = 'vital';

    const patchRes = await fetch(`${BASE_URL}/records/${metaId}`, {
        method: 'PATCH',
        headers: {
            'x-api-key': API_KEY,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ status: newStatus, tag: newTag })
    });

    if (!patchRes.ok) throw new Error(`PATCH failed: ${patchRes.status} ${await patchRes.text()}`);
    console.log('OK: PATCH update successful.');

    console.log(`[Update] Verifying updated fields in table query...`);
    const updateVerifyRes = await fetch(`${BASE_URL}/tables/ichibi_table?id=${metaId}`, {
        headers: { 'x-api-key': API_KEY }
    });
    const updateData = await updateVerifyRes.json();
    const updatedRow = updateData.rows[0];

    if (updatedRow.status === newStatus && updatedRow.tag === newTag && updatedRow.category === category) {
        console.log(`OK: Fields updated correctly: status=${updatedRow.status}, tag=${updatedRow.tag}, category=${updatedRow.category}`);
    } else {
        console.error('FAIL: Update verification mismatch!', updatedRow);
        throw new Error('Update verification mismatch');
    }

    // 7. Test BLOB delete (with tableName)
    console.log(`[Delete] Clearing BLOB ${tableName}.${blobColumn} for ID ${testId}...`);
    const deleteRes = await fetch(`${BASE_URL}/tables/${tableName}/blobs/${testId}/${blobColumn}?idColumn=id`, {
        method: 'DELETE',
        headers: { 'x-api-key': API_KEY }
    });
    if (!deleteRes.ok) throw new Error(`Delete failed: ${deleteRes.status} ${await deleteRes.text()}`);
    console.log('OK: Delete returned 200.');

    console.log(`[Delete] Verifying GET now returns 404 for deleted blob...`);
    const postDeleteRes = await fetch(`${BASE_URL}/tables/${tableName}/blobs/${testId}/${blobColumn}?idColumn=id`, {
        headers: { 'x-api-key': API_KEY }
    });
    if (postDeleteRes.status !== 404) {
        throw new Error(`Expected 404 after delete, got ${postDeleteRes.status}`);
    }
    console.log('OK: GET after delete returns 404.');

    console.log(`[Delete] Verifying DELETE on missing record returns 404...`);
    const missingDeleteRes = await fetch(`${BASE_URL}/tables/${tableName}/blobs/no_such_id_xyz/${blobColumn}?idColumn=id`, {
        method: 'DELETE',
        headers: { 'x-api-key': API_KEY }
    });
    if (missingDeleteRes.status !== 404) {
        throw new Error(`Expected 404 for missing record, got ${missingDeleteRes.status}`);
    }
    console.log('OK: DELETE on missing record returns 404.');

    console.log('--- ALL BLOB & UPDATE TESTS PASSED ---');
}

testBlobs().catch(err => {
    console.error('!!! BLOB TESTS FAILED !!!');
    console.error(err);
    process.exit(1);
});
