import { execute, executeWithParams, withConnection, VARCHAR } from './db.js';

const MIGRATION_KEY = 'migration_v2_timestamps';

export async function migrateTimestamps() {
    const metaCheck = await execute(
        `SELECT count(*) as count FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '_gateway_meta'`
    );
    if (metaCheck[0] && parseInt(metaCheck[0].count) > 0) {
        const migrated = await execute(`SELECT count(*) as count FROM _gateway_meta WHERE key = '${MIGRATION_KEY}'`);
        if (migrated[0] && parseInt(migrated[0].count) > 0) {
            console.log('[Migration] v2 (timestamps) already completed. Skipping.');
            return;
        }
    }

    console.log('[Migration] Checking tables for BIGINT created/updated columns...');

    const tables = await execute(
        `SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'`
    );

    let migrated = 0;
    for (const row of tables) {
        const tableName = row.table_name;
        const cols = await execute(
            `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = '${tableName}' AND column_name IN ('created', 'updated') AND data_type = 'BIGINT'`
        );

        if (cols.length === 0) continue;

        await withConnection(async (conn) => {
            for (const col of cols) {
                const colName = col.column_name;
                console.log(`[Migration] Converting ${tableName}.${colName} from BIGINT to TIMESTAMP...`);
                await conn.run(`ALTER TABLE "${tableName}" ALTER COLUMN "${colName}" SET DATA TYPE TIMESTAMP USING to_timestamp(CAST("${colName}" AS BIGINT))`);
            }
        });

        console.log(`[Migration] Updated ${tableName}: ${cols.map(c => c.column_name).join(', ')}`);
        migrated++;
    }

    if (migrated === 0) {
        console.log('[Migration] No BIGINT timestamp columns found. No migration needed.');
    } else {
        console.log(`[Migration] Done. Updated ${migrated} table(s).`);
    }

    await execute(`CREATE TABLE IF NOT EXISTS _gateway_meta (key VARCHAR, migrated_at TIMESTAMP)`);
    const alreadyRecorded = await execute(`SELECT count(*) as count FROM _gateway_meta WHERE key = '${MIGRATION_KEY}'`);
    if (alreadyRecorded[0] && parseInt(alreadyRecorded[0].count) === 0) {
        await execute(`INSERT INTO _gateway_meta VALUES ('${MIGRATION_KEY}', now())`);
    }
}
