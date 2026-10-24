// Several gateway instances can share one DuckLake catalog. DuckLake uses
// optimistic concurrency: when two instances write the same table at the same
// moment, one commit fails with a "Transaction conflict". Idempotent writes
// (check-then-insert bootstraps, CREATE TABLE IF NOT EXISTS, migrations) can
// simply be run again.

/**
 * A commit that lost a race with another instance's write to the same rows
 * (DuckLake: "Transaction conflict ..."). For a claim this means another
 * instance got there first.
 */
export function isTransactionConflict(err) {
    return /Transaction conflict|Failed to commit DuckLake transaction/i.test(String(err?.message || err));
}

/**
 * Run `fn`, retrying with jittered backoff when it loses a commit race (see
 * isTransactionConflict). Only for writes that are safe to repeat. Other
 * errors, and the last conflict, are rethrown.
 */
export async function retryOnConflict(fn, { attempts = 5, label } = {}) {
    for (let i = 1; ; i++) {
        try { return await fn(); } catch (err) {
            if (i >= attempts || !isTransactionConflict(err)) throw err;
            if (label) console.warn(`[${label}] DuckLake transaction conflict (attempt ${i}/${attempts}); retrying.`);
            await new Promise(r => setTimeout(r, 100 * i + Math.floor(Math.random() * 100 * i)));
        }
    }
}
