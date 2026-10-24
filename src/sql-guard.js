/**
 * Guard for user-supplied SQL (POST /query, the GraphQL `sql` resolver and
 * POST /graphs/:graphName/query).
 *
 * These endpoints are superuser-only (RBAC roles and table ACLs cannot be
 * applied to arbitrary SQL) and run on the isolated user-query instance, which
 * has no access to the console catalog, has external file/network access
 * disabled, and has dropped its DuckLake secret (see src/db.js). This guard is
 * the remaining layer:
 *
 *  1. Read-only: rejects DDL/DML/admin keywords.
 *  2. No introspection that reveals connection details: DuckDB exposes the
 *     DuckLake metadata connection string, including the Postgres password,
 *     through duckdb_databases(), pragma_database_list, duckdb_settings() and
 *     current_setting(); those functions are rejected outright.
 */

// Write/DDL/admin keywords, matched as whole words after comments, string
// literals and quoted identifiers are removed (so SELECT 'DROP me' is legal).
const FORBIDDEN_KEYWORD_RE = /\b(CREATE|DROP|INSERT|UPDATE|DELETE|ALTER|ATTACH|DETACH|COPY|TRUNCATE|MERGE|REPLACE|GRANT|REVOKE|VACUUM|CHECKPOINT|INSTALL|LOAD|EXPORT|IMPORT|CALL|PRAGMA|SET|RESET|USE)\b/i;

// Introspection functions that disclose secrets or connection strings. Matched
// on a scrub that KEEPS the contents of double-quoted identifiers, so a quoted
// call such as "duckdb_databases"() is still caught.
const FORBIDDEN_FUNCTION_RE = /\b(duckdb_databases|duckdb_settings|duckdb_secrets|which_secret|current_setting|database_list|pragma_[a-z0-9_]+)\b/i;

function stripComments(sql) {
    return String(sql)
        .replace(/--[^\n]*/g, ' ')
        .replace(/\/\*[\s\S]*?\*\//g, ' ');
}

/** Remove comments, string literals and quoted identifiers (keyword scan). */
export function stripSqlLiterals(sql) {
    return stripComments(sql)
        .replace(/'(?:''|[^'])*'/g, "''")
        .replace(/"(?:""|[^"])*"/g, '""');
}

/** Remove comments and string literals but unquote identifiers (function scan). */
function stripForFunctionScan(sql) {
    return stripComments(sql)
        .replace(/'(?:''|[^'])*'/g, "''")
        .replace(/"((?:""|[^"])*)"/g, (_, inner) => ` ${inner.replace(/""/g, '"')} `);
}

function forbidden(message) {
    return Object.assign(new Error(message), { statusCode: 403 });
}

/** Throws a 403-tagged Error unless `sql` is a read-only, non-introspective query. */
export function assertReadOnlySql(sql) {
    const keyword = stripSqlLiterals(sql || '').match(FORBIDDEN_KEYWORD_RE);
    if (keyword) {
        throw forbidden(
            `Read-only enforcement: statement "${keyword[0].toUpperCase()}" is not allowed on user-supplied SQL. ` +
            `Use the REST API or the admin console to change data or schemas.`);
    }
    const fn = stripForFunctionScan(sql || '').match(FORBIDDEN_FUNCTION_RE);
    if (fn) {
        throw forbidden(`Function "${fn[0].toLowerCase()}" is not allowed on user-supplied SQL.`);
    }
}
