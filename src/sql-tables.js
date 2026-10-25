/**
 * Table-aware authorization for user-supplied SQL (POST /query and the GraphQL
 * `sql` resolver) for non-superusers.
 *
 * The SQL is parsed by DuckDB itself: json_serialize_sql(sql) on the primary
 * instance returns the parsed statement as JSON (see serializeSql in
 * src/db.js), so there is no second, hand-written SQL parser whose idea of the
 * query could differ from what DuckDB executes. This module walks that JSON and
 * returns every real table the query reads, so the caller can authorize each
 * one with checkReadAccess (src/acl.js).
 *
 * The walker fails closed: every node is matched against the AST shapes
 * verified on DuckDB 2.0.0 (query nodes, table references, expression classes,
 * result modifiers), and anything it does not recognise is rejected rather
 * than skipped. It is pure (no DB access) so it can be unit-tested against
 * DuckDB's real parser output (test/sql-tables.test.js).
 *
 * Rules for non-superusers:
 *  - exactly one statement, and it must be a SELECT (json_serialize_sql only
 *    serializes SELECTs; INSERT, PIVOT statements, EXPLAIN, SET... error out);
 *  - no table functions at all (read_parquet, read_csv, glob, query, range,
 *    unnest in FROM, duckdb_* ...);
 *  - tables only in ducklake.main (written t, main.t, ducklake.t or
 *    ducklake.main.t); any other catalog or schema (memory, system, temp,
 *    information_schema, pg_catalog, the DuckLake metadata catalog) is refused;
 *  - no "_"-prefixed system tables (reported as not found, like the REST API);
 *  - SHOW TABLES / SHOW ALL TABLES style statements are refused; DESCRIBE t and
 *    SUMMARIZE t are allowed because they read table t, which is authorized.
 *
 * CTE scoping follows DuckDB's binder (verified on 2.0.0): a CTE name is
 * visible in the rest of its WITH clause after its own definition, in the main
 * query of that node and in every subquery nested inside it, but NOT inside its
 * own body: `WITH secret AS (SELECT * FROM secret)` reads the real table
 * `secret`. In a recursive CTE the name is visible only in the recursive term
 * (the right side of the UNION); the anchor term reads the real table.
 */

export const MAIN_CATALOG = 'ducklake';
export const MAIN_SCHEMA = 'main';

function policyError(message, statusCode = 403) {
    return Object.assign(new Error(message), { statusCode });
}

function unsupported(what) {
    return policyError(
        `SQL construct not supported for table-scoped SQL: ${what}. ` +
        'Use a plain SELECT over your tables, or ask a superuser.', 400);
}

// Query nodes (statement bodies, subqueries, CTE bodies).
const QUERY_NODE_TYPES = new Set(['SELECT_NODE', 'SET_OPERATION_NODE', 'RECURSIVE_CTE_NODE']);

// Table references (FROM clause items).
const TABLE_REF_TYPES = new Set([
    'BASE_TABLE', 'JOIN', 'SUBQUERY', 'EMPTY', 'EXPRESSION_LIST', 'PIVOT', 'SHOW_REF', 'TABLE_FUNCTION',
]);

// Result modifiers and ORDER BY items (OrderByNode.type is the sort order).
const MODIFIER_TYPES = new Set([
    'ORDER_MODIFIER', 'LIMIT_MODIFIER', 'LIMIT_PERCENT_MODIFIER', 'DISTINCT_MODIFIER',
    'ORDER_DEFAULT', 'ASCENDING', 'DESCENDING',
]);

// Expression classes. None of them can name a table except SUBQUERY, whose
// query is walked like any other query node.
const EXPRESSION_CLASSES = new Set([
    'BETWEEN', 'CASE', 'CAST', 'COLLATE', 'COLUMN_REF', 'COMPARISON', 'CONJUNCTION', 'CONSTANT',
    'FUNCTION', 'LAMBDA', 'LAMBDA_REF', 'OPERATOR', 'PARAMETER', 'POSITIONAL_REFERENCE', 'STAR',
    'SUBQUERY', 'TYPE', 'WINDOW',
]);

const lower = (s) => String(s).toLowerCase();
const withName = (scope, name) => new Set([...scope, lower(name)]);

function tablePath(ref) {
    const path = ref.qualified_name?.path;
    if (Array.isArray(path) && path.length > 0 && path.every(p => typeof p === 'string')) return path;
    return [ref.catalog_name, ref.schema_name, ref.table_name].filter(p => typeof p === 'string' && p !== '');
}

/**
 * Walk a parsed statement (the JSON from json_serialize_sql) and collect the
 * table references and table functions it uses. CTE references are resolved
 * and left out. Throws a 400/403-tagged Error for anything that is not a
 * single SELECT made of recognised constructs.
 *
 * Returns { tables: [{ path: [...] }], tableFunctions: [name] }.
 */
export function extractTableReferences(parsed) {
    if (!parsed || typeof parsed !== 'object') throw unsupported('unparseable statement');
    if (parsed.error) {
        const msg = String(parsed.error_message || parsed.error_type || 'parse error');
        if (/only select statements/i.test(msg)) {
            throw policyError(
                'Read-only enforcement: only a single SELECT statement is allowed on user-supplied SQL. ' +
                'Use the REST API to change data or schemas.');
        }
        throw policyError(`Could not parse SQL: ${msg}`, 400);
    }
    const statements = parsed.statements;
    if (!Array.isArray(statements) || statements.length !== 1) {
        throw policyError('Exactly one SQL statement is allowed per request.', 400);
    }

    const tables = [];
    const tableFunctions = [];

    function walkQueryNode(node, scope) {
        if (!QUERY_NODE_TYPES.has(node.type)) throw unsupported(`query node ${node.type}`);
        let inner = scope;
        const cteMap = node.cte_map?.map ?? [];
        if (!Array.isArray(cteMap)) throw unsupported('CTE list');
        for (const entry of cteMap) {
            if (!entry || typeof entry.key !== 'string' || !entry.value?.query_node) throw unsupported('CTE definition');
            // The body sees the CTEs defined before it, not itself.
            walk(entry.value, inner);
            inner = withName(inner, entry.key);
        }
        for (const [key, value] of Object.entries(node)) {
            if (key === 'cte_map') continue;
            if (node.type === 'RECURSIVE_CTE_NODE' && key === 'right') {
                walk(value, withName(inner, node.cte_name));
            } else {
                walk(value, inner);
            }
        }
    }

    function walkTableRef(ref, scope) {
        switch (ref.type) {
            case 'BASE_TABLE': {
                const path = tablePath(ref);
                if (path.length === 0) throw unsupported('table reference without a name');
                if (!(path.length === 1 && scope.has(lower(path[0])))) tables.push({ path });
                break;
            }
            case 'TABLE_FUNCTION':
                tableFunctions.push(lower(ref.function?.function_name ?? '?'));
                break;
            case 'SHOW_REF':
                // DESCRIBE / SUMMARIZE carry the described query; SHOW TABLES
                // and friends have no query and list the whole catalog.
                if (!ref.query || (ref.table_name && ref.table_name !== '')) {
                    throw policyError('SHOW statements that list the catalog are not allowed; use GET /tables.');
                }
                break;
            default:
                break;
        }
        for (const value of Object.values(ref)) walk(value, scope);
    }

    function walk(value, scope) {
        if (Array.isArray(value)) {
            for (const v of value) walk(v, scope);
            return;
        }
        if (!value || typeof value !== 'object') return;

        if ('class' in value) {
            if (!EXPRESSION_CLASSES.has(value.class)) throw unsupported(`expression ${value.class}`);
            for (const v of Object.values(value)) walk(v, scope);
            return;
        }
        if (typeof value.type === 'string') {
            if (QUERY_NODE_TYPES.has(value.type) || value.type.endsWith('_NODE')) return walkQueryNode(value, scope);
            if (TABLE_REF_TYPES.has(value.type)) return walkTableRef(value, scope);
            if (MODIFIER_TYPES.has(value.type)) {
                for (const v of Object.values(value)) walk(v, scope);
                return;
            }
            throw unsupported(value.type);
        }
        // Plain containers: { node, named_param_map }, CTE info, CASE checks,
        // function arguments, sample options, pivot entries, LogicalTypes...
        for (const v of Object.values(value)) walk(v, scope);
    }

    const stmt = statements[0];
    if (!stmt?.node) throw unsupported('statement without a query');
    walk(stmt.node, new Set());
    return { tables, tableFunctions };
}

/**
 * Apply the non-superuser policy to a parsed statement and return the distinct
 * table names it reads (as written, case preserved; DuckDB resolves them
 * case-insensitively). Throws a tagged Error on any violation.
 */
export function analyzeUserSql(parsed) {
    const { tables, tableFunctions } = extractTableReferences(parsed);
    if (tableFunctions.length > 0) {
        throw policyError(
            `Table function "${tableFunctions[0]}" is not allowed in table-scoped SQL ` +
            '(table functions can read files and catalog metadata).');
    }
    const names = new Map();
    for (const { path } of tables) {
        const name = path[path.length - 1];
        const qualifiers = path.slice(0, -1).map(lower);
        const ok = qualifiers.length === 0
            || (qualifiers.length === 1 && (qualifiers[0] === MAIN_SCHEMA || qualifiers[0] === MAIN_CATALOG))
            || (qualifiers.length === 2 && qualifiers[0] === MAIN_CATALOG && qualifiers[1] === MAIN_SCHEMA);
        if (!ok) {
            throw policyError(
                `Table "${path.join('.')}" is outside ${MAIN_CATALOG}.${MAIN_SCHEMA}; ` +
                'only lake tables can be queried.');
        }
        if (name.startsWith('_')) throw policyError(`Table "${name}" not found`, 404);
        if (!names.has(lower(name))) names.set(lower(name), name);
    }
    return [...names.values()];
}
