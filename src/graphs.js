/**
 * Pure helpers for SQL/PGQ property-graph DDL generation and identifier
 * validation. Kept free of side effects (no server, no DB) so they can be
 * unit-tested in isolation and shared between the HTTP layer (src/index.js)
 * and the durable graph registry (src/db.js).
 */

/**
 * Lightweight validation: graph, table and column identifiers must match
 * [A-Za-z_][A-Za-z0-9_]*. We do not quote them in emitted DDL because DuckPGQ's
 * parser is strict about graph identifiers.
 */
export function isSafeIdentifier(name) {
    return typeof name === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(name);
}

/**
 * Build a `CREATE PROPERTY GRAPH` statement from a JSON definition.
 */
export function buildCreatePropertyGraphSql(def) {
    const { name, vertexTables, edgeTables } = def || {};
    if (!isSafeIdentifier(name)) {
        throw new Error('Invalid or missing "name" (must be a simple identifier).');
    }
    if (!Array.isArray(vertexTables) || vertexTables.length === 0) {
        throw new Error('"vertexTables" must be a non-empty array.');
    }
    if (!Array.isArray(edgeTables)) {
        throw new Error('"edgeTables" must be an array.');
    }

    const formatVertex = (v) => {
        if (typeof v === 'string') {
            if (!isSafeIdentifier(v)) throw new Error(`Invalid vertex table name: ${v}`);
            return v;
        }
        if (v && typeof v === 'object' && isSafeIdentifier(v.name)) {
            let out = v.name;
            // DuckPGQ only accepts `KEY (col)` in conjunction with an
            // explicit `LABEL`. When a label is not provided, omit KEY and
            // let DuckPGQ auto-discover the primary key.
            if (v.label) {
                if (!isSafeIdentifier(v.label)) throw new Error(`Invalid vertex label: ${v.label}`);
                if (v.key) {
                    if (!isSafeIdentifier(v.key)) throw new Error(`Invalid vertex key: ${v.key}`);
                    out += ` KEY (${v.key})`;
                }
                out += ` LABEL ${v.label}`;
            } else if (v.key) {
                // Key provided without label: silently drop it, since DuckPGQ
                // does not accept KEY without LABEL. Callers who need an
                // explicit key must also supply a label.
            }
            return out;
        }
        throw new Error('Vertex entries must be a string or { name, label?, key? }.');
    };

    const formatEdge = (e) => {
        if (!e || typeof e !== 'object') throw new Error('Edge entries must be objects.');
        const { name: edgeName, source, destination, sourceKey, destinationKey, sourceRef, destinationRef, label } = e;
        if (!isSafeIdentifier(edgeName)) throw new Error(`Invalid edge table name: ${edgeName}`);
        if (!isSafeIdentifier(source)) throw new Error(`Invalid edge source: ${source}`);
        if (!isSafeIdentifier(destination)) throw new Error(`Invalid edge destination: ${destination}`);

        let out = edgeName;
        if (label) {
            if (!isSafeIdentifier(label)) throw new Error(`Invalid edge label: ${label}`);
            out += ` LABEL ${label}`;
        }
        // SOURCE [KEY (col) REFERENCES] vertex [(ref)]
        out += ' SOURCE';
        if (sourceKey) {
            if (!isSafeIdentifier(sourceKey)) throw new Error(`Invalid sourceKey: ${sourceKey}`);
            out += ` KEY (${sourceKey}) REFERENCES ${source}`;
            if (sourceRef) {
                if (!isSafeIdentifier(sourceRef)) throw new Error(`Invalid sourceRef: ${sourceRef}`);
                out += ` (${sourceRef})`;
            }
        } else {
            out += ` ${source}`;
        }
        out += ' DESTINATION';
        if (destinationKey) {
            if (!isSafeIdentifier(destinationKey)) throw new Error(`Invalid destinationKey: ${destinationKey}`);
            out += ` KEY (${destinationKey}) REFERENCES ${destination}`;
            if (destinationRef) {
                if (!isSafeIdentifier(destinationRef)) throw new Error(`Invalid destinationRef: ${destinationRef}`);
                out += ` (${destinationRef})`;
            }
        } else {
            out += ` ${destination}`;
        }
        return out;
    };

    const vertexClause = `VERTEX TABLES (${vertexTables.map(formatVertex).join(', ')})`;
    const edgeClause = edgeTables.length > 0
        ? ` EDGE TABLES (${edgeTables.map(formatEdge).join(', ')})`
        : '';

    return `CREATE PROPERTY GRAPH ${name} ${vertexClause}${edgeClause}`;
}
