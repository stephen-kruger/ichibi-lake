/**
 * Schema-evolution helpers for DuckDB STRUCT widening.
 *
 * When a table already has a STRUCT column and incoming JSON infers a
 * different shape, we ALTER the column to the union of both shapes so
 * DuckDB never hits "STRUCT to STRUCT cast must have at least one
 * matching member".
 */

/**
 * Parse a DuckDB STRUCT(...) type string into a Map of field-name -> type-string.
 * Handles nested structs, quoted field names, and commas inside nested
 * parentheses (e.g. DECIMAL(10,2) or MAP(VARCHAR, INTEGER)).
 */
export function parseStructType(typeStr) {
    typeStr = typeStr.trim();
    const prefix = 'STRUCT(';
    if (!typeStr.toUpperCase().startsWith(prefix)) return null;

    const fields = new Map();
    let depth = 0;
    let currentField = '';

    for (let i = prefix.length; i < typeStr.length; i++) {
        const char = typeStr[i];
        if (char === '(') {
            depth++;
            currentField += char;
        } else if (char === ')') {
            if (depth === 0) {
                // closing paren of the outer STRUCT
                if (currentField.trim()) {
                    addField(currentField.trim(), fields);
                }
                break;
            }
            depth--;
            currentField += char;
        } else if (char === ',' && depth === 0) {
            if (currentField.trim()) {
                addField(currentField.trim(), fields);
            }
            currentField = '';
        } else {
            currentField += char;
        }
    }
    return fields;
}

function addField(fieldStr, fields) {
    fieldStr = fieldStr.trim();
    let name, type;
    if (fieldStr.startsWith('"')) {
        const endQuote = fieldStr.indexOf('"', 1);
        name = fieldStr.slice(1, endQuote);
        type = fieldStr.slice(endQuote + 1).trim();
    } else {
        const firstSpace = fieldStr.indexOf(' ');
        name = fieldStr.slice(0, firstSpace);
        type = fieldStr.slice(firstSpace + 1).trim();
    }
    fields.set(name, type);
}

/**
 * Build a DuckDB STRUCT type string from a Map of fields.
 */
function buildStructType(fields) {
    const entries = [];
    for (const [name, type] of fields) {
        const safeName = /[^a-zA-Z0-9_]/.test(name) ? `"${name}"` : name;
        entries.push(`${safeName} ${type}`);
    }
    return `STRUCT(${entries.join(', ')})`;
}

/**
 * Compute the union of two DuckDB STRUCT type strings.
 *
 * Returns a new STRUCT type that contains all fields from both inputs.
 * Existing fields keep their original type. New fields are added.
 * For fields present in both where the types are different structs,
 * recursively merges them. For primitive type conflicts, keeps the
 * existing type.
 *
 * Returns null if either type is not a struct.
 */
export function unionStructTypes(type1, type2) {
    const fields1 = parseStructType(type1);
    const fields2 = parseStructType(type2);
    if (!fields1 || !fields2) return null;

    const union = new Map(fields1); // start with existing fields / types
    for (const [name, type] of fields2) {
        if (!union.has(name)) {
            union.set(name, type); // add brand-new field
        } else if (union.get(name) !== type) {
            // Field exists in both with different types. Try recursive merge.
            const merged = unionStructTypes(union.get(name), type);
            if (merged) {
                union.set(name, merged);
            } else {
                // Not both structs – fall back to VARCHAR so the data survives.
                union.set(name, 'VARCHAR');
            }
        }
    }

    return buildStructType(union);
}

/**
 * Decide how to handle a column that exists in the target table but has a
 * different inferred type in the incoming JSON.
 *
 * Returns an object:
 *   { action: 'insert' }                     -> safe to insert directly
 *   { action: 'widen', newType: string }     -> ALTER the column to this type first
 *   { action: 'skip',  reason: string }      -> omit the column from this batch
 *                                               (DuckLake cannot evolve the type)
 */
export function resolveTypeConflict(existingType, newType) {
    if (existingType === newType) {
        return { action: 'insert' };
    }

    const existingStruct = parseStructType(existingType);
    const newStruct = parseStructType(newType);

    // Neither is a struct – let DuckDB handle primitive casts
    if (!existingStruct && !newStruct) {
        return { action: 'insert' };
    }

    // One is struct and the other is not. DuckLake only supports lossless
    // type promotions in ALTER COLUMN SET DATA TYPE, so STRUCT <-> primitive
    // cannot be widened. Skip the column for this batch to keep ingestion
    // alive without corrupting the existing schema.
    if (!existingStruct || !newStruct) {
        return {
            action: 'skip',
            reason: `cannot evolve between ${existingType} and ${newType}`,
        };
    }

    // Both are structs. Determine whether we need to widen.
    let needWiden = false;

    // New fields that don't exist in the table -> need widen
    for (const name of newStruct.keys()) {
        if (!existingStruct.has(name)) {
            needWiden = true;
            break;
        }
    }

    // Shared fields where both types are structs but differ -> need recursive widen
    if (!needWiden) {
        for (const [name, type] of newStruct) {
            if (existingStruct.has(name) && existingStruct.get(name) !== type) {
                const merged = unionStructTypes(existingStruct.get(name), type);
                if (merged && merged !== existingStruct.get(name)) {
                    needWiden = true;
                    break;
                }
            }
        }
    }

    if (needWiden) {
        const unionType = unionStructTypes(existingType, newType);
        return { action: 'widen', newType: unionType };
    }

    // New struct is a subset of the existing one (or identical).
    // DuckDB can cast by filling missing fields with NULL.
    return { action: 'insert' };
}

/**
 * Format a table schema (array of {column_name, data_type} rows) into a
 * human-readable CREATE TABLE-like definition string.
 */
export function formatSchemaDefinition(tableName, columns) {
    const defs = columns
        .map(c => `  "${c.column_name}" ${c.data_type}`)
        .join(',\n');
    return `CREATE TABLE "${tableName}" (\n${defs}\n);`;
}
