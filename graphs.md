# How to perform Graph Queries with DuckDB

DuckDB supports graph queries primarily through the DuckPGQ extension, implementing the SQL/PGQ standard.

## 1. Installation
```sql
INSTALL duckpgq FROM community;
LOAD duckpgq;
```

## 2. Defining a Property Graph
Map your relational tables to nodes and edges:
```sql
CREATE PROPERTY GRAPH my_graph
VERTEX TABLES (users)
EDGE TABLES (follows SOURCE users DESTINATION users);
```

## 3. Querying with GRAPH_TABLE
Use Cypher-like syntax for pattern matching:
```sql
FROM GRAPH_TABLE (
  my_graph
  MATCH (a:User)-[f:Follows]->(b:User)
  COLUMNS (a.name AS follower, b.name AS followed)
);
```

## 4. Shortest Path
```sql
FROM GRAPH_TABLE (
  my_graph
  MATCH p = ANY SHORTEST (a:User)-[f:Follows]->+ (b:User)
  WHERE a.name = 'Alice'
  COLUMNS (path_length(p) AS distance)
);
```
