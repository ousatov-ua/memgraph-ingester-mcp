MATCH (file:File {project: $project})
  -[:DEFINES]->(method:Method {project: $project})
WHERE ($include_tests OR NOT file.path STARTS WITH 'src/test/')
  AND method.startLine IS NOT NULL AND method.endLine IS NOT NULL
  AND coalesce(method.isSynthetic, false) = false
WITH file, method, method.endLine - method.startLine + 1 AS lines
RETURN 'method' AS kind, method.ownerDisplayName AS owner, method.name AS name,
       lines AS score, file.path AS path,
       method.startLine AS startLine, method.endLine AS endLine,
       method.signature AS sortKey
ORDER BY score DESC, sortKey
LIMIT $limit
