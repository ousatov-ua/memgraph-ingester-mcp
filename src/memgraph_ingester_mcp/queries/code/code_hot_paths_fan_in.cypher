MATCH (caller:Method {project: $project})
  -[call:CALLS]->(method:Method {project: $project})
OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(method)
WITH method, file, count(call) AS callers
WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
RETURN 'fanIn' AS kind, method.ownerDisplayName AS owner, method.name AS name,
       callers AS score, file.path AS path,
       method.startLine AS startLine, method.endLine AS endLine,
       method.signature AS sortKey
ORDER BY score DESC, sortKey
LIMIT $limit
