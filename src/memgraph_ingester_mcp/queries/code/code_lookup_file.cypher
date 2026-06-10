MATCH (file:File {project: $project})
WHERE file.path CONTAINS $fragment
  AND ($include_tests OR NOT file.path STARTS WITH 'src/test/')
OPTIONAL MATCH (file)-[:DEFINES]->(definition {project: $project})
WITH file, count(DISTINCT definition) AS definitionCount
OPTIONAL MATCH (chunk:CodeChunk {project: $project})
WHERE chunk.path = file.path
WITH file, definitionCount, count(DISTINCT chunk) AS chunkCount
RETURN __PROJECTION__
ORDER BY file.path
SKIP $skip
LIMIT $limit
