MATCH (file:File {project: $project})-[:DEFINES]->(type {project: $project})
WHERE ($include_tests OR NOT file.path STARTS WITH 'src/test/')
  AND (type:Class OR type:Interface OR type:Annotation)
OPTIONAL MATCH (type)-[:DECLARES]->(method:Method {project: $project})
WITH file, type, count(DISTINCT method) AS methods
RETURN 'type' AS kind, labels(type)[0] AS owner, type.name AS name,
       methods AS score, file.path AS path, null AS startLine,
       null AS endLine, type.fqn AS sortKey
ORDER BY score DESC, sortKey
LIMIT $limit
