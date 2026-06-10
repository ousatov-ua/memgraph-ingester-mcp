MATCH (file:File {project: $project})
WHERE any(fragment IN $fragments WHERE file.path CONTAINS fragment)
  AND ($include_tests OR NOT (file.path STARTS WITH 'src/test/'
       OR file.path STARTS WITH 'test/'
       OR file.path STARTS WITH 'tests/'
       OR file.path CONTAINS '/test/'
       OR file.path CONTAINS '/tests/'))
WITH file
ORDER BY file.path
LIMIT $limit
CALL {
  WITH file
  OPTIONAL MATCH (file)-[:DEFINES]->(node)
  WHERE node IS NULL
     OR (node.project = $project
         AND (node:Class OR node:Interface OR node:Annotation))
  RETURN collect(DISTINCT CASE WHEN node IS NULL THEN null ELSE {
    label: labels(node)[0],
    name: node.name,
    fqn: node.fqn,
    kind: node.kind,
    startLine: node.startLine,
    endLine: node.endLine
  } END) AS types
}
CALL {
  WITH file
  OPTIONAL MATCH (file)-[:DEFINES]->(method)
  WHERE method IS NULL OR (method.project = $project AND method:Method)
  RETURN collect(DISTINCT CASE WHEN method IS NULL THEN null ELSE {
    owner: method.ownerDisplayName,
    name: method.name,
    startLine: method.startLine,
    endLine: method.endLine
  } END) AS methods
}
CALL {
  WITH file
  OPTIONAL MATCH (file)-[:DEFINES]->(field)
  WHERE field IS NULL OR (field.project = $project AND field:Field)
  RETURN collect(DISTINCT CASE WHEN field IS NULL THEN null ELSE {
    owner: coalesce(field.ownerDisplayName, field.ownerFqn),
    name: field.name,
    startLine: field.startLine,
    endLine: field.endLine
  } END) AS fields
}
CALL {
  WITH file
  OPTIONAL MATCH (chunk:CodeChunk {project: $project})
  WHERE chunk.path = file.path
  WITH coalesce(chunk.ragRole, chunk.sourceLabel, 'unknown') AS ragRole,
       count(chunk) AS count
  WITH collect(CASE WHEN count = 0 THEN null ELSE {
         ragRole: ragRole,
         count: count
       } END) AS chunkRoles,
       sum(count) AS chunkCount
  RETURN chunkRoles, chunkCount
}
RETURN file.path AS path,
       file.language AS language,
       size(types) + size(methods) + size(fields) AS definitionCount,
       chunkCount,
       chunkRoles,
       types,
       methods,
       fields
