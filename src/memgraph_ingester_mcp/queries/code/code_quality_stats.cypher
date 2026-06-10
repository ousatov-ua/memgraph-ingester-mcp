CALL {
  MATCH (n {project: $project})
  WITH labels(n) AS labels, count(n) AS count
  ORDER BY count DESC
  RETURN collect({labels: labels, count: count}) AS inventory
}
CALL {
  MATCH (file:File {project: $project})
    -[:DEFINES]->(method:Method {project: $project})
  WHERE ($include_tests OR NOT file.path STARTS WITH 'src/test/')
    AND method.startLine IS NOT NULL AND method.endLine IS NOT NULL
    AND coalesce(method.isSynthetic, false) = false
  WITH method.endLine - method.startLine + 1 AS lines
  RETURN {
    methods: count(lines),
    avgLines: round(avg(lines) * 100) / 100,
    maxLines: max(lines),
    methods50Plus: sum(CASE WHEN lines >= 50 THEN 1 ELSE 0 END),
    methods100Plus: sum(CASE WHEN lines >= 100 THEN 1 ELSE 0 END)
  } AS methodLengths
}
CALL {
  MATCH (method:Method {project: $project})
  OPTIONAL MATCH (method)-[call:CALLS]->(:Method {project: $project})
  WITH method, count(call) AS degree
  RETURN {
    methods: count(method),
    avgOut: round(avg(degree) * 100) / 100,
    maxOut: max(degree),
    methodsOut10Plus: sum(CASE WHEN degree >= 10 THEN 1 ELSE 0 END),
    methodsOut0: sum(CASE WHEN degree = 0 THEN 1 ELSE 0 END)
  } AS fanOut
}
CALL {
  MATCH (method:Method {project: $project})
  OPTIONAL MATCH (:Method {project: $project})-[call:CALLS]->(method)
  WITH method, count(call) AS degree
  RETURN {
    methods: count(method),
    avgIn: round(avg(degree) * 100) / 100,
    maxIn: max(degree),
    methodsIn10Plus: sum(CASE WHEN degree >= 10 THEN 1 ELSE 0 END),
    methodsIn0: sum(CASE WHEN degree = 0 THEN 1 ELSE 0 END)
  } AS fanIn
}
CALL {
  MATCH (type {project: $project})
  WHERE type:Class OR type:Interface OR type:Annotation
  OPTIONAL MATCH (type)-[:DECLARES]->(method:Method {project: $project})
  WITH type, count(method) AS methods
  RETURN {
    types: count(type),
    avgMethodsPerType: round(avg(methods) * 100) / 100,
    maxMethodsPerType: max(methods),
    types25MethodsPlus: sum(CASE WHEN methods >= 25 THEN 1 ELSE 0 END),
    types50MethodsPlus: sum(CASE WHEN methods >= 50 THEN 1 ELSE 0 END)
  } AS typeSizes
}
CALL {
  MATCH (chunk:CodeChunk {project: $project})
  WITH chunk.sourceLabel AS sourceLabel, count(chunk) AS chunks
  ORDER BY chunks DESC, sourceLabel
  RETURN collect({sourceLabel: sourceLabel, chunks: chunks}) AS chunksByLabel
}
CALL {
  MATCH (file:File {project: $project})
    -[:DEFINES]->(method:Method {project: $project})
  WHERE $include_tests OR NOT file.path STARTS WITH 'src/test/'
  WITH file.path AS path, count(method) AS methods
  ORDER BY methods DESC, path
  LIMIT $limit
  RETURN collect({path: path, methods: methods}) AS filesByMethods
}
RETURN inventory, methodLengths, fanOut, fanIn, typeSizes,
       chunksByLabel, filesByMethods
