MATCH (caller:Method {project: $project})
  -[:CALLS]->(callee:Method {project: $project})
WHERE caller.signature CONTAINS $fragment
OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
OPTIONAL MATCH (calleeFile:File {project: $project})-[:DEFINES]->(callee)
WITH caller, callee, callerFile, calleeFile
WHERE $include_tests
   OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
   AND (calleeFile.path IS NULL OR NOT calleeFile.path STARTS WITH 'src/test/'))
RETURN count(*) AS count
