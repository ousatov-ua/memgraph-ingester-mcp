MATCH (test:Method {project: $project})
  -[:CALLS]->(callee:Method {project: $project})
OPTIONAL MATCH (testFile:File {project: $project})-[:DEFINES]->(test)
OPTIONAL MATCH (calleeFile:File {project: $project})-[:DEFINES]->(callee)
WITH test, callee, testFile, calleeFile
WHERE (testFile.path STARTS WITH 'src/test/'
    OR testFile.path STARTS WITH 'test/'
    OR testFile.path STARTS WITH 'tests/'
    OR testFile.path CONTAINS '/test/'
    OR testFile.path CONTAINS '/tests/')
  AND NOT (calleeFile.path STARTS WITH 'src/test/'
    OR calleeFile.path STARTS WITH 'test/'
    OR calleeFile.path STARTS WITH 'tests/'
    OR calleeFile.path CONTAINS '/test/'
    OR calleeFile.path CONTAINS '/tests/')
  AND (test.signature CONTAINS $fragment OR test.name = $fragment)
RETURN DISTINCT callee.ownerDisplayName AS owner,
       callee.name AS name,
       calleeFile.path AS path,
       callee.startLine AS startLine,
       callee.endLine AS endLine,
       test.ownerDisplayName AS testOwner,
       test.name AS testName
ORDER BY path, startLine, name
LIMIT $limit
