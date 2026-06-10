MATCH (caller:Method {project: $project})-[:CALLS]->(callee:Method {project: $project})
WHERE caller.signature CONTAINS $fragment
OPTIONAL MATCH (calleeFile:File {project: $project})-[:DEFINES]->(callee)
OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
WITH caller, callee, callerFile, calleeFile
WHERE $include_tests
   OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
   AND (calleeFile.path IS NULL OR NOT calleeFile.path STARTS WITH 'src/test/'))
RETURN caller.signature AS callerSignature,
       caller.name AS callerName,
       caller.ownerDisplayName AS callerOwner,
       callee.signature AS calleeSignature,
       callee.name AS calleeName,
       callee.ownerDisplayName AS calleeOwner,
       callee.startLine AS calleeStartLine,
       callee.endLine AS calleeEndLine,
       calleeFile.path AS calleePath
ORDER BY caller.signature, callee.signature
SKIP $skip
LIMIT $limit
