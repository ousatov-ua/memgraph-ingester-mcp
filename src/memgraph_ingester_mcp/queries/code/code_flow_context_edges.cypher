MATCH (callerFile:File {project: $project})
  -[:DEFINES]->(caller:Method {project: $project})
  -[:CALLS]->(callee:Method {project: $project})
  <-[:DEFINES]-(calleeFile:File {project: $project})
WHERE (callerFile.path IN $paths OR calleeFile.path IN $paths)
  AND ($include_tests
       OR (NOT callerFile.path STARTS WITH 'src/test/'
           AND NOT calleeFile.path STARTS WITH 'src/test/'))
RETURN callerFile.path AS callerPath,
       caller.ownerDisplayName AS callerOwner,
       caller.name AS callerName,
       caller.startLine AS callerStartLine,
       calleeFile.path AS calleePath,
       callee.ownerDisplayName AS calleeOwner,
       callee.name AS calleeName,
       callee.startLine AS calleeStartLine
ORDER BY callerPath, callerStartLine, calleePath, calleeStartLine
LIMIT $limit
