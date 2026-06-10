MATCH (caller:Method {project: $project})-[:CALLS]->(target:Method {project: $project})
WHERE target.signature CONTAINS $fragment
OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
OPTIONAL MATCH (targetFile:File {project: $project})-[:DEFINES]->(target)
WITH caller, target, callerFile, targetFile
WHERE $include_tests
   OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
   AND (targetFile.path IS NULL OR NOT targetFile.path STARTS WITH 'src/test/'))
RETURN DISTINCT 1 AS depth,
       caller.signature AS callerSignature,
       caller.ownerDisplayName AS callerOwner,
       caller.ownerFqn AS callerOwnerFqn,
       caller.name AS callerName,
       caller.startLine AS callerStartLine,
       caller.endLine AS callerEndLine,
       callerFile.path AS callerPath,
       null AS viaSignature,
       null AS viaOwner,
       null AS viaOwnerFqn,
       null AS viaName,
       null AS viaPath,
       target.signature AS targetSignature,
       target.ownerDisplayName AS targetOwner,
       target.ownerFqn AS targetOwnerFqn,
       target.name AS targetName,
       targetFile.path AS targetPath
UNION ALL
MATCH (caller:Method {project: $project})
  -[:CALLS]->(via:Method {project: $project})
  -[:CALLS]->(target:Method {project: $project})
WHERE $depth >= 2 AND target.signature CONTAINS $fragment
OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
OPTIONAL MATCH (viaFile:File {project: $project})-[:DEFINES]->(via)
OPTIONAL MATCH (targetFile:File {project: $project})-[:DEFINES]->(target)
WITH caller, via, target, callerFile, viaFile, targetFile
WHERE $include_tests
   OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
   AND (viaFile.path IS NULL OR NOT viaFile.path STARTS WITH 'src/test/')
   AND (targetFile.path IS NULL OR NOT targetFile.path STARTS WITH 'src/test/'))
RETURN DISTINCT 2 AS depth,
       caller.signature AS callerSignature,
       caller.ownerDisplayName AS callerOwner,
       caller.ownerFqn AS callerOwnerFqn,
       caller.name AS callerName,
       caller.startLine AS callerStartLine,
       caller.endLine AS callerEndLine,
       callerFile.path AS callerPath,
       via.signature AS viaSignature,
       via.ownerDisplayName AS viaOwner,
       via.ownerFqn AS viaOwnerFqn,
       via.name AS viaName,
       viaFile.path AS viaPath,
       target.signature AS targetSignature,
       target.ownerDisplayName AS targetOwner,
       target.ownerFqn AS targetOwnerFqn,
       target.name AS targetName,
       targetFile.path AS targetPath
ORDER BY depth, callerSignature, viaSignature, targetSignature
SKIP $skip
LIMIT $impact_limit
