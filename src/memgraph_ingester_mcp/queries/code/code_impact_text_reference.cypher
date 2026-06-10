MATCH (caller:Method {project: $project})
  -[:HAS_RAG_CHUNK]->(chunk:CodeChunk {project: $project})
WHERE any(term IN $target_terms WHERE chunk.text CONTAINS term)
MATCH (target:Method {project: $project})
WHERE target.signature IN $target_signatures
  AND (chunk.text CONTAINS (target.name + '(')
    OR chunk.text CONTAINS (target.name + ' ('))
  AND caller <> target
OPTIONAL MATCH (caller)-[edge:CALLS]->(target)
OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
OPTIONAL MATCH (targetFile:File {project: $project})-[:DEFINES]->(target)
WITH caller, target, chunk, callerFile, targetFile, count(edge) AS existingEdges
WHERE existingEdges = 0
  AND (
    $include_tests
    OR (
      (callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
      AND (targetFile.path IS NULL OR NOT targetFile.path STARTS WITH 'src/test/')
    )
  )
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
       targetFile.path AS targetPath,
       true AS inferred,
       'textReference' AS evidence
ORDER BY callerPath, callerStartLine, callerSignature, targetSignature
SKIP $skip
LIMIT $fallback_limit
