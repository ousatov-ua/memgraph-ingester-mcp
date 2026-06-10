MATCH (caller:Method {project: $project})
  -[call:CALLS]->(sink:Method {project: $project})
OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(caller)
WITH caller, sink, file, call,
     toLower(coalesce(sink.name, '')) AS sinkNameText,
     toLower(coalesce(sink.ownerDisplayName, '') + ' '
             + coalesce(sink.ownerFqn, '') + ' '
             + coalesce(sink.signature, '')) AS sinkFullText,
     toLower(coalesce(caller.ownerDisplayName, '') + ' '
             + coalesce(caller.ownerFqn, '') + ' '
             + coalesce(caller.signature, '')) AS callerText
WHERE ($include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/')
  AND any(fragment IN $fragments
          WHERE sinkNameText CONTAINS fragment
             OR ($custom_fragments AND sinkFullText CONTAINS fragment))
  AND ($owner_fragment = '' OR callerText CONTAINS $owner_fragment)
  AND ($path_contains = '' OR file.path CONTAINS $path_contains)
WITH caller, file,
     count(call) AS sinkCallEdges,
     count(DISTINCT sink) AS distinctSinks,
     collect(DISTINCT coalesce(sink.ownerDisplayName, sink.ownerFqn, '')
                      + '.' + coalesce(sink.name, ''))[..6] AS sinks,
     CASE
       WHEN caller.startLine IS NOT NULL AND caller.endLine IS NOT NULL
       THEN caller.endLine - caller.startLine + 1
       ELSE 0
     END AS lines
RETURN caller.ownerDisplayName AS owner,
       caller.name AS name,
       caller.signature AS signature,
       file.path AS path,
       caller.startLine AS startLine,
       caller.endLine AS endLine,
       lines,
       sinkCallEdges,
       distinctSinks,
       sinks,
       (sinkCallEdges * 1000 + lines) AS score
ORDER BY score DESC, signature
LIMIT $limit
