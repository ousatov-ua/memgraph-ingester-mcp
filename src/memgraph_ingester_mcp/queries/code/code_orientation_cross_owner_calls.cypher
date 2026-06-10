MATCH (caller:Method {project: $project})
  -[:CALLS]->(callee:Method {project: $project})
WHERE caller.ownerFqn IS NOT NULL AND callee.ownerFqn IS NOT NULL
  AND caller.ownerFqn <> callee.ownerFqn
WITH caller.ownerDisplayName + ' -> ' + callee.ownerDisplayName AS edge,
     COUNT(*) AS calls
RETURN edge, calls
ORDER BY calls DESC, edge
LIMIT $limit
