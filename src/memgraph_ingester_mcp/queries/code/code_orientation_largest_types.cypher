MATCH (t {project: $project})-[:DECLARES]->(m:Method {project: $project})
WHERE (t:Class OR t:Interface OR t:Annotation)
  AND coalesce(t.isExternal, false) = false
  AND coalesce(m.isSynthetic, false) = false
WITH t.fqn AS type, labels(t)[0] AS label, count(m) AS methodCount
RETURN type, label, methodCount
ORDER BY methodCount DESC, type
LIMIT $limit
