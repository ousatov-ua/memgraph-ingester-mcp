MATCH (t {project: $project, fqn: $fqn})-[:DECLARES]->(m:Method)
WHERE (t:Class OR t:Interface OR t:Annotation)
RETURN __METHOD_PROJECTION__
ORDER BY m.name, m.signature
LIMIT $limit
