MATCH (t {project: $project, fqn: $fqn})-[:DECLARES]->(field:Field)
WHERE (t:Class OR t:Interface OR t:Annotation)
RETURN __FIELD_PROJECTION__
ORDER BY field.name
LIMIT $limit
