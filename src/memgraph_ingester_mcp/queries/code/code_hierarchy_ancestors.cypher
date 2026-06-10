MATCH path =
  (c:Class {fqn: $fqn, project: $project})
  -[:EXTENDS*]->(a:Class {project: $project})
RETURN [node IN nodes(path) | node.fqn] AS ancestors
ORDER BY size(ancestors)
