MERGE (root:Memory {project: $project})
MERGE (node:__LABEL__ {id: $memory_id, project: $project})
SET node += $properties,
    node.createdAt = coalesce(node.createdAt, datetime()),
    node.updatedAt = datetime()
MERGE (root)-[:__RELATION__]->(node)
RETURN labels(node) AS labels, properties(node) AS properties
