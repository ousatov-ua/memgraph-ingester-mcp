MATCH (node:__LABEL__ {id: $memory_id, project: $project})
SET node.status = $status, node.updatedAt = datetime()
RETURN labels(node) AS labels, properties(node) AS properties
