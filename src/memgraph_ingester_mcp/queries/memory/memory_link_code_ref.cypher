MATCH (node:__LABEL__ {id: $memory_id, project: $project})
MATCH (target:__TARGET_LABEL__ {project: $project})
WHERE __TARGET_PREDICATE__
MERGE (ref:CodeRef {project: $project, targetType: $target_type, key: $target_key})
MERGE (node)-[:REFERS_TO]->(ref)
MERGE (ref)-[:RESOLVES_TO]->(target)
RETURN node.id AS memoryId, ref.targetType AS targetType, ref.key AS key,
       labels(target) AS targetLabels
