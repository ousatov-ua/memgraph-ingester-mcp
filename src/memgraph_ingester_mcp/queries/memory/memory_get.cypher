MATCH (memory {project: $project, id: $memory_id})
WHERE __MEMORY_LABEL_PREDICATE__
OPTIONAL MATCH (memory)-[:REFERS_TO]->(ref:CodeRef)-[:RESOLVES_TO]->(target)
WITH memory, collect(
    CASE WHEN ref IS NULL THEN NULL ELSE {
        targetType: ref.targetType,
        key: ref.key,
        targetLabels: labels(target)
    } END
) AS refs
RETURN labels(memory) AS labels, properties(memory) AS properties,
       [ref IN refs WHERE ref IS NOT NULL] AS codeRefs
