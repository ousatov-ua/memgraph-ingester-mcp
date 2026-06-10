MATCH (memory {project: $project, id: $memory_id})
WHERE __MEMORY_LABEL_PREDICATE__
OPTIONAL MATCH (memory)-[:HAS_RAG_CHUNK]->(chunk:MemoryChunk {project: $project})
OPTIONAL MATCH (memory)-[:REFERS_TO]->(ref:CodeRef {project: $project})
RETURN labels(memory) AS labels, properties(memory) AS properties,
       [id IN collect(DISTINCT chunk.id) WHERE id IS NOT NULL] AS chunkIds,
       [codeRef IN collect(DISTINCT CASE WHEN ref IS NULL THEN NULL ELSE {
           targetType: ref.targetType,
           key: ref.key
       } END) WHERE codeRef IS NOT NULL] AS codeRefs
