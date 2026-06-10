MATCH (memory {project: $project, id: $memory_id})
WHERE __MEMORY_LABEL_PREDICATE__
OPTIONAL MATCH (memory)-[:HAS_RAG_CHUNK]->(chunk:MemoryChunk {project: $project})
WITH memory, [chunk IN collect(DISTINCT chunk) WHERE chunk IS NOT NULL] AS chunks
FOREACH (chunk IN chunks | DETACH DELETE chunk)
DETACH DELETE memory
RETURN true AS deleted
