CALL embeddings.text([$query], $embed_config) YIELD embeddings
WITH embeddings[0] AS queryVector
CALL vector_search.search($index, $limit, queryVector)
YIELD node AS chunk, similarity
WITH chunk, similarity
WHERE chunk.project = $project
MATCH (memory {project: $project})-[:HAS_RAG_CHUNK]->(chunk)
RETURN labels(memory) AS type, memory.id AS id, memory.title AS title,
       memory.status AS status, chunk.sourceLabel AS sourceLabel,
       chunk.sourceId AS sourceId, similarity
ORDER BY similarity DESC
