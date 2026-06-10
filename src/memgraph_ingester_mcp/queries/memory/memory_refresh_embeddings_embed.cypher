MATCH (chunk:MemoryChunk {project: $project})
WHERE chunk.id IN $ids
WITH chunk
ORDER BY chunk.id
WITH collect(chunk) AS chunks
WITH chunks, [chunk IN chunks | chunk.id] AS embeddedIds
CALL embeddings.node_sentence(chunks, $embed_config)
YIELD success, dimension
RETURN success AS success, dimension AS dimension, embeddedIds AS ids
