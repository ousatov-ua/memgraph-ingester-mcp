MATCH (node:__LABEL__ {id: $memory_id, project: $project})
MERGE (chunk:MemoryChunk {id: $chunk_id, project: $project})
SET chunk.sourceLabel = $memory_type,
    chunk.sourceId = node.id,
    chunk.text = $text,
    chunk.textHash = $text_hash,
    chunk.createdAt = coalesce(chunk.createdAt, datetime()),
    chunk.updatedAt = datetime(),
    chunk.embeddingDirty = true
REMOVE chunk.embedding, chunk.embeddingModel, chunk.embeddingDimensions
MERGE (node)-[:HAS_RAG_CHUNK]->(chunk)
RETURN chunk.id AS id, chunk.textHash AS textHash, chunk.embeddingDirty AS dirty
