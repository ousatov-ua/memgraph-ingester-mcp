MATCH (chunk:MemoryChunk {project: $project})
WHERE chunk.id IN $ids
SET chunk.embeddingModel = $model_name,
    chunk.embeddingDimensions = $dimension,
    chunk.embeddingDirty = false,
    chunk.updatedAt = datetime()
RETURN chunk.id AS id
