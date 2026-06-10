MATCH (chunk:MemoryChunk {project: $project})
WHERE chunk.id IN $ids
  AND chunk.text IS NOT NULL
  AND (chunk.embedding IS NULL
    OR chunk.embeddingModel IS NULL
    OR chunk.embeddingModel <> $model_name
    OR chunk.embeddingDimensions IS NULL
    OR chunk.embeddingDimensions <> $dimension
    OR coalesce(chunk.embeddingDirty, false) = true)
RETURN chunk.id AS id
ORDER BY chunk.id
