MATCH (chunk:CodeChunk {project: $project})
WHERE chunk.sourceLabel = 'File'
  AND coalesce(chunk.ragRole, 'file') = 'file'
  AND ($include_tests OR chunk.path IS NULL OR NOT chunk.path STARTS WITH 'src/test/')
  AND ($path_contains = '' OR chunk.path CONTAINS $path_contains)
RETURN chunk.path AS path,
       chunk.language AS language,
       chunk.text AS text
ORDER BY chunk.path
LIMIT $limit
