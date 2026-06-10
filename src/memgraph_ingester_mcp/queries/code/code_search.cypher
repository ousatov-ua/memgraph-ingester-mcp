CALL embeddings.text($queries, $embed_config) YIELD embeddings
UNWIND embeddings AS queryVector
CALL vector_search.search($index, $limit, queryVector)
YIELD node AS chunk, similarity
WITH chunk, max(similarity) AS similarity
WHERE chunk.project = $project
  AND ($include_tests OR chunk.path IS NULL OR NOT chunk.path STARTS WITH 'src/test/')
MATCH (source {project: $project})-[:HAS_RAG_CHUNK]->(chunk)
WITH chunk, source, similarity,
     CASE
       WHEN chunk.sourceLabel = 'Method'
         AND coalesce(source.startLine, 0) <= 0 THEN 'synthetic'
       WHEN chunk.sourceLabel = 'Class'
         AND coalesce(chunk.kind, source.kind, '') = 'module' THEN 'synthetic'
       WHEN chunk.sourceLabel = 'Method'
         AND coalesce(chunk.kind, '') = 'constructor' THEN 'secondary'
       WHEN chunk.sourceLabel = 'Field' THEN 'secondary'
       WHEN chunk.sourceLabel = 'File' THEN 'file'
       ELSE coalesce(chunk.ragRole, 'primary')
     END AS effectiveRole
WHERE size($rag_roles) = 0 OR effectiveRole IN $rag_roles
WITH chunk, source, similarity, effectiveRole,
     coalesce(chunk.sourceLabel, labels(source)[0]) AS kind,
     chunk.sourceId AS sourceId,
     coalesce(source.ownerDisplayName, source.ownerFqn, chunk.ownerFqn) AS owner,
     coalesce(source.name, chunk.signature, chunk.sourceId) AS name,
     chunk.path AS path,
     source.startLine AS startLine,
     source.endLine AS endLine
WHERE (size($kinds) = 0 OR kind IN $kinds)
  AND (size($path_prefixes) = 0
       OR any(prefix IN $path_prefixes WHERE path STARTS WITH prefix))
  AND ($path_contains = '' OR path CONTAINS $path_contains)
  AND ($owner_fragment = '' OR coalesce(owner, '') CONTAINS $owner_fragment)
  AND ($min_score <= 0 OR similarity >= $min_score)
RETURN __RETURN_PROJECTION__
ORDER BY similarity DESC
