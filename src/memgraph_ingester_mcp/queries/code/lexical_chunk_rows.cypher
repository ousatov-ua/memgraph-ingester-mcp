MATCH (source {project: $project})
  -[:HAS_RAG_CHUNK]->(chunk:CodeChunk {project: $project})
WITH source, chunk,
     toLower(coalesce(chunk.text, '') + ' ' + coalesce(chunk.path, '') + ' '
             + coalesce(chunk.sourceId, '')) AS haystack,
     toLower(coalesce(source.name, chunk.name, '')) AS nameLower,
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
WITH source, chunk, haystack, nameLower, effectiveRole,
     [term IN $search_terms WHERE haystack CONTAINS term] AS matchedTerms
WHERE ($include_tests OR chunk.path IS NULL OR NOT chunk.path STARTS WITH 'src/test/')
  AND (size($all_terms) = 0 OR all(term IN $all_terms WHERE haystack CONTAINS term))
  AND (size($any_terms) = 0 OR any(term IN $any_terms WHERE haystack CONTAINS term))
  AND (size($kinds) = 0 OR coalesce(chunk.sourceLabel, labels(source)[0]) IN $kinds)
  AND (size($rag_roles) = 0 OR effectiveRole IN $rag_roles)
  AND ($path_contains = '' OR chunk.path CONTAINS $path_contains)
WITH source, chunk, effectiveRole, matchedTerms,
     size(matchedTerms) AS termMatches,
     size([term IN matchedTerms WHERE nameLower CONTAINS term]) AS nameMatches
ORDER BY nameMatches DESC, termMatches DESC, chunk.path, source.startLine,
chunk.sourceId
LIMIT $limit
RETURN coalesce(chunk.sourceLabel, labels(source)[0]) AS kind,
       chunk.sourceId AS sourceId,
       coalesce(source.ownerDisplayName, source.ownerFqn, chunk.ownerFqn) AS owner,
       coalesce(source.name, chunk.signature, chunk.sourceId) AS name,
       chunk.path AS path,
       effectiveRole AS ragRole,
       source.startLine AS startLine,
       source.endLine AS endLine,
       termMatches__TEXT_PROJECTION__
