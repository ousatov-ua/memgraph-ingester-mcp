MATCH (ref:CodeRef {project: $project})
WHERE any(codeRef IN $code_refs
          WHERE codeRef.targetType = ref.targetType AND codeRef.key = ref.key)
  AND NOT (()-[:REFERS_TO]->(ref))
WITH collect(ref) AS refs
FOREACH (ref IN refs | DETACH DELETE ref)
RETURN size(refs) AS deleted
