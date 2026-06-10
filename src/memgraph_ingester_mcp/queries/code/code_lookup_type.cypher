MATCH (t {project: $project})
WHERE (t:Class OR t:Interface OR t:Annotation) AND __PREDICATE__
OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(t)
WITH t, file
WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
WITH t, collect(DISTINCT file.path) AS files
ORDER BY t.fqn
LIMIT $limit
__MEMBER_COUNT_CYPHER__
RETURN labels(t) AS labels, t.fqn AS fqn, t.name AS name, t.kind AS kind,
       __EXTRA_TYPE_COLS__files__MEMBER_COUNT_COLS__
