MATCH (field:Field {project: $project})
WHERE field.fqn CONTAINS $fragment OR field.name = $fragment
OPTIONAL MATCH (owner {project: $project})-[:DECLARES]->(field)
WHERE owner:Class OR owner:Interface OR owner:Annotation
OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(field)
WITH field, owner, file
WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
WITH field, owner, collect(DISTINCT file.path) AS files
RETURN __PROJECTION__
ORDER BY __ORDER_BY__
SKIP $skip
LIMIT $limit
