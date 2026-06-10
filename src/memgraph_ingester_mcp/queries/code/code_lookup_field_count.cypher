MATCH (field:Field {project: $project})
WHERE field.fqn CONTAINS $fragment OR field.name = $fragment
OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(field)
WITH field, file
WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
RETURN count(DISTINCT field) AS count
