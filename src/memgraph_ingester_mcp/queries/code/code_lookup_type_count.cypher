MATCH (t {project: $project})
WHERE (t:Class OR t:Interface OR t:Annotation) AND __PREDICATE__
OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(t)
WITH t, file
WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
RETURN count(DISTINCT t) AS count
