MATCH (method:Method {project: $project})
WHERE all(term IN $fragment_terms WHERE toLower(method.signature) CONTAINS term)
OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(method)
WITH method, file
WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
WITH method, collect(DISTINCT file.path) AS files
RETURN __RETURN_PROJECTION__
ORDER BY __ORDER_BY__
SKIP $skip
LIMIT $limit
