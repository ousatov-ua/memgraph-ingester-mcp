MATCH (target:Method {project: $project})
WHERE target.signature CONTAINS $fragment
OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(target)
WITH target, collect(DISTINCT file.path) AS files
WHERE $include_tests
   OR files = []
   OR any(path IN files WHERE NOT path STARTS WITH 'src/test/')
RETURN target.signature AS signature,
       target.ownerDisplayName AS owner,
       target.ownerFqn AS ownerFqn,
       target.name AS name,
       target.startLine AS startLine,
       target.endLine AS endLine,
       files
ORDER BY signature
LIMIT $limit
