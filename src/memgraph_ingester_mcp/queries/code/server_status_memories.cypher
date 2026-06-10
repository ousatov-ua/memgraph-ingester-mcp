MATCH (node {project: $project})
WHERE node:Decision OR node:ADR OR node:Rule OR node:Context
   OR node:Finding OR node:Task OR node:Risk OR node:Question OR node:Idea
RETURN labels(node)[0] AS type, count(node) AS count
ORDER BY type
