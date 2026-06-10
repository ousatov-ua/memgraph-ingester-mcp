MATCH (p:Package {project: $project})
OPTIONAL MATCH (p)-[:CONTAINS]->(c:Class {project: $project})
WITH p, count(DISTINCT c) AS classes
RETURN p.language AS language, p.name AS package, classes
ORDER BY language, package
LIMIT $limit
