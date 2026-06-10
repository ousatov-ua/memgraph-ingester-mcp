MATCH (l:Language {project: $project})-[:CONTAINS]->(c:Code)
RETURN l.name AS languageName, l.graphName AS graphName, c.language AS language,
       c.lastIngested AS lastIngested
ORDER BY languageName
