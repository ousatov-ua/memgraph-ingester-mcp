MATCH (m:Memory {project: $project})-[:HAS_RULE]->(rule:Rule)
RETURN __RETURN_PROJECTION__
ORDER BY rule.severity, rule.id
