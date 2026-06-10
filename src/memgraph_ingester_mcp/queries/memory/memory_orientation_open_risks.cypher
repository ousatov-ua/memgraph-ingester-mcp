MATCH (m:Memory {project: $project})-[:HAS_RISK]->(risk:Risk)
WHERE risk.status = 'open'
RETURN __RETURN_PROJECTION__
ORDER BY risk.severity, risk.id
