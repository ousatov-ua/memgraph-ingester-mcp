MATCH (m:Memory {project: $project})-[:HAS_FINDING]->(finding:Finding)
WHERE finding.status = 'open'
RETURN __RETURN_PROJECTION__
ORDER BY finding.id
