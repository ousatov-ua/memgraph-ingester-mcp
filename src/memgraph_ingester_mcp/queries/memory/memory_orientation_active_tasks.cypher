MATCH (m:Memory {project: $project})-[:HAS_TASK]->(task:Task)
WHERE task.status IN ['todo', 'doing', 'blocked']
RETURN __RETURN_PROJECTION__
ORDER BY task.priority, task.status, task.id
