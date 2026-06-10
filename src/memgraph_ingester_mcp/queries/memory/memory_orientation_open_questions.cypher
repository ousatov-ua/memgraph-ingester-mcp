MATCH (m:Memory {project: $project})-[:HAS_QUESTION]->(question:Question)
WHERE question.status = 'open'
RETURN question.id AS id, question.title AS title
ORDER BY question.id
