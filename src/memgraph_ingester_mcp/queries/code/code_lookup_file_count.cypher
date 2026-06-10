MATCH (file:File {project: $project})
WHERE file.path CONTAINS $fragment
  AND ($include_tests OR NOT file.path STARTS WITH 'src/test/')
RETURN count(DISTINCT file) AS count
