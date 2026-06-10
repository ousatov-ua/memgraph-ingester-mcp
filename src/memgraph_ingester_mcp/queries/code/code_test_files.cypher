MATCH (file:File {project: $project})
WITH file, toLower(file.path) AS haystack
WHERE (file.path STARTS WITH 'src/test/'
    OR file.path STARTS WITH 'test/'
    OR file.path STARTS WITH 'tests/'
    OR file.path CONTAINS '/test/'
    OR file.path CONTAINS '/tests/')
  AND (file.path CONTAINS $fragment
    OR ($owner_fragment <> '' AND file.path CONTAINS $owner_fragment)
    OR size([term IN $terms WHERE haystack CONTAINS term]) >= $min_term_matches)
RETURN file.path AS path, file.language AS language
ORDER BY file.path
LIMIT $limit
