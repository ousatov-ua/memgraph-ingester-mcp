MATCH (test:Method {project: $project})
OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(test)
WITH test, file,
     toLower(coalesce(test.signature, '') + ' ' + coalesce(test.name, '') + ' '
             + coalesce(file.path, '')) AS haystack
WITH test, file, haystack,
     size([term IN $terms WHERE haystack CONTAINS term]) AS termMatches
WHERE (file.path STARTS WITH 'src/test/'
    OR file.path STARTS WITH 'test/'
    OR file.path STARTS WITH 'tests/'
    OR file.path CONTAINS '/test/'
    OR file.path CONTAINS '/tests/')
  AND (test.signature CONTAINS $fragment
    OR test.name CONTAINS $fragment
    OR file.path CONTAINS $fragment
    OR ($owner_fragment <> ''
        AND (test.ownerDisplayName CONTAINS $owner_fragment
          OR test.signature CONTAINS $owner_fragment
          OR file.path CONTAINS $owner_fragment))
    OR termMatches >= $min_term_matches)
RETURN test.ownerDisplayName AS owner,
       test.name AS name,
       file.path AS path,
       test.startLine AS startLine,
       test.endLine AS endLine,
       CASE WHEN test.signature CONTAINS $fragment OR test.name = $fragment
            THEN true ELSE false END AS exactish,
       termMatches
ORDER BY exactish DESC, termMatches DESC, path, startLine, name
LIMIT $limit
