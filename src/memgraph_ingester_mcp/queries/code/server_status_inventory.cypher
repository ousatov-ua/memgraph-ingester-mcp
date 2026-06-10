MATCH (code:Code {project: $project})
OPTIONAL MATCH (file:File {project: $project})
WITH collect(DISTINCT code.language) AS languages, count(DISTINCT file) AS files
OPTIONAL MATCH (type {project: $project})
WHERE type:Class OR type:Interface OR type:Annotation
WITH languages, files, count(DISTINCT type) AS types
OPTIONAL MATCH (method:Method {project: $project})
RETURN size(languages) AS languageCount, files AS fileCount,
       types AS typeCount, count(DISTINCT method) AS methodCount
