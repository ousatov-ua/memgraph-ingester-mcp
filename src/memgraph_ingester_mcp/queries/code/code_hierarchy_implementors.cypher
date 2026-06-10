MATCH (impl:Class {project: $project})-[:EXTENDS*0..]->(:Class {project: $project})
      -[:IMPLEMENTS]->(:Interface {project: $project})
      -[:EXTENDS*0..]->(i:Interface {fqn: $fqn, project: $project})
RETURN DISTINCT impl.fqn AS implementor
ORDER BY implementor
