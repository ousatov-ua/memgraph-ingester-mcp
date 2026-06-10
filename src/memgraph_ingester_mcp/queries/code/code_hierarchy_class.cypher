MATCH (c:Class {fqn: $fqn, project: $project})
OPTIONAL MATCH (c)-[:EXTENDS]->(parent:Class {project: $project})
OPTIONAL MATCH (c)-[:IMPLEMENTS]->(iface:Interface {project: $project})
OPTIONAL MATCH (child:Class {project: $project})-[:EXTENDS]->(c)
WITH c.fqn AS classFqn, collect(DISTINCT parent.fqn) AS parents,
     collect(DISTINCT iface.fqn) AS interfaces, collect(DISTINCT child.fqn) AS children
RETURN classFqn, parents, interfaces, children
