LOAD CSV WITH HEADERS FROM "file:///Conferences.csv" AS csvLine
CREATE (e:Edition {key: csvLine.key, year: csvLine.year})
with csvLine as csvLine
MATCH (c:Conference {name: csvLine.booktitle}), (e:Edition {key: csvLine.key})
CREATE (c)-[:HAS]->(e)
RETURN c