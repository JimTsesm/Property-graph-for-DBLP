# Create Conference
LOAD CSV FROM "file:///Conference.csv" AS csvLine
CREATE (c:Conference {name: csvLine[0]})

# Create Edition nodes first and then the edges between Conferences and Editions
LOAD CSV WITH HEADERS FROM "file:///Edition.csv" AS csvLine
CREATE (e:Edition {key: csvLine.key, year: csvLine.year})
with csvLine as csvLine
MATCH (c:Conference {name: csvLine.booktitle}), (e:Edition {key: csvLine.key})
CREATE (c)-[:HAS]->(e)
RETURN c