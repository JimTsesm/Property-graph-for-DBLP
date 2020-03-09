# Create Conference
LOAD CSV FROM "file:///Conference.csv" AS csvLine
CREATE (c:Conference {name: csvLine[0]})

# Create Edition nodes first and then the edges between Conferences and Editions
LOAD CSV WITH HEADERS FROM "file:///Edition.csv" AS csvLine
CREATE (e:Edition {key: csvLine.key, year: csvLine.year})
with csvLine as csvLine
MATCH (c:Conference {name: csvLine.booktitle}), (e:Edition {key: csvLine.key})
CREATE (c)-[:HAS]->(e)

# Create Paper for Edition and then the edges between Paper and Edition
LOAD CSV WITH HEADERS FROM "file:///Paper(Edition).csv" AS csvLine
CREATE (e:Paper {key: csvLine.key, title: csvLine.title, year: csvLine.year})
with csvLine as csvLine
MATCH (p:Paper {key: csvLine.key}), (e:Edition {key: csvLine.crossref})
CREATE (p)-[:PUBLISHED]->(e)

# Create Author for Edition's Paper
LOAD CSV FROM "file:///Author(Edition_Paper).csv" AS csvLine
CREATE (e:Author {name: csvLine[0]})

# Create the edges between Author and Paper
LOAD CSV WITH HEADERS FROM "file:///Edition_Paper_Author.csv" AS csvLine
MATCH (p:Paper {key: csvLine.key}), (a:Author {name: csvLine.author})
CREATE (a)-[:WRITES]->(p)

# Create citations
LOAD CSV WITH HEADERS FROM "file:///Cite.csv" AS csvLine
Create (c:Citation {source_id: csvLine.New_ID})
with csvLine as csvLine
MATCH (p:Paper {key: csvLine.key}), (a:Citation {source_id: csvLine.New_ID})
CREATE (p)-[:CITED_BY]->(a)