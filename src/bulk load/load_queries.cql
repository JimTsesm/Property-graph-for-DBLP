# Create Conference
LOAD CSV FROM "file:///Conferences.csv" AS csvLine
CREATE (c:Conference {name: csvLine[0]})

# Create Edition nodes first and then the edges between Conferences and Editions
LOAD CSV WITH HEADERS FROM "file:///Edition.csv" AS csvLine
CREATE (e:Edition {key: csvLine.key, year: csvLine.year})
with csvLine as csvLine, e as e
MATCH (c:Conference {name: csvLine.booktitle})
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

# Create Journal
LOAD CSV FROM "file:///journal.csv" as csvLine
CREATE(j:Journal {name: csvLine[0]})

# Create Volume for Journal
LOAD CSV WITH HEADERS FROM "file:///volume(journal).csv" as csvLine
CREATE (v:Volume {key: csvLine.key, year: csvLine.year})
WITH csvLine as csvLine, v as v
MATCH (j:Journal {name: csvLine.journal})
CREATE (j)-[:HAS]->(v)

# Create Paper for Volume
LOAD CSV WITH HEADERS FROM "file:///paper(volume).csv" as csvLine
CREATE (p:Paper {key: csvLine.key, title: csvLine.title, year: csvLine.year})
WITH csvLine as csvLine, p as p
MATCH(v:Volume{key: csvLine.crossref})
CREATE (p)-[r:PUBLISHED_IN]->(v)


# Create Author for Journal's Paper
LOAD CSV WITH HEADERS FROM "file:///author(paper).csv" AS csvLine
CREATE (e:Author {name: csvLine.author})
WITH csvLine as csvLine, e as e
MATCH (p:Paper {key: csvLine.key})
CREATE (e)-[:WRITES]->(p)

# Create Workshop
LOAD CSV FROM "file:///Workshops.csv" AS csvLine
CREATE (c:Workshop {name: csvLine[0]})

# Create Edition nodes first and then link the edges between Workshop and Editions
LOAD CSV WITH HEADERS FROM "file:///Edition(Workshop).csv" AS csvLine
CREATE (e:Edition {key: csvLine.key, year: csvLine.year})
with csvLine as csvLine, e as e
MATCH (c:Workshop {name: csvLine.booktitle})
CREATE (c)-[:HAS]->(e)

# Create Proceeding nodes first and then link the edges between Proceeding and Conference/Workshop
LOAD CSV WITH HEADERS FROM "file:///Proceedings.csv" AS csvLine
CREATE (p:Proceeding {key: csvLine.key, name: csvLine.name, isbn: csvLine.isbn})
with csvLine as csvLine, p as p
MATCH (w:Workshop {name: csvLine.booktitle})
CREATE (w)-[:HAS]->(p)
with csvLine as csvLine, p as p
MATCH (c:Conference {name: csvLine.booktitle})
CREATE (c)-[:HAS]->(p)

