# Create Conference
LOAD CSV FROM "file:///Conferences.csv" AS csvLine
CREATE (c:Conference {name: csvLine[0]})

# Create Edition nodes first and then link edges between Conferences and Editions
LOAD CSV WITH HEADERS FROM "file:///Edition.csv" AS csvLine
CREATE (e:Edition {key: csvLine.key, year: csvLine.year})
with csvLine as csvLine, e as e
MATCH (c:Conference {name: csvLine.booktitle})
CREATE (c)-[:HAS]->(e)

# Create Paper for Edition and then link edges between Paper and Edition
LOAD CSV WITH HEADERS FROM "file:///Paper(Edition).csv" AS csvLine
CREATE (p:Paper {key: csvLine.key, title: csvLine.title, year: csvLine.year})
with csvLine as csvLine, p as p
MATCH (e:Edition {key: csvLine.crossref})
CREATE (p)-[:PUBLISHED_IN]->(e)

# Create Authors
LOAD CSV FROM "file:///authors.csv" AS csvLine
CREATE (e:Author {name: csvLine[0]})

# Create the edges between Author and Paper
LOAD CSV WITH HEADERS FROM "file:///Edition_Paper_Author.csv" AS csvLine
MATCH (p:Paper {key: csvLine.key}), (a:Author {name: csvLine.author})
CREATE (a)-[:WRITES]->(p)

# Create citations
LOAD CSV WITH HEADERS FROM "file:///Cite.csv" AS csvLine
Create (c:Citation {source_id: csvLine.New_ID})
with csvLine as csvLine, c as c
MATCH (p:Paper {key: csvLine.key})
CREATE (p)-[:CITED_BY]->(c)

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
MATCH (p:Paper {key: csvLine.key}),(e:Author {name: csvLine.author})
CREATE (e)-[:WRITES]->(p)

# Create Workshop	
LOAD CSV FROM "file:///Workshops.csv" AS csvLine	
CREATE (c:Workshop {name: csvLine[0]})	

# Create Edition nodes first and then link the edges between Workshop and Editions	
LOAD CSV WITH HEADERS FROM "file:///Edition(Workshop).csv" AS csvLine	
CREATE (e:Edition {key: csvLine.key, year: csvLine.year})	
WITH csvLine as csvLine, e as e	
MATCH (c:Workshop {name: csvLine.booktitle})	
CREATE (c)-[:HAS]->(e)	

# Create Proceeding nodes first and then link the edges between Proceeding and Conference/Workshop	
LOAD CSV WITH HEADERS FROM "file:///Proceedings.csv" AS csvLine	
CREATE (p:Proceeding {key: csvLine.key, name: csvLine.name, isbn: csvLine.isbn})	
WITH csvLine as csvLine, p as p	
MATCH (w:Workshop {name: csvLine.booktitle})	
CREATE (w)-[:HAS]->(p)	
with csvLine as csvLine, p as p	
MATCH (c:Conference {name: csvLine.booktitle})	
CREATE (c)-[:HAS]->(p)


# Create Reviewer, Reviews, and link the edges between Author, Reviewer and Paper, Reviewer
LOAD CSV WITH HEADERS FROM "file:///paper_reviewer.csv" as csvLine
CREATE (rv: Reviewer{ name: csvLine.reviewer}
CREATE (rev: Review { paper: csvLine.paper, reviewer: csvLine.reviewer})
WITH csvLine as csvLine, rv as rv, rev as rev
MATCH (p: Paper{ key: csvLine.paper })
MATCH (a: Author{ name: csvLine.reviewer })
CREATE (rv)-[:IS_A]->(a)
CREATE (rv)-[:SENDS]->(rev)
CREATE (rev)-[:FOR]->(p)

# Create Keywords
CREATE(k:Keyword {name: 'data management'})
CREATE(k2:Keyword {name: 'indexing'})
CREATE(k3:Keyword {name: 'data modeling'})
CREATE(k4:Keyword {name: 'big data'})
CREATE(k5:Keyword {name: 'data processing'})
CREATE(k6:Keyword {name: 'data storage'})
CREATE(k7:Keyword {name: 'data querying'})
CREATE(k8:Keyword {name: 'computer vision'})
CREATE(k9:Keyword {name: 'keyword1'})
CREATE(k10:Keyword {name: 'keyword2'})
CREATE(k11:Keyword {name: 'keyword3'})
CREATE(k12:Keyword {name: 'keyword4'})

# Link Paper(Edition) to Keyword
LOAD CSV WITH HEADERS FROM "file:///Keyword.csv" as csvLine
MATCH (p: Paper{ key: csvLine.key }), (k1: Keyword{ name: csvLine.keyword1 }), (k2: Keyword{ name: csvLine.keyword2 })
CREATE (p)-[:CONTAINS]->(k1)
CREATE (p)-[:CONTAINS]->(k2)

# Link Paper(Volume) to Keyword
LOAD CSV WITH HEADERS FROM "file:///Keyword(journal).csv" as csvLine
MATCH (p: Paper{ key: csvLine.key }), (k1: Keyword{ name: csvLine.keyword1 }), (k2: Keyword{ name: csvLine.keyword2 })
CREATE (p)-[:CONTAINS]->(k1)
CREATE (p)-[:CONTAINS]->(k2)

# Add conference attribute to Citation
LOAD CSV FROM "file:///cite_conf.csv" as csvLine
MATCH (c:Citation{source_id: csvLine[0]})<-[:CITED_BY]-(p:Paper)<-[:HAS_PAPER]-(:dbCommCJ)
SET c.confjour=csvLine[1]