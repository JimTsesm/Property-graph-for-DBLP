# Create Review nodes first and then link the edges between Review and Reviewers
LOAD CSV WITH HEADERS FROM "file:///Reviews.csv" AS csvLine
CREATE (r:Review {content: csvLine.content, sug_decision: csv.decision})
with csvLine as csvLine, r as r
MATCH (vr:Reviewer {source_id: csvLine.id})
MATCH (p:Paper{key: csvLine.paper})
CREATE (vr)-[:SENDS]->(r), (r)-[:FOR]->(p)

# Create Universities 
LOAD CSV WITH HEADERS FROM "file:///Universities.csv" AS csvLine
CREATE(u:University {name: csvLine.name, country: csvLine.country})

#Randomly affiliate Authors with Universities
MATCH (p1:Author), (p2:University)
WITH p1, p2
WHERE rand() < 0.05
MERGE (p1)-[:AFFILIATED_TO]->(p2)