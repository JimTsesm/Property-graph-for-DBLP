// Find the database community
MATCH(k:Keyword)
WHERE k.name in ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage',  'data querying']
// Find all the conferences/journals that are related to the database community
WITH collect(k.name) as db_community
MATCH(k:Keyword)<-[:CONTAINS]-(p:Paper)-[:PUBLISHED_IN]->(e)<-[:HAS]-(c)
WHERE k.name in db_community AND ANY(lbl IN ["Edition", "Volume"] WHERE lbl IN LABELS(e)) AND ANY(lbl IN ["Conference", "Journal"] WHERE lbl IN LABELS(c))
WITH c.name as conference, count(distinct p) as number_of_db_community_papers
MATCH(k:Keyword)<-[:CONTAINS]-(p:Paper)-[:PUBLISHED_IN]->(e)<-[:HAS]-(c)
WHERE ANY(lbl IN ["Edition", "Volume"] WHERE lbl IN LABELS(e)) AND ANY(lbl IN ["Conference", "Journal"] WHERE lbl IN LABELS(c))
WITH conference as confjour, LABELS(c)[0] as type, number_of_db_community_papers as number_of_db_community_papers , c.name as confjour2, count(distinct p) as number_of_papers
WHERE confjour2 = confjour and number_of_db_community_papers * 1.0 / number_of_papers >= 0.9
WITH confjour as confjour, type as type, number_of_db_community_papers as number_of_db_community_papers, number_of_papers as number_of_papers, number_of_db_community_papers * 1.0 / number_of_papers as percentage
CREATE (cj:dbCommCJ{ name:confjour, type:type, n_dbPapers: number_of_db_community_papers,n_papers:number_of_papers, percentage: percentage }) 
WITH cj as cj
MATCH (c)-[:HAS]->(e)<-[:PUBLISHED_IN]-(p:Paper)
WHERE c.name = cj.name and ANY(lbl IN ["Edition", "Volume"] WHERE lbl IN LABELS(e)) AND ANY(lbl IN ["Conference", "Journal"] WHERE lbl IN LABELS(c))
CREATE UNIQUE (cj)-[:HAS_PAPER]->(p)
CREATE UNIQUE (c)-[:IS_A]->(cj)
RETURN DISTINCT cj.name AS conf_journal, cj.type AS type, cj.n_dbPapers As number_of_db_community_papers, cj.n_papers AS number_of_papers, cj.percentage AS percentage
ORDER BY percentage DESC
// ==================================================================

// ###############################################################

MATCH (cj:dbCommCJ)
WITH  collect(cj.name) as dbCommunity
MATCH (cj:dbCommCJ)-[:HAS_PAPER]->(p:Paper)
OPTIONAL MATCH (p)-[:CITED_BY]->(c:Citation)
WHERE c.confjour in dbCommunity
WITH  p as p,cj.name as conference_journal, cj.type as type, count(c) as citations
ORDER BY citations DESC Limit 100
CREATE (tp: TopPaper{n_citations: citations})
CREATE (p)-[:IS_A]->(tp)
RETURN p.title as paper, conference_journal, type, citations
// ###############################################################

CREATE (x:DbReviewer)
CREATE (y:Guru)
WITH x as x, y as y
MATCH (a:Author)-[:WRITES]->(p:Paper)-[:IS_A]->(tp:TopPaper)
WITH a, count(p) as cnt, x as x, y as y
WITH a as a, CASE WHEN cnt>1 THEN 'YES' ELSE 'NO' END as IS_GURU, x as x, y as y
ORDER BY IS_GURU DESC
CREATE (a)-[:IS_A]->(x)
WITH IS_GURU as IS_GURU, a as a, y as y
WHERE IS_GURU = 'YES'
CREATE (a)-[:IS_A]->(y)

MATCH (a:Author)-[:WRITES]->(p:Paper)-[:IS_A]->(tp:TopPaper)
WITH a, count(p) as cnt
WITH a.name as TopAuthors, CASE WHEN cnt>1 THEN 'YES' ELSE 'NO' END as IS_GURU
RETURN TopAuthors, IS_GURU
ORDER BY IS_GURU DESC