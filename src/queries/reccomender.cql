// Find database community
MATCH(k:Keyword)
WHERE k.name in ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage',  'data querying']
// Find all the conferences that are related to the database community
// 1)TO CHECK IF WE CAN COMPUTE THE 2 WITHs WITH 1 A UNION, BECAUSE NOW WE COMPUTE A CARTESIAN
// PRODUCT
// 2)LOAD Keyword FOR JOURNALS' PAPERS
// 3)SEE IF THE JOURNALS' PAPERS CAN BE FOUND WITH THE CONFERENCES' ONES
WITH collect(k.name) as db_community
MATCH(k:Keyword)<-[:CONTAINS]-(p:Paper)-[:PUBLISHED_IN]->(e:Edition)<-[:HAS]-(c:Conference)
WHERE k.name in db_community
WITH c.name as conference, count(distinct p) as number_of_db_community_papers
MATCH(k:Keyword)<-[:CONTAINS]-(p:Paper)-[:PUBLISHED_IN]->(e:Edition)<-[:HAS]-(c:Conference)
WITH conference as conference, number_of_db_community_papers as number_of_db_community_papers , c.name as conference2, count(distinct p) as number_of_papers
WHERE conference2 = conference and number_of_db_community_papers * 1.0 / number_of_papers >= 0.9
RETURN conference, number_of_db_community_papers, conference2, number_of_papers, number_of_db_community_papers * 1.0 / number_of_papers as percentage

// =========================== TO UPDATE PREVIOUS WITH THIS ============================
MATCH(k:Keyword)
WHERE k.name in ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage',  'data querying']
// Find all the conferences that are related to the database community
// 1)TO CHECK IF WE CAN COMPUTE THE 2 WITHs WITH 1 A UNION, BECAUSE NOW WE COMPUTE A CARTESIAN
// PRODUCT
// 2)LOAD Keyword FOR JOURNALS' PAPERS
// 3)SEE IF THE JOURNALS' PAPERS CAN BE FOUND WITH THE CONFERENCES' ONES
WITH collect(k.name) as db_community
MATCH(k:Keyword)<-[:CONTAINS]-(p:Paper)-[:PUBLISHED_IN]->(e:Edition)<-[:HAS]-(c:Conference)
WHERE k.name in db_community
WITH c.name as conference, count(distinct p) as number_of_db_community_papers
MATCH(k:Keyword)<-[:CONTAINS]-(p:Paper)-[:PUBLISHED_IN]->(e:Edition)<-[:HAS]-(c:Conference)
WITH conference as confjour, 'Conference' as type, number_of_db_community_papers as number_of_db_community_papers , c.name as confjour2, count(distinct p) as number_of_papers
WHERE confjour2 = confjour and number_of_db_community_papers * 1.0 / number_of_papers >= 0.9
WITH confjour as confjour, type as type, number_of_db_community_papers as number_of_db_community_papers, number_of_papers as number_of_papers, number_of_db_community_papers * 1.0 / number_of_papers as percentage
CREATE (cj:dbCommCJ{ name:confjour, type:type, n_dbPapers: number_of_db_community_papers,n_papers:number_of_papers, percentage: percentage }) 
WITH cj as cj
MATCH (c:Conference {name: cj.name})-[:HAS]->(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)
CREATE UNIQUE (cj)-[:HAS_PAPER]->(p)
CREATE UNIQUE (c)-[:IS_A]->(cj)
RETURN DISTINCT cj.name AS conf_journal, cj.type AS type, cj.n_dbPapers As number_of_db_community_papers, cj.n_papers AS number_of_papers, cj.percentage AS percentage
ORDER BY percentage DESC
// ==================================================================


UNION

// Find database community
MATCH(k:Keyword)
WHERE k.name in ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage',  'data querying']
// Find all the journals that are related to the database community
WITH collect(k.name) as db_community
MATCH(k:Keyword)<-[:CONTAINS]-(p:Paper)-[:PUBLISHED_IN]->(e:Volume)<-[:HAS]-(c:Journal)
WHERE k.name in db_community
WITH c.name as conference, count(distinct p) as number_of_db_community_papers
MATCH(k:Keyword)<-[:CONTAINS]-(p:Paper)-[:PUBLISHED_IN]->(e:Volume)<-[:HAS]-(c:Journal)
WITH conference as conference, number_of_db_community_papers as number_of_db_community_papers , c.name as conference2, count(distinct p) as number_of_papers
WHERE conference2 = conference and number_of_db_community_papers * 1.0 / number_of_papers >= 0.9

RETURN conference, number_of_db_community_papers, conference2, number_of_papers, number_of_db_community_papers * 1.0 / number_of_papers as percentage
// ###############################################################

MATCH (cj:dbCommCJ)
WITH  collect(cj.name) as dbCommunity
MATCH (cj:dbCommCJ)-[:HAS_PAPER]->(p:Paper)
OPTIONAL MATCH (p)-[:CITED_BY]->(c:Citation)
// WHERE c.confjour in dbCommunity
WITH  p as p,cj.name as conference_journal, cj.type as type, count(c) as citations
ORDER BY citations DESC Limit 10
CREATE (tp: TopPaper{n_citations: citations})
CREATE (p)-[:IS_A]->(tp)
RETURN p.title as paper, conference_journal, type, citations
// ###############################################################

MATCH (a:Author)-[:WRITES]->(p:Paper)-[:IS_A]->(tp:TopPaper)
WITH a, count(p) as cnt
RETURN a.name as Author, CASE WHEN cnt>1 THEN 'YES' ELSE 'NO' END as IS_GURU
ORDER BY IS_GURU DESC