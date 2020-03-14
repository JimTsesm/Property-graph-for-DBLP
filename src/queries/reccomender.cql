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
