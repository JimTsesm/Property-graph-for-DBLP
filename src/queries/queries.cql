// Find the h-indexes of the authors in your graph
MATCH(a:Author)-[:WRITES]->(p:Paper)-[:CITED_BY]->(c:Citation)
WITH a as authors, p.key as papers, count(c) as number_of_citations
ORDER BY number_of_citations DESC
WITH authors as authors, collect(number_of_citations) as citations_list
WITH authors as authors, citations_list AS citations_list
UNWIND range(0,size(citations_list)-1) as l_index
WITH authors as authors, 
CASE 
	WHEN citations_list[l_index] >= l_index+1 THEN l_index+1
    ELSE -1
END AS hindex
WHERE hindex <> -1
RETURN authors.name, max(hindex)

// For each conference find its community: i.e., those authors that have published papers
// on that conference in, at least, 4 different editions.
MATCH(c:Conference)-[:HAS]->(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)<-[w:WRITES]-(a:Author)
WITH c as conference, a as author, count(distinct e) as number_of_editions
WHERE number_of_editions >= 4
RETURN conference.name as Conference, collect(author.name) as Community


// Find the top 3 most cited papers of each conference.
MATCH(c:Conference)-[:HAS]-(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)-[:CITED_BY]-(t:Citation)
WITH c as c, p as p, count(t) as cites
ORDER BY c.name ASC, cites DESC
RETURN c.name as Conference, collect(p.title)[..3] as Most3CitedPapers


// Find the impact factors of the journals in your graph
Match(j:Journal)
OPTIONAL MATCH (j)-[:HAS]->(v:Volume)
OPTIONAL MATCH (v)<-[:PUBLISHED_IN]-(p:Paper)
OPTIONAL MATCH (p)-[:CITED_BY]->(c:Citation)
WHERE p.year >= '2018' and p.year <= '2019'
RETURN j.name as Journal,  count(DISTINCT p) as Papers18_19, count(c) as Citations2019, count(c)*1.0/count(DISTINCT p) as IMPACT_FACTOR
ORDER BY IMPACT_FACTOR DESC

// Page rank algorithm
CALL algo.pageRank.stream(
  'MATCH (n:Paper) RETURN id(n) AS id UNION MATCH (n:Citation) RETURN id(n) AS id',
  'MATCH (n:Paper)-[:CITED_BY]->(m:Citation) RETURN id(m) AS source, id(n) AS target',
  {graph: 'cypher'})
YIELD nodeId, score
RETURN algo.asNode(nodeId).key AS page,score
ORDER BY score DESC