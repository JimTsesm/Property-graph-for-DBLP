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

