# Calculate h-index for every Author
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