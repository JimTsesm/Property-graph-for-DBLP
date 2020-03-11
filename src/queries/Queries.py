import py2neo

class RunQueries():


    def __init__(self):
        self.graph = py2neo.Graph()

    # Find the h-indexes of the authors in your graph
    def find_hindex(self):
        query = """MATCH(a:Author)-[:WRITES]->(p:Paper)-[:CITED_BY]->(c:Citation)
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
                    RETURN authors.name, max(hindex)"""

        return self.graph.run(query).data()

    #def most_cited_papers(self):

    # For each conference find its community: i.e., those authors that have published papers
    # on that conference in, at least, 4 different editions.
    def find_communities(self):
        query = """MATCH(c:Conference)-[:HAS]->(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)<-[w:WRITES]-(a:Author)
                    WITH c as conference, a as author, count(distinct e) as number_of_editions
                    WHERE number_of_editions >= 4
                    RETURN conference.name as Conference, collect(author.name) as Community"""

        return self.graph.run(query).data()

    #def find_impact_factor(self):

if __name__ == '__main__':
    rq = RunQueries()
    print(rq.find_hindex())
    print(rq.find_communities())
