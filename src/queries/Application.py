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
    
    # Find the top 3 most cited papers of each conference
    def most_cited_papers(self):
        query = """MATCH(c:Conference)-[:HAS]-(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)-[:CITED_BY]-(t:Citation)
                    WITH c as c, p as p, count(t) as cites
                    ORDER BY c.name ASC, cites DESC
                    RETURN c.name as Conference, collect(p.title)[..3] as Most3CitedPapers"""
        return self.graph.run(query).data()

    # For each conference find its community: i.e., those authors that have published papers
    # on that conference in, at least, 4 different editions.
    def find_communities(self):
        query = """MATCH(c:Conference)-[:HAS]->(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)<-[w:WRITES]-(a:Author)
                    WITH c as conference, a as author, count(distinct e) as number_of_editions
                    WHERE number_of_editions >= 4
                    RETURN conference.name as Conference, collect(author.name) as Community"""

        return self.graph.run(query).data()

    # Find the impact factors of the journals in your graph
    def find_impact_factor(self):
        query = """Match(j:Journal)
                    OPTIONAL MATCH (j)-[:HAS]->(v:Volume)
                    OPTIONAL MATCH (v)<-[:PUBLISHED_IN]-(p:Paper)
                    OPTIONAL MATCH (p)-[:CITED_BY]->(c:Citation)
                    WHERE p.year >= '2018' and p.year <= '2019'
                    RETURN j.name as Journal,  count(DISTINCT p) as Papers18_19, count(c) as Citations2019, count(c)*1.0/count(DISTINCT p) as IMPACT_FACTOR
                    ORDER BY IMPACT_FACTOR DESC"""
        return self.graph.run(query).data()

    def pageRank(self):
        query = """CALL algo.pageRank.stream(
                    'MATCH (n:Paper) RETURN id(n) AS id UNION MATCH (n:Citation) RETURN id(n) AS id',
                    'MATCH (n:Paper)-[:CITED_BY]->(m:Citation) RETURN id(m) AS source, id(n) AS target',
                    {graph: 'cypher'})
                    YIELD nodeId, score
                    RETURN algo.asNode(nodeId).key AS page,score
                    ORDER BY score DESC"""
        return self.graph.run(query).data()
        
    def betweenness(self):
        query = """CALL algo.betweenness.stream(
                    'MATCH (p) WHERE ANY(lbl IN ["Paper", "Keyword", "Topic"] WHERE lbl IN LABELS(p)) RETURN id(p) as id', 
                    'MATCH (p1)-[r]->(p2) WHERE TYPE(r) IN ["RELATED_TO", "CONTAINS"] RETURN id(p1) as source,id(p2) as target', 
                    {concurrency:4, graph:'cypher'})
                YIELD nodeId, centrality
                WITH nodeId AS nodeId, centrality AS centrality
                MATCH (k:Keyword) WHERE id(k) = nodeId
                RETURN algo.asNode(nodeId).name, centrality
                ORDER BY centrality desc"""
        return self.graph.run(query).data()

    def reccomender(self):
        database_community = """MATCH(k:Keyword)
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
                                ORDER BY percentage DESC"""
        
        self.graph.run(database_community)
        
        top_papers = """MATCH (cj:dbCommCJ)
                        WITH  collect(cj.name) as dbCommunity
                        MATCH (cj:dbCommCJ)-[:HAS_PAPER]->(p:Paper)
                        OPTIONAL MATCH (p)-[:CITED_BY]->(c:Citation)
                        // WHERE c.confjour in dbCommunity
                        WITH  p as p,cj.name as conference_journal, cj.type as type, count(c) as citations
                        ORDER BY citations DESC Limit 10
                        CREATE (tp: TopPaper{n_citations: citations})
                        CREATE (p)-[:IS_A]->(tp)
                        RETURN p.title as paper, conference_journal, type, citations"""
        self.graph.run(top_papers)
        
        top_reviewers = """"""
        
        self.graph.run(top_reviewers).data()

if __name__ == '__main__':
    rq = RunQueries()
    
    # Run the queries
    print("Hindex")
    print(rq.find_hindex())
    print("Most cited papers")
    print(rq.most_cited_papers())
    print("Communities")
    print(rq.find_communities())
    print("Impact factor")
    print(rq.find_impact_factor())

    # Run the algorithms
    print("Page rank")
    print(rq.pageRank())
    print("Betweenness centrality")
    print(rq.betweenness())
    
    # Run the reccomender
    print("Reccomender")
    print(rq.reccomender())