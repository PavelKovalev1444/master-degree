from neo4j import GraphDatabase

from serializer.DataSerializer import DataSerializer


class Core(object):


    def __init__(self):
        pass


    def load_csvs(self, db, movies_df, directors_df, relations_df):

        for i in range(5):
            nodes_dict = movies_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MERGE (n:Movie {title: node['title'], format: node['Format'], releaseYear: node['ReleaseYear'], mpaa_rating: node['MPAA_Rating'], price: node['Price']})
                    SET n += node
                """

                session.run(query, nodes=nodes_dict)

        for i in range(5):
            nodes_dict = directors_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MERGE (n:Director {name: node['name']})
                    SET n += node
                """

                session.run(query, nodes=nodes_dict)

        for i in range(5):
            nodes_dict = relations_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MATCH (m:Movie { title: node.title })
                    MATCH (d:Director { name: node.director })
                    CREATE (d)-[:MADE]->(m)
                    RETURN m, d
                """

                session.run(query, nodes=nodes_dict)

    def get_recommendation(self, db, movie_description) -> list:

        serializer = DataSerializer()

        title = movie_description['title']
        director = movie_description['director']

        with db.session() as session:
            query = """
                OPTIONAL MATCH (movie:Movie)<-[:MADE]-(director:Director)
                WHERE movie.title CONTAINS $title
                AND director.name CONTAINS $director
                RETURN movie
                ORDER BY movie.mpaa_rating
                LIMIT 5
            """

            result = session.run(query, title=title, director=director)

            return [serializer.serialize_movie(record["movie"]) for record in result]
