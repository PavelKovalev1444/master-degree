from neo4j import GraphDatabase

from serializer.DataSerializer import DataSerializer


class Core(object):


    def __init__(self):
        self.serializer = DataSerializer()
        pass


    def load_movies(self, db, movies_df):
        iters = len(movies_df)
        iters = 20

        for i in range(iters):
            nodes_dict = movies_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MERGE (n:Movie {
                        title: node['title'], 
                        format: node['Format'], 
                        releaseYear: node['ReleaseYear'], 
                        mpaa_rating: node['MPAA_Rating'], 
                        price: node['Price']
                    })
                    SET n += node
                """

                session.run(query, nodes=nodes_dict)
    
    
    def load_directors(self, db, directors_df):
        iters = len(directors_df)
        iters = 20

        for i in range(iters):
            nodes_dict = directors_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MERGE (n:Director {
                        name: node['name']
                    })
                    SET n += node
                """

                session.run(query, nodes=nodes_dict)


    def load_users(self, db, users_df):
        iters = len(users_df)
        iters = 20

        for i in range(iters):
            nodes_dict = users_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MERGE (n:User {
                        id: node['id'],
                        name: node['name']
                    })
                    SET n += node
                """

                session.run(query, nodes=nodes_dict)


    def load_genres(self, db, genres_df):
        iters = len(genres_df)

        for i in range(iters):
            nodes_dict = genres_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MERGE (n:Genre {
                        name: node['name']
                    })
                    SET n += node
                """

                session.run(query, nodes=nodes_dict)


    def load_csvs(self, db, movies_df, directors_df, users_df, genres_df):
        self.load_movies(db, movies_df)
        self.load_directors(db, directors_df)
        self.load_users(db, users_df)
        self.load_genres(db, genres_df)

    
    def load_director_relations(self, db, relations_df):
        iters = len(relations_df)
        iters = 20

        for i in range(iters):
            nodes_dict = relations_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MATCH (m:Movie {
                        title: node.title
                    })
                    MATCH (d:Director {
                        name: node.director
                    })
                    CREATE (d)-[:DIRECTED]->(m)
                    RETURN m, d
                """

                session.run(query, nodes=nodes_dict)

    
    def load_user_relations(self, db, relations_df):
        iters = len(relations_df)
        iters = 20

        for i in range(iters):
            nodes_dict = relations_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MATCH (m:Movie {
                        title: node.title
                    })
                    MATCH (u:User {
                        id: node.userId
                    })
                    CREATE (u)-[:HAS_WATCHED]->(m)
                    RETURN m, u
                """

                session.run(query, nodes=nodes_dict)


    def load_genre_relations(self, db, relations_df):
        iters = len(relations_df)
        iters = 20

        for i in range(iters):
            nodes_dict = relations_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MATCH (m:Movie { title: node.title })
                    MATCH (g:Genre { name: node.genre })
                    CREATE (g)-[:ASSOCIATED_WITH {accuracy: 1.0} ]->(m)
                    RETURN m, g
                """

                session.run(query, nodes=nodes_dict)

    
    def load_relations(self, db, director_relations_df, user_relations_df, genre_relations_df):
        self.load_director_relations(db, director_relations_df)
        self.load_user_relations(db, user_relations_df)
        self.load_genre_relations(db, genre_relations_df)


    def get_content_based_recommendation(self, db, recommendation_meta_info):

        user = recommendation_meta_info['user']

        with db.session() as session:
            query = """
                MATCH (u:User { name: $user })-[:HAS_WATCHED]->(m:Movie)-[r:ASSOCIATED_WITH]->(g:Genre)
                WITH u, g.id AS classifier, sum(r.accuracy) AS features
                WITH u, collect(classifier) AS ux_classifiers, collect(features) AS ux_features
                MATCH (m:Movie)-[r:ASSOCIATED_WITH]->(g:Genre)
                WHERE NOT (u)-[:HAS_WATCHED]->(m)
                WITH 
                    m.title AS title,
                    ux_classifiers,
                    ux_features,
                    collect(g.id) AS classifiers,
                    collect(r.accuracy) AS features,
                    collect(g.name) AS genres
                WITH 
                    title,
                    genres,
                    reduce(
                        total = 0,
                        i IN range(0, size(ux_classifiers)-1) |
                        total + (
                            CASE 
                                WHEN classifiers[i] = ux_classifiers[i] AND features[i] = ux_features[i] 
                                THEN 1.0
                                WHEN classifiers[i] = ux_classifiers[i] AND abs(features[i] - ux_features[i]) <= 0.1
                                THEN 0.5
                                ELSE 0.0
                            END
                        )
                    ) AS score
                WHERE score > 0.4
                RETURN title, genres, score
                ORDER BY score DESC
                LIMIT 7
            """
            result = session.run(query, user=user)
            return [self.serializer.serialize_content_based(record) for record in result]


    def get_collaborative_filtering_recommendation(self, db, recommendation_meta_info):

        user = recommendation_meta_info['user']

        with db.session() as session:
            query="""
                MATCH (u:User { name: $user })-[:HAS_WATCHED]->(m:Movie)-[r:IS_ASSOCIATED_WITH]->(g:Genre)
                WITH u, g.id AS classifier, SUM(r.accuracy) AS total_accuracy
                WITH u, COLLECT(classifier) AS ux_classifiers, COLLECT(total_accuracy) AS ux_features
                MATCH (similar:User)-[:HAS_WATCHED]->(m:Movie)-[r:IS_ASSOCIATED_WITH]->(g:Genre)
                WHERE similar <> u
                WITH u, similar, ux_classifiers, ux_features, g.id AS classifier, SUM(r.accuracy) AS total_feature
                WITH u, similar, ux_classifiers, ux_features, COLLECT(classifier) AS classifiers, COLLECT(total_feature) AS features
                WITH 
                    u, 
                    similar, 
                    reduce(
                        total = 0,
                        i IN range(0, size(ux_classifiers)-1) |
                        total + (
                            CASE 
                                WHEN classifiers[i] = ux_classifiers[i] AND abs(features[i] - ux_features[i]) <= 0.1
                                THEN 1.0
                                ELSE 0.0
                            END
                        )
                    ) AS score
                WHERE score > 0.6
                MATCH (m:Movie)-[r:IS_ASSOCIATED_WITH]->(g:Genre)
                WHERE NOT (u)-[:HAS_WATCHED]->(m) AND (similar)-[:HAS_WATCHED]-(m)
                WITH 
                    ux_classifiers, 
                    ux_features, 
                    m.title AS title, 
                    COLLECT(g.id) AS classifiers, 
                    COLLECT(r.accuracy) AS features,
                    COLLECT(g.name) AS genres
                WITH 
                    title,
                    reduce(
                        total = 0,
                        i IN range(0, size(ux_classifiers)-1) |
                        total + (
                            CASE 
                                WHEN classifiers[i] = ux_classifiers[i] AND abs(features[i] - ux_features[i]) <= 0.1
                                THEN 1.0
                                ELSE 0.0
                            END
                        )
                    ) AS score
                RETURN title, score
                ORDER BY score DESC
                LIMIT 5
            """
            result = session.run(query, user=user)
            return [self.serializer.serialize_content_based(record) for record in result]
