from serializer.DataSerializer import DataSerializer

import math


SIMILARITY_LIMIT = 0.5


class Core(object):


    def __init__(self, verbose):
        self.serializer = DataSerializer()
        self.verbose = verbose
        pass


    def load_movies(self, db, movies_df):
        iters = len(movies_df)

        if self.verbose:
            print("Started creating movies nodes in db. Please, wait.")

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
        
        if self.verbose:
            print("Finished creating movies nodes in db.")
    
    
    def load_directors(self, db, directors_df):
        iters = len(directors_df)

        if self.verbose:
            print("Started creating directors nodes in db. Please, wait.")

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

        if self.verbose:
            print("Finished creating directors nodes in db.")


    def load_users(self, db, users_df):
        iters = len(users_df)

        if self.verbose:
            print("Started creating users nodes in db. Please, wait.")

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

        if self.verbose:
            print("Finished creating users nodes in db.")


    def load_genres(self, db, genres_df):
        iters = len(genres_df)

        if self.verbose:
            print("Started creating genres nodes in db. Please, wait.")

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

        if self.verbose:
            print("Finished creating genres nodes in db.")


    def load_csvs(self, db, movies_df, directors_df, users_df, genres_df):
        self.load_movies(db, movies_df)
        self.load_directors(db, directors_df)
        self.load_users(db, users_df)
        self.load_genres(db, genres_df)

    
    def load_director_relations(self, db, relations_df):
        iters = len(relations_df)

        if self.verbose:
            print("Started creating directors relations in db. Please, wait.")

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

        if self.verbose:
            print("Finished creating directors relations in db.")

    
    def load_user_relations(self, db, relations_df):
        iters = len(relations_df)

        if self.verbose:
            print("Started creating users relations in db. Please, wait.")

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

        if self.verbose:
            print("Finished creating users relations in db.")


    def load_genre_relations(self, db, relations_df):
        iters = len(relations_df)

        if self.verbose:
            print("Started creating genres relations in db. Please, wait.")

        for i in range(iters):
            nodes_dict = relations_df.iloc[i].fillna(0).to_dict()
            with db.session() as session:
                query = """
                    UNWIND $nodes AS node
                    MATCH (m:Movie { title: node.title })
                    MATCH (g:Genre { name: node.genre })
                    CREATE (m)-[:ASSOCIATED_WITH {accuracy: 1.0} ]->(g)
                    RETURN m, g
                """
                session.run(query, nodes=nodes_dict)

        if self.verbose:
            print("Finished creating genres relations in db.")

    
    def load_relations(self, db, director_relations_df, user_relations_df, genre_relations_df):
        self.load_director_relations(db, director_relations_df)
        self.load_user_relations(db, user_relations_df)
        self.load_genre_relations(db, genre_relations_df)

    
    def similar(
        self,
        left_classifiers,
        left_features,
        right_classifiers,
        right_features,
    ) -> float:
        left_size = len(left_classifiers)
        right_size = len(right_classifiers)
        
        if left_size == 0 or right_size == 0:
            return 0.0
        
        if left_size != len(left_features):
            raise ValueError(f"Длина left_classifiers ({left_size}) должна быть равна длине left_features ({len(left_features)})")
            
        if right_size != len(right_features):
            raise ValueError(f"Длина right_classifiers ({right_size}) должна быть равна длине right_features ({len(right_features)})")

        union_classifier_set = set(left_classifiers)
        union_classifier_set.update(right_classifiers)
        union_classifiers = list(union_classifier_set)

        left_vector = self.get_features_vector(left_classifiers, left_features, union_classifiers)
        right_vector = self.get_features_vector(right_classifiers, right_features, union_classifiers)

        result = self.cosine(left_vector, right_vector)
        return 0.0 if math.isnan(result) else result


    def cosine(self, vector_a, vector_b):
        dot_product = sum(a * b for a, b in zip(vector_a, vector_b))
        norm_a = math.sqrt(sum(x * x for x in vector_a))
        norm_b = math.sqrt(sum(x * x for x in vector_b))
        
        return dot_product / (norm_a * norm_b)

    def get_features_vector(
        self,
        classifiers,
        classifier_features,
        union_classifiers
    ):
        classifier_to_features = dict(zip(classifiers, classifier_features))
        return [
            classifier_to_features.get(classifier, 0.0)
            for classifier in union_classifiers
    ]


    def get_content_based_recommendation(self, db, recommendation_meta_info):
        user = recommendation_meta_info['user']

        if self.verbose:
            print("Started content-based filtering. Please, wait.")

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
                RETURN title, ux_classifiers, ux_features, classifiers, features, genres
            """
            result = session.run(query, user=user)

            if self.verbose:
                print("Finished fetching data from db. Calculating similarity.")

            movies = [self.serializer.serialize_content_based_raw(record) for record in result]
            processed_movies = []
            for movie in movies:
                tmp_score = self.similar(movie['ux_classifiers'], movie['ux_features'], movie['classifiers'], movie['features'])
                if tmp_score > SIMILARITY_LIMIT:
                    processed_movies.append({'title': movie['title'], 'genres': movie['genres'], 'score': tmp_score})
            
            if self.verbose:
                print("Similarity has been calculated. Sorting results.")

            return sorted(processed_movies, key=lambda movie: movie['score'], reverse=True)


    def get_collaborative_filtering_recommendation(self, db, recommendation_meta_info):
        user = recommendation_meta_info['user']

        if self.verbose:
            print("Started collaborative filtering. Please, wait.")

        with db.session() as session:
            query="""
                MATCH (u:User { name: $user })-[:HAS_WATCHED]->(m:Movie)-[r:ASSOCIATED_WITH]->(g:Genre)
                WITH u, g.id AS classifier, SUM(r.accuracy) AS total_accuracy
                WITH u, COLLECT(classifier) AS ux_classifiers, COLLECT(total_accuracy) AS ux_features
                MATCH (similar:User)-[:HAS_WATCHED]->(m:Movie)-[r:ASSOCIATED_WITH]->(g:Genre)
                WHERE similar <> u
                WITH u, similar, ux_classifiers, ux_features, g.id AS classifier, SUM(r.accuracy) AS total_feature
                WITH u AS user, similar, ux_classifiers, ux_features, COLLECT(classifier) AS classifiers, COLLECT(total_feature) AS features
                RETURN user, similar, ux_classifiers, ux_features, classifiers, features
            """
            result = session.run(query, user=user)
            records = [self.serializer.serialize_collaborative_similar_users(record) for record in result]
            
            processed_records = []
            for record in records:
                tmp_score = self.similar(record['ux_classifiers'], record['ux_features'], record['classifiers'], record['features'])
                if tmp_score > SIMILARITY_LIMIT:
                    processed_records.append({
                        'user': record['user'], 
                        'similar': record['similar'], 
                        'ux_classifiers': record['ux_classifiers'], 
                        'ux_features': record['ux_features'], 
                        'classifiers': record['classifiers'], 
                        'features': record['features'], 
                        'score': tmp_score
                    })

            query="""
                UNWIND $similars AS similar
                UNWIND $ux_classifiers_list AS ux_classifiers
                UNWIND $ux_features_list AS ux_features
                MATCH (m:Movie)-[r:ASSOCIATED_WITH]->(g:Genre)
                WHERE NOT (:User { name: $user })-[:HAS_WATCHED]->(m) AND (:User { name: similar.name })-[:HAS_WATCHED]-(m)
                WITH 
                    m.title AS title, 
                    ux_classifiers, 
                    ux_features, 
                    COLLECT(g.id) AS classifiers, 
                    COLLECT(r.accuracy) AS features,
                    COLLECT(g.name) AS genres
                RETURN title, ux_classifiers, ux_features, classifiers, features, genres
            """
            result = session.run(
                query, 
                user=user, 
                similars=[record['similar'] for record in processed_records],
                ux_classifiers_list=[record['ux_classifiers'] for record in processed_records],
                ux_features_list=[record['ux_features'] for record in processed_records]
            )

            if self.verbose:
                print("Finished fetching data from db. Calculating similarity.")

            movies = [self.serializer.serialize_collaborative(record) for record in result]
            processed_movies = []
            for movie in movies:
                tmp_score = self.similar(movie['ux_classifiers'], movie['ux_features'], movie['classifiers'], movie['features'])
                if tmp_score > SIMILARITY_LIMIT:
                    processed_movies.append({'title': movie['title'], 'genres': movie['genres'], 'score': tmp_score})

            if self.verbose:
                print("Similarity has been calculated. Sorting results.")

            return sorted(processed_movies, key=lambda movie: movie['score'], reverse=True)
