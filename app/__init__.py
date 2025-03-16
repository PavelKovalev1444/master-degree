import pandas as pd
from neo4j import GraphDatabase


def connect_to_db():
    db = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test4test"))
    return db


def load_data():
    df = pd.read_csv('./dataset.csv')
    return df


def get_movies_df(df):
    movies_df = df[[
        'title', 
        # 'Movie_Rating', 
        # 'No_of_Ratings', 
        'Format', 
        'ReleaseYear', 
        'MPAA_Rating', 
        # 'Directed_By', 
        # 'Starring', 
        'Price'
    ]]

    movies_df.columns = [
        'title', 
        # 'Movie_Rating',
        # 'No_of_Ratings',
        'Format',
        'ReleaseYear',
        'MPAA_Rating',
        # 'Directed_By',
        # 'Starring',
        'Price'
    ]

    movies_df.to_csv('./tmp/nodes_movies.csv', index=False)

    return movies_df


def get_directors_df(df):
    directors_df = df[['Directed_By']]

    directors_df.columns = ['Directed_By']

    directors_list = []
    
    for directors in directors_df['Directed_By'].fillna('').str.split(','):
        if isinstance(directors, list):
            for director in directors:
                directors_list.append({'name': director.strip()})
    
    directors_list_df = pd.DataFrame(directors_list)
    
    directors_list_df.to_csv('./tmp/nodes_directors.csv', index=False)

    return directors_list_df


def get_relations_df(df):
    relations = []
    for idx, row in df.iterrows():
        if pd.notna(row['Directed_By']):
            for director in row['Directed_By'].split(','):
                relations.append({
                    'title': row['title'],
                    'director': director.strip()
                })

    relations_df = pd.DataFrame(relations)

    relations_df.to_csv('./tmp/relations.csv', index=False)

    return relations_df


def load_csvs(db, movies_df, directors_df, relations_df):

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


def serialize_movie(movie):
    return {
        "format": movie["format"],
        "price": movie["price"],
        "releaseYear": movie["releaseYear"],
        "title": movie["title"],
        "mpaa_rating": movie["mpaa_rating"],
    }


def get_recommendation(db, movie_description):

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

        return [serialize_movie(record["movie"]) for record in result]
    
# MATCH (n) DETACH DELETE n

if __name__ == '__main__':
    db = connect_to_db()
    df = load_data()

    movies_df = get_movies_df(df)
    directors_df = get_directors_df(df)
    relations_df = get_relations_df(df)

    load_csvs(db, movies_df, directors_df, relations_df)

    movie_description = {
        'title': 'Totally Killer',
        'director': 'Nahnatchka Khan'
    }
    recs = get_recommendation(db, movie_description)
    print(recs)

    db.close()
