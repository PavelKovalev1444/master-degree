from core.Core import Core
from db.DatabaseCient import DatabaseClient
from preparations.DataPreparator import DataPreparator
import argparse

parser = argparse.ArgumentParser()
    
parser.add_argument("--title", action ="store", dest='title', required=True, help="""Название фильма""")
parser.add_argument("--director", action ="store", dest='director', required=True, help="""Режиссер""")
parser.add_argument("--price", action ="store", dest='price', required=True, help="""Цена""")
parser.add_argument("--starring", action ="store", dest='starring', required=True, help="""Актерский состав""")

args = parser.parse_args()

def main():
    db_connector = DatabaseClient()
    data_preparator = DataPreparator('./dataset.csv', True)
    
    movies = data_preparator.get_movies()
    directors = data_preparator.get_directors()
    users = data_preparator.get_users('./users.csv')
    genres = data_preparator.get_genres('./genres.csv')
    
    director_relations = data_preparator.get_director_relations()
    user_relations = data_preparator.get_user_relation()
    genre_relations = data_preparator.get_genre_relation()

    db = db_connector.connect_to_db()

    core = Core()

    core.load_csvs(db, movies, directors, users, genres)
    core.load_relations(db, director_relations, user_relations, genre_relations)

    recommendation_meta_info = {
        'title': args['title'],
        'director': args['director'],
        'price': args['price'],
        'starring': args['starring']
    }

    rec1 = core.get_content_based_recommendation(db, recommendation_meta_info)
    rec2 = core.get_collaborative_filtering_recommendation(db, recommendation_meta_info)

    db_connector.close_connection()


if __name__ == '__main__':

    main()
