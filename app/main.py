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
    relations = data_preparator.get_relations()

    db = db_connector.connect_to_db()

    core = Core()

    core.load_csvs(db, movies, directors, relations)

    movie_description = {
        'title': args['title'],
        'director': args['director'],
        'price': args['price'],
        'starring': args['starring']
    }

    res = core.get_recommendation(db, movie_description)

    db_connector.close_connection()


if __name__ == '__main__':

    main()
