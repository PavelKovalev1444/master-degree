# MATCH (n) DETACH DELETE n

from core.Core import Core
from db.DatabaseCient import DatabaseClient
from preparations.DataPreparator import DataPreparator


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
        'title': 'Totally Killer',
        'director': 'Nahnatchka Khan'
    }

    res = core.get_recommendation(db, movie_description)

    db_connector.close_connection()


if __name__ == '__main__':

    main()
