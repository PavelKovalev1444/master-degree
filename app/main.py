from core.Core import Core
from db.DatabaseCient import DatabaseClient
from preparations.DataPreparator import DataPreparator
import argparse

parser = argparse.ArgumentParser()
    
parser.add_argument("--user", 
                    action ="store", 
                    dest='user', 
                    required=True, 
                    help="""Пользователь, для которого составляются рекомендации."""
)
parser.add_argument("--save", 
                    action ="store", 
                    dest='save', 
                    default=False, 
                    required=False, 
                    help="""Флаг сохранения промежуточных результатов в .csv файлы. Если True, то промежуточные результаты будут сохранены в .csv файлы."""
)
parser.add_argument("--result", 
                    action ="store", 
                    dest='result', 
                    default="", 
                    required=False, 
                    help="""Название .csv файла для сохранения результата. Если задано, то результат сохранится в него."""
)
parser.add_argument("--filter", 
                    action ="store", 
                    dest='filter', 
                    default=1, 
                    required=False, 
                    help="""Тип фильтрации, которую модель должна осуществить. Возможные варианты: 1 - по содержимому, 2 - коллаборативная."""
)
parser.add_argument("--limit", 
                    action ="store", 
                    dest='limit', 
                    default=5, 
                    required=False, 
                    help="""Лимит рекомендованных записей, который будет выведен/сохранен. По умолчанию равен 5."""
)
parser.add_argument("--verbose", 
                    action ="store", 
                    dest='verbose', 
                    default=False, 
                    required=False, 
                    help="""Вывод дополнительной информации о текущем этапе работы. По умолчанию отключен."""
)


args = parser.parse_args()

def main():
    save_data = args.save
    result_file_name = args.result
    filter_type = args.filter
    limit = args.limit
    verbose = args.verbose == 'True'

    db_connector = DatabaseClient(verbose)
    data_preparator = DataPreparator('./dataset.csv', save_data, verbose)
    
    movies = data_preparator.get_movies()
    directors = data_preparator.get_directors()
    users = data_preparator.get_users('./users.csv')
    genres = data_preparator.get_genres('./genres.csv')
    
    director_relations = data_preparator.get_director_relations()
    user_relations = data_preparator.get_user_relation()
    genre_relations = data_preparator.get_genre_relation()

    db = db_connector.connect_to_db()

    core = Core(verbose)

    # core.load_csvs(db, movies, directors, users, genres)
    # core.load_relations(db, director_relations, user_relations, genre_relations)
    recommendation_meta_info = {
        'user': args.user
    }

    if int(filter_type) == 1:
        content_recs = core.get_content_based_recommendation(db, recommendation_meta_info)
        if not result_file_name:
            print("Content-based filtering result:")
            for i in range(0, min(limit, len(content_recs))):
                print(content_recs[i])
        else:
            data_preparator.save_data_to_file(f'content_based_{result_file_name}', content_recs[0:int(limit)])
        
    if int(filter_type) == 2:
        collab_recs = core.get_collaborative_filtering_recommendation(db, recommendation_meta_info)

        if not result_file_name:
            print("Collaborative filtering result:")
            for i in range(0, min(limit, len(collab_recs))):
                print(collab_recs[i])
        else:
            data_preparator.save_data_to_file(f'collaborative_{result_file_name}', content_recs[0:int(limit)])

    db_connector.close_connection()
    
    return 0


if __name__ == '__main__':

    main()
