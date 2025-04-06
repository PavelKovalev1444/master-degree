import pandas as pd
import uuid
from random import randint


def create_genres_dataframe():
    genres_dataframe = pd.read_csv('../genres.csv')
    initial_dataset = pd.read_csv('../initial_dataset.csv')

    values = genres_dataframe['name'].values

    genres_column = []

    for i in range(len(initial_dataset)):
        genres_count = randint(1, 3)
        genres_string_arr = []
        for j in range(genres_count):
            genres_string_arr.append(values[randint(0, len(values) - 1)])
        genres_column.append(",".join(genres_string_arr))

    initial_dataset['genre'] = genres_column

    initial_dataset.to_csv('../dataset.csv', index=False)


def create_users_dataframe():
    people_dataframe = pd.read_csv('../people.csv')

    people_dataframe = people_dataframe[['Name']]
    people_dataframe.columns = ['Name']

    users_list = []
    i = 0

    for item in people_dataframe['Name'].fillna('').str.split(','):
        if isinstance(item, list):
            for el in item:
                users_list.append({'id': str(i), 'name': el.strip()})
                i += 1

    users_dataframe = pd.DataFrame(users_list, columns=['id', 'name'])

    users_dataframe.to_csv('../users.csv', index=False)


def add_user_column_to_movie():
    people_dataframe = pd.read_csv('../users.csv')
    people_ids = people_dataframe['id'].values

    dataset = pd.read_csv('../dataset.csv')
    dataset['rated_by'] = [
        people_ids[randint(0, len(people_ids) - 1)] for i in range(len(dataset))
    ]

    dataset.to_csv('../dataset.csv', index=False)


create_genres_dataframe()
create_users_dataframe()
add_user_column_to_movie()
