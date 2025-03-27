import pandas as pd
import uuid
from random import randint


def create_users_dataframe():
    people_dataframe = pd.read_csv('../people.csv')

    people_dataframe = people_dataframe[['Name']]
    people_dataframe.columns = ['Name']

    users_list = []

    for item in people_dataframe['Name'].fillna('').str.split(','):
        if isinstance(item, list):
            for el in item:
                id = uuid.uuid1()
                users_list.append({'id': id, 'name': el.strip()})

    users_dataframe = pd.DataFrame(users_list, columns=['id', 'name'])

    users_dataframe.to_csv('../tmp/users.csv', index=False)


def add_user_column_to_movie():
    people_dataframe = pd.read_csv('../tmp/users.csv')
    people_ids = people_dataframe['id'].values

    dataset = pd.read_csv('../dataset.csv')
    dataset['rated_by'] = [
        people_ids[randint(0, len(people_ids) - 1)] for i in range(len(dataset))
    ]

    dataset.to_csv('../dataset.csv', index=False)


create_users_dataframe()
add_user_column_to_movie()
