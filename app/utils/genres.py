import pandas as pd
from random import randint


def create_genres_dataframe():
    genres_dataframe = pd.read_csv('../genres.csv')
    initial_dataset = pd.read_csv('../initial_dataset.csv')

    values = genres_dataframe['name'].values

    initial_dataset['genre'] = [
        values[randint(0, len(values) - 1)] for i in range(len(initial_dataset))
    ]

    initial_dataset.to_csv('../dataset.csv', index=False)


create_genres_dataframe()
