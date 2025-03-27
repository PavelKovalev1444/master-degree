import pandas as pd
from random import randint


genres_dataframe = pd.read_csv('../genres.csv')
initial_dataset = pd.read_csv('../dataset.csv')

values = genres_dataframe['name'].values

initial_dataset['genre'] = [
    values[randint(0, len(values) - 1)] for i in range(len(initial_dataset))
]

print(initial_dataset.head())
