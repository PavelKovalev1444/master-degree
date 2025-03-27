import pandas as pd
import uuid 

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
