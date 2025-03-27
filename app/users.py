import pandas as pd
import uuid 

data = [uuid.uuid1() for i in range(100)]

users_dataframe = pd.DataFrame(data, columns=['userId'])

users_dataframe.to_csv('./users.csv', index=False)

main_df = pd.read_csv('./dataset.csv')

main_df