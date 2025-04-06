import pandas as pd


class DataPreparator(object):


    def __init__(self, path: str, enable_saving_to_csv: bool):
        self.df = pd.read_csv(path)
        self.enable_saving_to_csv = enable_saving_to_csv


    def get_movies(self) -> pd.DataFrame:
        # нормально переименовать колонки
        self.movies_df = self.df[[
            'title', 
            # 'Movie_Rating', 
            # 'No_of_Ratings', 
            'Format', 
            'ReleaseYear', 
            'MPAA_Rating', 
            # 'Directed_By', 
            # 'Starring', 
            'Price',
            'genre'
        ]]

        self.movies_df['id'] = [i for i in range(len(self.movies_df))]

        if self.enable_saving_to_csv:
            self.movies_df.to_csv('./tmp/nodes_movies.csv', index=False)

        return self.movies_df


    def get_directors(self) -> pd.DataFrame:
        directors_df = self.df[['Directed_By']]

        directors_list = []
        i = 0

        for directors in directors_df['Directed_By'].fillna('').str.split(','):
            if isinstance(directors, list):
                for director in directors:
                    directors_list.append({'id': i,'name': director.strip()})
                    i += 1
        
        self.directors_list_df = pd.DataFrame(directors_list)
        
        if self.enable_saving_to_csv:
            self.directors_list_df.to_csv('./tmp/nodes_directors.csv', index=False)

        return self.directors_list_df
    

    def get_genres(self, path: str) -> pd.DataFrame:
        self.genres_df = pd.read_csv(path)

        self.genres_df['id'] = [i for i in range(len(self.genres_df))]

        return self.genres_df
    
    def get_users(self, path: str) -> pd.DataFrame:
        self.users_df = pd.read_csv(path)

        self.users_df['id'] = [i for i in range(len(self.users_df))]

        return self.users_df


    def get_director_relations(self) -> pd.DataFrame:
        relations = []
        for idx, row in self.df.iterrows():
            if pd.notna(row['Directed_By']):
                for director in row['Directed_By'].split(','):
                    relations.append({
                        'title': row['title'],
                        'director': director.strip(),
                    })

        relations_df = pd.DataFrame(relations)

        if self.enable_saving_to_csv:
            relations_df.to_csv('./tmp/director_relations.csv', index=False)

        return relations_df


    def get_genre_relation(self):
        relations = []
        for idx, row in self.df.iterrows():
            if pd.notna(row['genre']):
                for genre in row['genre'].split(','):
                    relations.append({
                        'title': row['title'],
                        'genre': genre.strip()
                    })

        relations_df = pd.DataFrame(relations)

        if self.enable_saving_to_csv:
            relations_df.to_csv('./tmp/genre_relations.csv', index=False)

        return relations_df
    

    def get_user_relation(self):
        relations = []
        for idx, row in self.df.iterrows():
            if pd.notna(row['rated_by']):
                # for user in row['rated_by'].split(','):
                relations.append({
                    'title': row['title'],
                    'userId': row['rated_by']
                })

        relations_df = pd.DataFrame(relations)

        if self.enable_saving_to_csv:
            relations_df.to_csv('./tmp/user_relations.csv', index=False)

        return relations_df

