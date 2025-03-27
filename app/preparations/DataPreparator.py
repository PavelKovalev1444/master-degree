import pandas as pd


class DataPreparator(object):


    def __init__(self, path: str, enable_saving_to_csv: bool):
        self.df = pd.read_csv(path)
        self.enable_saving_to_csv = enable_saving_to_csv


    def get_movies(self) -> pd.DataFrame:
        self.movies_df = self.df[[
            'title', 
            # 'Movie_Rating', 
            # 'No_of_Ratings', 
            'Format', 
            'ReleaseYear', 
            'MPAA_Rating', 
            # 'Directed_By', 
            # 'Starring', 
            'Price'
        ]]

        self.movies_df.columns = [
            'title', 
            # 'Movie_Rating',
            # 'No_of_Ratings',
            'Format',
            'ReleaseYear',
            'MPAA_Rating',
            # 'Directed_By',
            # 'Starring',
            'Price'
        ]

        if self.enable_saving_to_csv:
            self.movies_df.to_csv('./tmp/nodes_movies.csv', index=False)

        return self.movies_df


    def get_directors(self) -> pd.DataFrame:
        directors_df = self.df[['Directed_By']]

        directors_df.columns = ['Directed_By']

        directors_list = []
        
        for directors in directors_df['Directed_By'].fillna('').str.split(','):
            if isinstance(directors, list):
                for director in directors:
                    directors_list.append({'name': director.strip()})
        
        self.directors_list_df = pd.DataFrame(directors_list)
        
        if self.enable_saving_to_csv:
            self.directors_list_df.to_csv('./tmp/nodes_directors.csv', index=False)

        return self.directors_list_df


    def get_relations(self) -> pd.DataFrame:
        relations = []
        for idx, row in self.df.iterrows():
            if pd.notna(row['Directed_By']):
                for director in row['Directed_By'].split(','):
                    relations.append({
                        'title': row['title'],
                        'director': director.strip()
                    })

        relations_df = pd.DataFrame(relations)

        if self.enable_saving_to_csv:
            relations_df.to_csv('./tmp/relations.csv', index=False)

        return relations_df
