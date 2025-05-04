import pandas as pd
from neo4j import GraphDatabase


def connect_to_db():
    db = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test4test"))
    
    return db


def load_data(db, df):

    nodes_dict = df.to_dict()

    nodes_dict = {
        'title': ['Totally Killer', "Guy Ritchie's The Covenant"],
        'Movie_Rating': [4.3, 4.7],
        'No_of_Ratings': [323, 13268],
        'Format': ['Prime Video', 'Prime Video'],
        'ReleaseYear': [2023.0, 2023.0],
        'MPAA_Rating': ['R', 'R'],
        'Directed_By': ['Nahnatchka Khan', 'Guy Ritchie'],
        'Starring': ['Kiernan Shipka,Olivia Holt,Julie Bowen', 'Jake Gyllenhaal,Dar Salim,Antony Starr']
    }

    with db.session() as session:
        # Создаем Cypher-запрос для создания узлов и отношений
        query = """
        UNWIND $nodes AS node
        MERGE (n:Person {title: node['title'], movieRating: node['Movie_Rating'], noOfRatings: node['No_of_Ratings'], format: node['Format'], releaseYear: node['ReleaseYear'], mpaa: node['MPAA_Rating'], directed: node['Directed_By'], starring: node['Starring']})
        SET n += node
        """

        # Выполняем запрос
        session.run(query, nodes=nodes_dict)


def prepare_data():
    df = pd.read_csv('./dataset.csv')
    return df


def bfs(db, start_node, k):

    with db.session() as session:
        query = """
        MATCH path=(n)-[*..{k}]-(endNode)
        WHERE n.title = $start_node
        RETURN collect(distinct endNode) as nodes
        """

        result = session.run(query, start_node=start_node, k=k)
        
        record = result.single()
        if record:
            return record["nodes"]
        else:
            return []


if __name__ == '__main__':
    db = connect_to_db()
    df = prepare_data()

    df = df.drop(columns=['Unnamed: 0', 'Price'])

    load_data(db, df.iloc[0:2])

    nodes = bfs(db, 'Totally Killer', 2)
    print(nodes)
    
    db.close()
# создание узлов
[
    618.9533574581146,
    682.3567340373993,
    727.5540714263916,
    596.3527095317841,
    718.8691051006317,
    561.1915802955627,
    519.7289509773254,
    721.138733625412,
    759.4077451229095,
    744.8832304477692
]

#content
[
    1.169731855392456,
    0.22544121742248535,
    0.2531750202178955,
    0.20768189430236816,
    0.22305655479431152,
    0.19933795928955078,
    0.25249171257019043,
    0.18299555778503418,
    0.2908647060394287,
    0.3927896022796631,
]

# collab
[
    37.48155379295349,
    36.38484334945679,
    37.76167845726013,
    35.45934796333313,
    35.46225333213806,
    35.15385174751282,
    37.139806270599365,
    35.10145688056946,
    35.11531949043274,
    38.515789270401
]