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
