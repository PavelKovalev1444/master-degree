from neo4j import GraphDatabase

from serializer.DataSerializer import DataSerializer


class DatabaseClient(object):


    def __init__(self):
        pass


    def connect_to_db(self):
        self.db = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test4test"))
        return self.db


    def close_connection(self):
        self.db.close()
