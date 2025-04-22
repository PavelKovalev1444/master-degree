from neo4j import GraphDatabase

from serializer.DataSerializer import DataSerializer


class DatabaseClient(object):


    def __init__(self, verbose):
        self.verbose = verbose
        self.db = None
        pass


    def connect_to_db(self):
        try:
            if self.verbose:
                print("Trying to connect to db.")
            self.db = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test4test"))
        except:
            print("Cannot establish connection with db.")
        else:
            if self.verbose:
                print("Has connected to db.")

        return self.db


    def close_connection(self):
        if self.verbose:
            print("Trying to close db connection.")

        try:
            self.db.close()
        except:
            print("Cannot close connection to db.")
        else:
            if self.verbose:
                print("Has closed conntection to db.")

