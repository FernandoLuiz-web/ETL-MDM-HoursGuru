import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

class DatabaseConnection:
    __EXCEPTION_COLLECTION = "Banco de dados não conectado."

    def __init__(self):
        self.uri = os.getenv("HOST")
        self.database_name = os.getenv("DATABASE")
        self.client = None
        self.db = None

    def connect(self):
        """
            Estabelece a conexão com o banco de dados.
        """
        if not self.client:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.database_name]

    def get_collection(self, collection_name: str):
        """
            Obtém uma coleção do banco de dados.
        """
        if self.db is None:
            raise ConnectionError(self.__EXCEPTION_COLLECTION)
        return self.db[collection_name]

    def close(self):
        """
            Fecha a conexão com o banco de dados.
        """
        if self.client:
            self.client.close()
            self.client = None
            self.db = None