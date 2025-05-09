import os
from dotenv import load_dotenv
from pymongo import MongoClient
from shared.logger import info, error
from constants.logger_messages import (
    PYMONGO_LOGGER_CONNECT_HOST, 
    PYMONGO_LOGGER_CONNECT_DATABASE, 
    PYMONGO_LOGGER_GET_COLLECTION, 
    PYMONGO_LOGGER_CLOSE_CONNECTION
    )

load_dotenv()

class DatabaseConnection:
    """Classe para gerenciar a conexão com o MongoDB."""
    __EXCEPTION_COLLECTION = "Banco de dados não conectado."

    def __init__(self):
        """Inicializa a conexão com o MongoDB."""
        info(PYMONGO_LOGGER_CONNECT_HOST)
        self.uri = os.getenv("HOST")
        self.database_name = os.getenv("DATABASE")
        self.client = None
        self.db = None

    def connect(self):
        """
            Estabelece a conexão com o banco de dados.
        """
        info(PYMONGO_LOGGER_CONNECT_DATABASE)
        if not self.client:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.database_name]

    def get_collection(self, collection_name: str):
        """Obtém uma coleção do banco de dados."""
        info(PYMONGO_LOGGER_GET_COLLECTION)
        if self.db is None:
            raise ConnectionError(self.__EXCEPTION_COLLECTION)
        return self.db[collection_name]

    def close(self):
        """Fecha a conexão com o banco de dados."""
        info(PYMONGO_LOGGER_CLOSE_CONNECTION)
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
