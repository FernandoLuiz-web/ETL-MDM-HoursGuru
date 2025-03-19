import json
from typing import Union
from database.db_connection import DatabaseConnection
from constants.Database_objects import COLLECTION_NAME_PPLANNINGS
from shared.logger import logger

class ProjectPlanning_repository:
    __GET_ALL_PPLANNING_LOGGER_MSG = "Requisição para obter todos os planejamentos de projetos."
    __INSERT_NEW_PPLANNINGS_LOGGER_MSG = "Inserindo novos registros de planejamento no banco de dados."
    __DELETE_ALL_PPLANNING_LOGGER_MSG = "Deletando todos os planejamentos de projetos do banco de dados."

    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.collection = db_connection.get_collection(COLLECTION_NAME_PPLANNINGS)
    
    @logger(__GET_ALL_PPLANNING_LOGGER_MSG)
    def get_all_pplaning(self) -> Union[list, str]:
        response: object
        try:
            response = list(self.collection.find())
        except Exception as e:
            response = json.loads({"Erro": str(e)})
        return response
    
    @logger(__INSERT_NEW_PPLANNINGS_LOGGER_MSG)
    def insert_new_plannings(self, plannings: dict):
        response: dict
        try:
            self.collection.insert_many(plannings)
            response = {"Message": "Registros inseridos!"}
        except Exception as e:
            response = {"Erro": str(e)}
        return json.dumps(response)
    
    @logger(__DELETE_ALL_PPLANNING_LOGGER_MSG)
    def delete_plannings(self):
        response: dict
        try:
            self.collection.delete_many({})
            response = {"Message": "Registros excluidos!"}
        except Exception as e:
            response = {"Erro": str(e)}
        return json.dumps(response)