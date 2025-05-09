import json
from typing import Union
from database.db_connection import DatabaseConnection
from constants.Database_objects import COLLECTION_NAME_PPLANNINGS
from shared.logger import info, error

class ProjectPlanning_repository:
    __GET_ALL_PPLANNING_LOGGER_MSG = "Requisição para obter todos os planejamentos de projetos."
    __INSERT_NEW_PPLANNINGS_LOGGER_MSG = "Inserindo novos registros de planejamento no banco de dados."
    __DELETE_ALL_PPLANNING_LOGGER_MSG = "Deletando todos os planejamentos de projetos do banco de dados."

    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.collection = db_connection.get_collection(COLLECTION_NAME_PPLANNINGS)
    
    def get_all_pplaning(self) -> Union[list, str]:
        info(self.__GET_ALL_PPLANNING_LOGGER_MSG)
        response: object
        try:
            response = list(self.collection.find())
        except Exception as e:
            error(f"Erro ao obter todos os planejamentos: {e}")
            response = json.loads({"Erro": str(e)})
        return response
    
    def insert_new_plannings(self, plannings: dict):
        info(self.__INSERT_NEW_PPLANNINGS_LOGGER_MSG)
        response: dict
        try:
            self.collection.insert_many(plannings)
            response = {"Message": "Registros inseridos!"}
        except Exception as e:
            error(f"Erro ao inserir novos planejamentos: {e}")
            response = {"Erro": str(e)}
        return json.dumps(response)
    
    def delete_plannings(self):
        info(self.__DELETE_ALL_PPLANNING_LOGGER_MSG)
        response: dict
        try:
            self.collection.delete_many({})
            response = {"Message": "Registros excluidos!"}
        except Exception as e:
            error(f"Erro ao deletar planejamentos: {e}")
            response = {"Erro": str(e)}
        return json.dumps(response)