import json
from typing import Union
from database.db_connection import DatabaseConnection
from constants.Database_objects import COLLECTION_NAME_PPLANNINGS
from constants.logger_messages import (
    GET_ALL_PPLANNING_LOGGER_MSG,
    GET_PPLANNING_FOR_ID_LOGGER_MSG,
    INSERT_NEW_PPLANNINGS_LOGGER_MSG,
    DELETE_ALL_PPLANNING_LOGGER_MSG
)
from shared.logger import info, error

class ProjectPlanning_repository:
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.collection = db_connection.get_collection(COLLECTION_NAME_PPLANNINGS)
    
    def get_all_pplaning(self) -> Union[list, str]:
        info(GET_ALL_PPLANNING_LOGGER_MSG)
        response: object
        try:
            response = list(self.collection.find())
        except Exception as e:
            error(f"Erro ao obter todos os planejamentos: {e}")
            response = json.loads({"Erro": str(e)})
        return response
    
    def get_pplanning_for_id(self, id: str):
        info(GET_PPLANNING_FOR_ID_LOGGER_MSG)
        response: dict
        try:
            response = json.dumps(self.collection.find_one({'planning_id': id}), indent=3)
        except Exception as e:
            response = json.dumps({"Erro": str(e)})
        return response
    
    def insert_new_plannings(self, plannings: dict):
        info(INSERT_NEW_PPLANNINGS_LOGGER_MSG)
        response: dict
        try:
            self.collection.insert_many(plannings)
            response = {"Message": "Registros inseridos!"}
        except Exception as e:
            error(f"Erro ao inserir novos planejamentos: {e}")
            response = {"Erro": str(e)}
        return json.dumps(response)
    
    def delete_plannings(self):
        info(DELETE_ALL_PPLANNING_LOGGER_MSG)
        response: dict
        try:
            self.collection.delete_many({})
            response = {"Message": "Registros excluidos!"}
        except Exception as e:
            error(f"Erro ao deletar planejamentos: {e}")
            response = {"Erro": str(e)}
        return json.dumps(response)