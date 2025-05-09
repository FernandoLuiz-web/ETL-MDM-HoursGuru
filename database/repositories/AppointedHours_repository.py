import json
from typing import Union
from bson import ObjectId
from database.db_connection import DatabaseConnection
from constants.Database_objects import COLLECTION_NAME_APPOINTMENTS
from shared.logger import info, error

class AppointedHours_repository:
    __GET_ALL_APPOINTED_HOURS_LOGGER_MSG = "Requisição para obter todos os apontamentos de horas."
    __GET_APPOINTED_FOR_ID_LOGGER_MSG = "Requisição para obter um apontamento de horas por id."
    __INSERT_NEW_APPOINTMENT_LOGGER_MSG = "Inserindo registros de apontamento no banco de dados."
    __DELETE_APPOINTMENT_FOR_ID_LOGGER_MSG = "Deletando registro de um apontamento no banco de dados."
    __DELETE_ALL_APPOINTMENTS_IN_DOCUMENT = "Deletando todos os registros de apontamento no banco de dados."
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.collection = self.db_connection.get_collection(COLLECTION_NAME_APPOINTMENTS)

    def get_all_appointed_hours(self) -> Union[list, str]:
        info(self.__GET_ALL_APPOINTED_HOURS_LOGGER_MSG)
        response: object
        try:
            response = list(self.collection.find())
        except Exception as e:
            error(f"Erro ao obter todos os apontamentos de horas: {e}")
            response = json.dumps({"Erro": str(e)})
        return response
    
    def get_appointed_for_id(self, id: str):
        info(self.__GET_APPOINTED_FOR_ID_LOGGER_MSG)
        response: dict
        try:
            response = json.dumps(self.collection.find_one({'_id': ObjectId(id)}), indent=3, default = str)
        except Exception as e:
            error(f"Erro ao obter apontamento de horas por id: {e}")
            response = json.dumps({"Erro": str(e)})
        return response
    
    def insert_new_appointment(self, appointments: dict):
        info(self.__INSERT_NEW_APPOINTMENT_LOGGER_MSG)
        response: dict
        try:
            self.collection.insert_many(appointments)
            response = {"Message": "Registros inseridos com sucesso!"}
        except Exception as e:
            error(f"Erro ao inserir novos apontamentos: {e}")
            response = {"Erro": str(e)}
        return json.dumps(response)
    
    def delete_appointment_for_id(self, id: str):
        info(self.__DELETE_APPOINTMENT_FOR_ID_LOGGER_MSG)
        response: dict
        try:
            self.collection.delete_one({'_id': ObjectId(id)})
            response = {"Message": f"Registro {id} deletado com sucesso!"}
        except Exception as e:
            error(f"Erro ao deletar apontamento de horas por id: {e}")
            response = {"Erro": str(e)}
        return json.dumps(response)
    
    def delete_all_appointments_in_document(self):
        info(self.__DELETE_ALL_APPOINTMENTS_IN_DOCUMENT)
        response: dict
        try:
            self.collection.delete_many({})
            response = {"Message": f"Registros deletados!"}
        except Exception as e:
            error(f"Erro ao deletar todos os apontamentos de horas: {e}")
            response = {"Erro": str(e)}
        return json.dumps(response)