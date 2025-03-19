import json
from typing import Union
from bson import ObjectId
from database.db_connection import DatabaseConnection
from constants.Database_objects import COLLECTION_NAME_APPOINTMENTS
from shared.logger import logger

class AppointedHours_repository:
    __GET_ALL_APPOINTED_HOURS_LOGGER_MSG = "Requisição para obter todos os apontamentos de horas."
    __GET_APPOINTED_FOR_ID_LOGGER_MSG = "Requisição para obter um apontamento de horas por id."
    __INSERT_NEW_APPOINTMENT_LOGGER_MSG = "Inserindo registros de apontamento no banco de dados."
    __DELETE_APPOINTMENT_FOR_ID_LOGGER_MSG = "Deletando registro de um apontamento no banco de dados."
    __DELETE_ALL_APPOINTMENTS_IN_DOCUMENT = "Deletando todos os registros de apontamento no banco de dados."
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.collection = self.db_connection.get_collection(COLLECTION_NAME_APPOINTMENTS)

    @logger(__GET_ALL_APPOINTED_HOURS_LOGGER_MSG)
    def get_all_appointed_hours(self) -> Union[list, str]:
        response: object
        try:
            response = list(self.collection.find())
        except Exception as e:
            response = json.dumps({"Erro": str(e)})
        return response
    
    @logger(__GET_APPOINTED_FOR_ID_LOGGER_MSG)
    def get_appointed_for_id(self, id: str):
        response: dict
        try:
            response = json.dumps(self.collection.find_one({'_id': ObjectId(id)}), indent=3, default = str)
        except Exception as e:
            response = json.dumps({"Erro": str(e)})
        return response
    
    @logger(__INSERT_NEW_APPOINTMENT_LOGGER_MSG)
    def insert_new_appointment(self, appointments: dict):
        response: dict
        try:
            self.collection.insert_many(appointments)
            response = {"Message": "Registros inseridos com sucesso!"}
        except Exception as e:
            response = {"Erro": str(e)}
        return json.dumps(response)
    
    @logger(__DELETE_APPOINTMENT_FOR_ID_LOGGER_MSG)
    def delete_appointment_for_id(self, id: str):
        response: dict
        try:
            self.collection.delete_one({'_id': ObjectId(id)})
            response = {"Message": f"Registro {id} deletado com sucesso!"}
        except Exception as e:
            response = {"Erro": str(e)}
        return json.dumps(response)
    
    @logger(__DELETE_ALL_APPOINTMENTS_IN_DOCUMENT)
    def delete_all_appointments_in_document(self):
        response: dict
        try:
            self.collection.delete_many({})
            response = {"Message": f"Registros deletados!"}
        except Exception as e:
            response = {"Erro": str(e)}
        return json.dumps(response)