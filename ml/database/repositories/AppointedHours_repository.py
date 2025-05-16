import json
from bson import ObjectId
from typing import Union
from database.db_connection import DatabaseConnection
from constants.Database_objects import COLLECTION_NAME_APPOINTMENTS

class AppointedHours_repository:
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection =  db_connection
        self.collection = self.db_connection.get_collection(COLLECTION_NAME_APPOINTMENTS)
    
    def get_all_appointed_hours(self) -> Union[list, str]:
        response: object
        try:
            response = list(self.collection.find())
        except Exception as e:
            response = json.dumps({"Erro": str(e)})
        return response
    
    def get_appointed_for_id(self, id: str):
        response: dict
        try:
            response = json.dumps(self.collection.find_one({'_id': ObjectId(id)}), indent=3, default = str)
        except Exception as e:
            response = json.dumps({"Erro": str(e)})
        return response