import json
from typing import Union
from datetime import datetime, timedelta
from database.db_connection import DatabaseConnection
from constants.Database_objects import COLLECTION_NAME_PPLANNINGS

class ProjectPlanning_repository:

    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.collection = db_connection.get_collection(COLLECTION_NAME_PPLANNINGS)
    
    def get_all_pplaning(self) -> Union[list, str]:
        response: object
        try:
            response = list(self.collection.find())
        except Exception as e:
            response = json.loads({"Erro": str(e)})
        return response

    def get_pplanning_for_id(self, id: str):
        response: dict
        try:
            response = json.dumps(self.collection.find_one({'planning_id': id}), indent=3)
        except Exception as e:
            response = json.dumps({"Erro": str(e)})
        return response
    
    def get_all_pplanning_with_month_worked(self):
        response: dict
        try:
            first_day = datetime(datetime.now().year, datetime.now().month, 1)
            response = list(self.collection.find({'date_start': {'$lt': first_day}}))
        except Exception as e:
            response = json.dumps({"Erro": str(e)})
        return response

    def get_pplanning_in_current_month(self):
        response: dict
        try:
            first_day = datetime(datetime.now().year, datetime.now().month, 1)
            last_day = datetime(datetime.now().year, datetime.now().month + 1, 1) - timedelta(days=1)
            response = list(self.collection.find({'date_start': {'$gte': first_day, '$lte': last_day}}))
        except Exception as e:
            response = json.dumps({"Erro": str(e)})
        return response