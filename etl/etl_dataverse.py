import pandas as pd
import pandera as pa
from pandera import Check, Column
from pandera.engines import pandas_engine
from shared.logger import info, error
from clients.DataverseClient import Dataverse
from database.repositories.ProjectPlanning_repository import ProjectPlanning_repository
from constants.logger_messages import (
    ETL_DATAVERSE_LOGGER_EXTRACT,
    ETL_DATAVERSE_LOGGER_TRANSFORM,
    ETL_DATAVERSE_LOGGER_LOAD
)
from prefect import task
from prefect.cache_policies import NO_CACHE

class Etl_dataverse():
    def __init__(self, dataverse_client: Dataverse, pplanning_repository: ProjectPlanning_repository):
        self.dataverse_client =  dataverse_client
        self.pplanning_repository =  pplanning_repository

    @task(cache_policy=NO_CACHE)
    def extract(self) -> pd.DataFrame:
        info(ETL_DATAVERSE_LOGGER_EXTRACT)
        return self.dataverse_client.get_project_plannings_df()
    
    @task(cache_policy=NO_CACHE)
    def transform(self, extract: pd.DataFrame) -> pd.DataFrame:
        info(ETL_DATAVERSE_LOGGER_TRANSFORM)
        extract['date_start'] = pd.to_datetime(extract['date_start']).dt.tz_convert(None)
        extract['date_end'] = pd.to_datetime(extract['date_end']).dt.tz_convert(None)
        return extract
    
    @task(cache_policy=NO_CACHE)
    def load(self, transformed_datas: pd.DataFrame):
        info(ETL_DATAVERSE_LOGGER_LOAD)
        promised_schema = pa.DataFrameSchema({
            "project_id": Column(str, nullable=False),
            "date_start": Column(pandas_engine.DateTime(), nullable=False),
            "date_end": Column(pandas_engine.DateTime(), nullable=False),
            "contracted_hours": Column(float, checks=Check(lambda x: x >= 0), nullable = False),
            "used_hours": Column(float, checks=Check(lambda x: x >= 0), nullable = False),
            "remaining_hours": Column(float, checks=Check(lambda x: x>= 0), nullable = False)
        })
        try:
            promised_schema.validate(transformed_datas)
            records =  transformed_datas.to_dict(orient='records')
            records_on_database = [doc['planning_id'] for doc in self.pplanning_repository.get_all_pplaning()]
            new_records_for_database = [record for record in records if record['planning_id'] not in records_on_database]
            info(new_records_for_database)
            self.pplanning_repository.insert_new_plannings(new_records_for_database)
        except pa.errors.SchemaError as se:
            error(f"Erro na validação do schema dos dados transformados {se}")
