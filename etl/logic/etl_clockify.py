import pandas as pd
import pandera as pa
from pandera import Check, Column
from pandera.engines import pandas_engine
from shared.logger import info, error
from clients.ClockifyClient import Clockify
from database.repositories.AppointedHours_repository import AppointedHours_repository
from constants.logger_messages import (
    ETL_CLOCKIFY_LOGGER_GET_ACTIVE_PROJECTS_ID,
    ETL_CLOCKIFY_LOGGER_GET_APPOINTMENTS_PER_DAY,
    ETL_CLOCKIFY_LOGGER_EXTRACT,
    ETL_CLOCKIFY_LOGGER_TRANSFORM,
    ETL_CLOCKIFY_LOGGER_LOAD
    )
from prefect import task
from prefect.cache_policies import NO_CACHE

class Etl_clockify:
    _FUSO = "America/Sao_Paulo"
    def __init__(self, clockify_client: Clockify, appointed_repository: AppointedHours_repository, processing_day: str):
        self.clockify_client = clockify_client
        self.appointed_repository = appointed_repository
        self.processing_day = processing_day

    def _get_active_projects_id_from_clockify(self) -> pd.DataFrame:
        info(ETL_CLOCKIFY_LOGGER_GET_ACTIVE_PROJECTS_ID)
        try:
            active_projects = self.clockify_client.get_workspace_active_projects()
        except Exception as e:
            error(f"Erro ao obter lista de projetos: {e}")
            return pd.DataFrame()
        return pd.DataFrame(active_projects)['id']
    
    def _get_appointments_for_day(self, active_projects_id: list) -> pd.DataFrame:
        info(ETL_CLOCKIFY_LOGGER_GET_APPOINTMENTS_PER_DAY)
        try:
            appointments = self.clockify_client.get_reports_detailed(active_projects_id,self.processing_day, self.processing_day)
        except Exception as e:
            error(f"Erro ao obter lista de apontamentos: {e}")
            return pd.DataFrame()
        return pd.DataFrame(appointments)

    @task(cache_policy=NO_CACHE)
    def extract(self) -> pd.DataFrame:
        info(ETL_CLOCKIFY_LOGGER_EXTRACT)
        extract_active_projects = self._get_active_projects_id_from_clockify()
        extract_appointments_per_daily = self._get_appointments_for_day(extract_active_projects)
        return extract_appointments_per_daily
    
    @task(cache_policy=NO_CACHE)
    def transform(self, extract: pd.DataFrame) -> pd.DataFrame:
        info(ETL_CLOCKIFY_LOGGER_TRANSFORM)
        columns = ['userId','projectId', '_id', 'timeInterval.start', 'timeInterval.end', 'timeInterval.duration']
        rename_columns = {
            'userId': 'user',
            'projectId': 'project',
            '_id': 'entry_id',
            'timeInterval.start': 'start_at',
            'timeInterval.end': 'end_at',
            'timeInterval.duration': 'worked_hours'
        }
        if extract.empty: return pd.DataFrame()
        extract = extract[columns].copy()
        extract['timeInterval.duration'] = round(extract['timeInterval.duration']/3600,2)
        extract['timeInterval.start'] = pd.to_datetime(extract['timeInterval.start'], errors="coerce").dt.tz_convert(self._FUSO)
        extract['timeInterval.end'] = pd.to_datetime(extract['timeInterval.end'], errors="coerce").dt.tz_convert(self._FUSO)
        extract.rename(columns=rename_columns, inplace=True)
        return extract

    @task(cache_policy=NO_CACHE)
    def load(self, transformed_datas: pd.DataFrame):
        promised_schema = pa.DataFrameSchema({
            "user": Column(str, nullable=False),
            "project": Column(str, nullable=False),
            "entry_id": Column(str, nullable=False),
            "start_at": Column(pandas_engine.DateTime(tz=self._FUSO), nullable=False),
            "end_at": Column(pandas_engine.DateTime(tz=self._FUSO), nullable=False),
            "worked_hours": Column(float, checks=Check(lambda x: x >= 0), nullable=False)
        })
        info(ETL_CLOCKIFY_LOGGER_LOAD)
        try:
            promised_schema.validate(transformed_datas)
            records = transformed_datas.to_dict(orient='records')
            records_on_database = [doc['entry_id'] for doc in self.appointed_repository.get_all_appointed_hours()]
            new_records_for_database = [record for record in records if record['entry_id'] not in records_on_database]
            self.appointed_repository.insert_new_appointment(new_records_for_database)
        except pa.errors.SchemaError as se:
            error(f"schema validation failed: {se} para data {self.processing_day}")