from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from clients.DataverseClient import Dataverse
from constants.logger_messages import ETL_DATAVERSE_LOGGER_RUN
from database.db_connection import DatabaseConnection
from database.repositories.ProjectPlanning_repository import ProjectPlanning_repository
from etl.etl_dataverse import Etl_dataverse
from shared.logger import info

@flow(task_runner=ConcurrentTaskRunner())
def dataverse_flow():
    # Parameters
    info(ETL_DATAVERSE_LOGGER_RUN)
    db_conn = DatabaseConnection()
    db_conn.connect()
    dataverse_client = Dataverse()
    pplanning_repository = ProjectPlanning_repository(db_conn)

    # ETL Process
    etl = Etl_dataverse(dataverse_client, pplanning_repository)

    # Extract
    extract_data = etl.extract()
    # Transform
    transformed_data = etl.transform(extract_data)
    # Load
    etl.load(transformed_data)