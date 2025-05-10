from datetime import datetime, timedelta
from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from clients.ClockifyClient import Clockify
from constants.logger_messages import ETL_CLOCKIFY_LOGGER_RUN
from database.db_connection import DatabaseConnection
from database.repositories.AppointedHours_repository import AppointedHours_repository
from logic.etl_clockify import Etl_clockify
from shared.logger import info

def data_runs() -> list[str]:
    format_date = "%Y-%m-%d"
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    first_day_of_previous_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)
    return [(first_day_of_previous_month + timedelta(days=i)).strftime(format_date) 
            for i in range((first_day_of_current_month - first_day_of_previous_month).days)]

@flow(task_runner=ConcurrentTaskRunner())
def clockify_flow():
    # Parameters
    info(ETL_CLOCKIFY_LOGGER_RUN)
    db_conn = DatabaseConnection()
    db_conn.connect()
    clockify_client = Clockify() 
    appointed_repository = AppointedHours_repository(db_conn)

    # ETL Process
    for date in data_runs():
        info(f"Date: {date}")
        etl_clockify = Etl_clockify(clockify_client, appointed_repository, date)
        # Extract
        extract_data = etl_clockify.extract()
        # Transform
        transformed_data = etl_clockify.transform(extract_data)
        # Load
        etl_clockify.load(transformed_data)