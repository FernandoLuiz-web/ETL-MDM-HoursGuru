from datetime import datetime, timedelta
from database.AppointedHours_repository import AppointedHours_repository
from database.ProjectPlanning_repository import ProjectPlanning_repository
from database.db_connection import DatabaseConnection
from clients.ClockifyClient import Clockify
from clients.DataverseClient import Dataverse
from etl.etl_clockify import Etl_clockify
from etl.etl_dataverse import Etl_dataverse

def data_runs(start: str) -> list[str]:
    format_date = "%Y-%m-%d"
    start_date = datetime.strptime(start, format_date)
    today = datetime.today()

    return [(start_date + timedelta(days=i)).strftime(format_date) for i in range((today - start_date).days + 1)]

def main():
    datas = data_runs("2025-01-22")
    db_conn = DatabaseConnection()

    db_conn.connect()

    clockify_client = Clockify()
    dataverse_client = Dataverse()

    appointed_repository = AppointedHours_repository(db_conn)
    pplanning_repository = ProjectPlanning_repository(db_conn)

    for data in datas:
        print(f"Dia {data}")
        etl_clockify = Etl_clockify(clockify_client, appointed_repository, str(data))
        etl_clockify.run()

    etl_pplanning = Etl_dataverse(dataverse_client, pplanning_repository)
    etl_pplanning.run()

    db_conn.close()

if __name__ == "__main__":
    main()