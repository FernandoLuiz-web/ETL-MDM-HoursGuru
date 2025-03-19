import os
import requests
import pandas as pd
from shared.logger import logger
from dotenv import load_dotenv
from pandas import json_normalize
from constants.endpoints import CLOCKIFY_API, CLOCKIFY_REPORTS
from constants.logger_messages import (
    CLOCKIFY_LOGGER_PROJECTS, 
    CLOCKIFY_LOGGER_ACTIVE_USERS, 
    CLOCKIFY_LOGGER_APPOINTMENTS
)

load_dotenv()

class Clockify:
    def __init__(self):
        self.__PAGE_SIZE = "page-size=5000"
        self.__HEADERS = {
            "X-Api-Key": os.getenv("CLOCKIFY_API_KEY"),
            "Content-Type": "application/json"
        }

    @property
    def headers(self) -> dict:
        return self.__HEADERS

    @logger(CLOCKIFY_LOGGER_PROJECTS)
    def get_workspace_active_projects(self) -> pd.DataFrame:
        """Obtém projetos do Clockify e retorna como DataFrame."""
        columns = ['id', 'name', 'clientId', 'clientName', 'billable']
        url = f"{CLOCKIFY_API}/projects?{self.__PAGE_SIZE}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        projects = pd.DataFrame(response.json())
        projects = projects.loc[(~projects['archived']) & 
                                (projects['name'].str.contains('Modernization')) & 
                                (~projects['name'].str.contains('Academy'))]
        return projects[columns]

    @logger(CLOCKIFY_LOGGER_ACTIVE_USERS)
    def get_workspace_users(self) -> pd.DataFrame:
        """Obtém os usuários do workspace do Clockify."""
        url = f"{CLOCKIFY_API}/users?page-size=5000"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return pd.DataFrame(response.json())

    @logger(CLOCKIFY_LOGGER_APPOINTMENTS)
    def get_reports_detailed(self, project_id: list, dateStart: str, dateEnd: str):
        """Gera um relatório detalhado de time entries por projetos."""
        dateStart = f"{dateStart}T00:00:00.000Z"
        dateEnd = f"{dateEnd}T23:59:59.999Z"
        if isinstance(project_id, pd.Series):
            project_id = project_id.tolist()

        url = f"{CLOCKIFY_REPORTS}/reports/detailed"
        body = {
            "dateRangeStart": dateStart,
            "dateRangeEnd": dateEnd,
            "sortOrder": "DESCENDING",
            "description": "",
            "rounding": False,
            "withoutDescription": False,
            "amounts": ["EARNED"],
            "amountShown": "EARNED",
            "zoomLevel": "MONTH",
            "userLocale": "pt-BR",
            "customFields": None,
            "userCustomFields": None,
            "kioskIds": [],
            "projects": {
                "contains": "CONTAINS",
                "ids": project_id,
                "numberOfDeleted": 0
            },
            "detailedFilter": {
                "sortColumn": "DATE",
                "page": 1,
                "pageSize": 1000,
                "auditFilter": None,
                "quickbooksSelectType": "ALL",
                "options": {"totals": "CALCULATE"}
            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=body)
            response.raise_for_status()
            return json_normalize(response.json()['timeentries'])
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            return pd.DataFrame()