import requests
import pandas as pd
from shared.logger import logger
from constants.endpoints import EXPORTS_PROJECTPLANNING
from constants.logger_messages import DATAVERSE_LOGGER_PROJECTS_PLANNING

class Dataverse:
    """
    Client para interagir com a API do Dataverse.
    """
    def __init__(self, base_url: str = EXPORTS_PROJECTPLANNING):
        self.base_url = base_url

    @logger(DATAVERSE_LOGGER_PROJECTS_PLANNING)
    def get_project_plannings_df(self) -> pd.DataFrame:
        """
        Retorna os registros de planejamento de projetos como um DataFrame.
        """
        data = self._fetch_project_plannings()
        df = pd.DataFrame(data)[self._columns()]
        df.rename(columns=self._rename_columns(), inplace=True)
        return df
    
    def _fetch_project_plannings(self) -> list:
        """
        Obtém os registros de planejamento de projetos no Dataverse.
        Retorna uma lista de dicionários (JSON).
        """
        response = requests.get(self.base_url)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _columns() -> list:
        return [
            'premsoft_projectplanningmonthlyid',
            '_premsoft_project_value@OData.Community.Display.V1.FormattedValue',
            'premsoft_datestart',
            'premsoft_dateend',
            'premsoft_contractedhours',
            'premsoft_usedhours',
            'premsoft_remaininghours',
        ]

    @staticmethod
    def _rename_columns() -> dict:
        return {
            'premsoft_projectplanningmonthlyid': 'planning_id',
            '_premsoft_project_value@OData.Community.Display.V1.FormattedValue': 'project_id',
            'premsoft_datestart': 'date_start',
            'premsoft_dateend': 'date_end',
            'premsoft_contractedhours': 'contracted_hours',
            'premsoft_usedhours': 'used_hours',
            'premsoft_remaininghours': 'remaining_hours'
        }
