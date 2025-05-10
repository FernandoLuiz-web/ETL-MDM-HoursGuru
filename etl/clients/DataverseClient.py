import requests
import pandas as pd
from shared.logger import info, error
from constants.endpoints import EXPORTS_PROJECTPLANNING
from constants.logger_messages import DATAVERSE_LOGGER_PROJECTS_PLANNING

class Dataverse:
    """
    Client para interagir com a API do Dataverse.
    """
    def __init__(self, base_url: str = EXPORTS_PROJECTPLANNING):
        self.base_url = base_url

    def get_project_plannings_df(self) -> pd.DataFrame:
        """
        Retorna os registros de planejamento de projetos como um DataFrame.
        """
        info(DATAVERSE_LOGGER_PROJECTS_PLANNING)
        data = self._fetch_project_plannings()
        df = pd.DataFrame(data)[self._columns()]
        df = df.loc[df['premsoft_projectplanningmonthlyname'].str.contains('Modernization')]
        df.rename(columns=self._rename_columns(), inplace=True)
        df = df.drop('premsoft_projectplanningmonthlyname', axis=1)
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
            'premsoft_projectplanningmonthlyname',
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
