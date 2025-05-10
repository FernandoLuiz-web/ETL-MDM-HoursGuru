import pandas as pd
from database.db_connection import DatabaseConnection
from database.repositories.AppointedHours_repository import AppointedHours_repository
from database.repositories.ProjectPlanning_repository import ProjectPlanning_repository

COLUMNS_APPOINTMENT = ['project', 'user', 'start_at','end_at','worked_hours']
COLUMNS_PPLANNING = ['project_id', 'date_start', 'date_end', 'contracted_hours']
COLUMNS_PREPROCESS_DATAS = ['user', 'project', 'period', 'worked_hours', 'contracted_hours']

class LoadData:

    def run(self) -> None:
        appointments_df, plannings_df = self.__get_dataframes()
        appointments_df = self.__make_period_for_dataframe(appointments_df, 'start_at', 'end_at')
        plannings_df = self.__make_period_for_dataframe(plannings_df, 'date_start', 'date_end')
        
        appointments_df = appointments_df.groupby(['user','project','period']).agg({
            'worked_hours': 'sum'
        }).reset_index()

        merged_df = pd.merge(appointments_df, plannings_df, left_on=['project', 'period'], right_on=['project_id', 'period'], how='inner')
        
        merged_df = merged_df[COLUMNS_PREPROCESS_DATAS].sort_values(by='period')

        merged_df['remaining_hours'] = merged_df['contracted_hours'] - merged_df['worked_hours']

        merged_df.to_csv(f"./preprocess/{merged_df['period'].min()}_to_{merged_df['period'].max()}", index=False)

    def __get_dataframes() -> tuple[pd.DataFrame, pd.DataFrame]:
        database = DatabaseConnection()
        database.connect()

        appointed_repository = AppointedHours_repository(database)
        planning_repository = ProjectPlanning_repository(database)

        database.close()

        appointments_df = pd.DataFrame(appointed_repository.get_appointed_hours())
        plannings_df = pd.DataFrame(planning_repository.get_all_pplaning())

        return appointments_df, plannings_df

    def __make_period_for_dataframe(df, date_column_start, date_column_end) -> pd.DataFrame:
        df['period'] = df[date_column_start].dt.to_period('M').astype(str)
        df = df.drop([date_column_start, date_column_end], axis=1)
        return df
    
