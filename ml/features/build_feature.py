import os
import pandas as pd
from database.db_connection import DatabaseConnection
from database.repositories.AppointedHours_repository import AppointedHours_repository
from database.repositories.ProjectPlanning_repository import ProjectPlanning_repository
from datetime import datetime

COLUMNS_PREPROCESS_DATAS = ['user', 'project', 'period', 'contracted_hours', 'remaining_hours']
COLUMNS_FEATURE_DATAS = ['user', 'project', 'month', 'year', 'contracted_hours', 'remaining_hours']
OUTPUT_DIR = "ml/data/features"

class FeatureBuilder:
    def run(self) -> None:
        appointments_df, plannings_df = self.__get_dataframes()
        
        appointments_df = self.__make_period_for_dataframe(appointments_df, 'start_at', 'end_at')
        plannings_df = self.__make_period_for_dataframe(plannings_df, 'date_start', 'date_end')

        appointments_df = appointments_df.groupby(['user','project','period']).agg({
            'worked_hours': 'sum'
        }).reset_index()

        appointments_df = appointments_df[['user', 'project']].drop_duplicates()

        merged_df = pd.merge(
            plannings_df, 
            appointments_df, 
            left_on=['project_id'], 
            right_on=['project'], 
            how='inner', 
            suffixes=('_planning', '_appointment')
        )
        
        merged_df = merged_df[COLUMNS_PREPROCESS_DATAS].sort_values(by='period')

        merged_df['month'] = pd.to_datetime(merged_df['period']).dt.month
        merged_df['year'] = pd.to_datetime(merged_df['period']).dt.year

        period_min = merged_df['period'].min()
        merged_df = merged_df.drop('period', axis=1)
        merged_df = merged_df[COLUMNS_FEATURE_DATAS]

        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)

        merged_df.to_csv(f"{OUTPUT_DIR}/FEATURE_{period_min}.csv", index=False)

    def __get_dataframes(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        database = DatabaseConnection()
        database.connect()

        appointed_repository = AppointedHours_repository(database)
        planning_repository = ProjectPlanning_repository(database)

        appointments_df = pd.DataFrame(appointed_repository.get_all_appointed_hours())
        plannings_df = pd.DataFrame(planning_repository.get_pplanning_in_current_month())

        plannings_df = plannings_df.drop(columns=['used_hours'], errors='ignore')

        database.close()

        return appointments_df, plannings_df

    def __make_period_for_dataframe(self, df, date_column_start, date_column_end) -> pd.DataFrame:
        df['period'] = df[date_column_start].dt.to_period('M').astype(str)
        df = df.drop([date_column_start, date_column_end], axis=1)
        return df
    