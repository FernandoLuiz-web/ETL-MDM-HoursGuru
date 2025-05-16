import mlflow.sklearn
import mlflow.sklearn
import pandas as pd
import mlflow
from constants.Mlflow_objects import (
    MLFLOW_TRACKING_URI,
    MLFLOW_EXPERIMENT_ID
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

PREPROCESS_NAME = "PREPROCESS_2025-01_to_2025-04.csv"
PREPROCESS_DIR = "ml/data/preprocess"

class ModelTrainer:
    def __init__(self):
        self.__full_path_dataset = f"./{PREPROCESS_DIR}/{PREPROCESS_NAME}"
        self.__model = RandomForestRegressor(min_samples_leaf=1, n_estimators=400)
        self.__label_encoder = LabelEncoder()
        self.__mlflow_tracking_uri = MLFLOW_TRACKING_URI
        self.__mlflow_experiment_id = MLFLOW_EXPERIMENT_ID
    
    def training(self):

        mlflow.set_tracking_uri(self.__mlflow_tracking_uri)
        mlflow.set_experiment(experiment_id=self.__mlflow_experiment_id)

        training_dataset_df = pd.read_csv(self.__full_path_dataset)

        training_dataset_df['user'] = self.__label_encoder.fit_transform(training_dataset_df['user'])
        training_dataset_df['project'] = self.__label_encoder.fit_transform(training_dataset_df['project'])
        training_dataset_df['period'] = pd.to_datetime(training_dataset_df['period'])
        training_dataset_df['month'] = training_dataset_df['period'].dt.month
        training_dataset_df['year'] =  training_dataset_df['period'].dt.year

        training_dataset_df['period'] = training_dataset_df['period'].dt.to_period('M')

        training_dataset_df = training_dataset_df.drop('period', axis=1)

        with mlflow.start_run():
            
            mlflow.sklearn.autolog()
            x_columns = ['user', 'project', 'month', 'year', 'contracted_hours', 'remaining_hours']

            x = training_dataset_df[x_columns]
            y = training_dataset_df['worked_hours']

            x_train, x_test, y_train, y_test = train_test_split(x,y, test_size=0.2, random_state=42)

            self.__model.fit(x_train, y_train)

            y_pred_rf_regressor_test = self.__model.predict(x_test)
            self._get_metrics_of_model_run(y_pred_rf_regressor_test, y_test)

    
    def _get_metrics_of_model_run(self, y_predictor, y_test) -> None:
        mse_rf_regressor = mean_squared_error(y_test, y_predictor)
        rmse_rf_regressor = mse_rf_regressor ** 0.5
        r2_rf_regressor = r2_score(y_test, y_predictor)

        mlflow.log_metric("RMSE", rmse_rf_regressor)
        mlflow.log_metric("R²", r2_rf_regressor)