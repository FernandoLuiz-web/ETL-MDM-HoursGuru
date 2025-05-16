import mlflow
import mlflow.client
import pandas as pd
import mlflow.sklearn
from sklearn.calibration import LabelEncoder
from constants.Mlflow_objects import (
    MLFLOW_TRACKING_URI,
    MODEL_NAME
)

FEATURE_DIR = "ml/data/features"
FEATURE_NAME = "FEATURE_2025-05.csv"
COLUMNS_PREDICTED_DF = ['user', 'project', 'month', 'year', 'contracted_hours', 'worked_hours', 'remaining_hours']

class ModelEvaluator:
    def __init__(self):
        self.__mlflow_tracking_uri = MLFLOW_TRACKING_URI
        self.__label_encoder = LabelEncoder() 
        self.__mlflow_client = mlflow.client.MlflowClient()

    def evaluate(self) -> None:
        mlflow.set_tracking_uri(self.__mlflow_tracking_uri)
        # version = self.get_latest_version(MODEL_NAME)
        version = 2
        model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{version}")

        evaluate_df = pd.read_csv(f"{FEATURE_DIR}/{FEATURE_NAME}", sep=",")

        evaluate_df['user'] = self.__label_encoder.fit_transform(evaluate_df['user'])
        evaluate_df['project'] = self.__label_encoder.fit_transform(evaluate_df['project'])

        x = evaluate_df.head()[model.feature_names_in_]

        x['worked_hours'] = model.predict(x)

        return x[COLUMNS_PREDICTED_DF]
    
    def get_latest_version(self, model_name: str) -> int:
        return max([int(i.version) for i in self.__mlflow_client.get_latest_versions(model_name)])