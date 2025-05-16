from concurrent.futures import ThreadPoolExecutor
import contextvars
import click
import sys
import os

# Adiciona os diretórios ETL e ML ao sys.path para resolver os imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'etl')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'ml')))

# Importa os módulos de ETL
from etl.flows.dataverse_flow import dataverse_flow
from etl.flows.clockify_flow import clockify_flow

# Importa os módulos de ML
from ml.data.load_data import LoadData
from ml.models.train_model import ModelTrainer
from ml.features.build_feature import FeatureBuilder
from ml.models.evaluate_model import ModelEvaluator


@click.group()
def cli():
    """Hours Guru CLI - Centraliza os comandos de ETL e Machine Learning."""
    pass


# Comandos para ETL
@cli.group()
def etl():
    """Comandos relacionados ao processo de ETL."""
    pass


@etl.command()
def run_clockify():
    """Executa o ETL para o Clockify."""
    click.echo("🔄 Executando o ETL do Clockify...")
    clockify_flow()
    click.echo("✅ ETL do Clockify concluído com sucesso!")


@etl.command()
def run_dataverse():
    """Executa o ETL para o Dataverse."""
    click.echo("🔄 Executando o ETL do Dataverse...")
    dataverse_flow()
    click.echo("✅ ETL do Dataverse concluído com sucesso!")

@etl.command()
def run_all():
    click.echo("🔄 Executando todos os ETLs...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        w1 = executor.submit(contextvars.copy_context().run, clockify_flow)
        w2 = executor.submit(contextvars.copy_context().run, dataverse_flow)
    click.echo("✅ Todos os ETLs concluídos com sucesso!")

@etl.command()
def prefect_server_up():
    click.echo("🔄 Executando prefect UI...")
    os.system("prefect server start --background")
    click.echo("✅ Prefect server rodando com sucesso!")

@etl.command()
def prefect_server_down():
    click.echo("🔄 Executando prefect UI...")
    os.system("prefect server stop")
    click.echo("✅ Prefect server parado com sucesso!")

# Comandos para Machine Learning
@cli.group()
def ml():
    """Comandos relacionados ao treinamento de Machine Learning."""
    pass


@ml.command()
def load_data():
    """Carrega os dados do sistema."""
    click.echo("🔄 Carregando os dados...")
    LoadData().run()
    click.echo("✅ Dados carregados com sucesso!")


@ml.command()
def train():
    """Treina o modelo de Machine Learning."""
    click.echo("🔄 Treinando o modelo...")
    ModelTrainer().training()
    click.echo("✅ Modelo treinado com sucesso!")


@ml.command()
def build_features():
    """Constrói as features para o modelo."""
    click.echo("🔄 Construindo features...")
    FeatureBuilder().run()
    click.echo("✅ Features construídas com sucesso!")


@ml.command()
def evaluate():
    """Avalia o modelo treinado."""
    click.echo("🔄 Avaliando o modelo...")
    result = ModelEvaluator().evaluate()
    click.echo(f"✅ Resultado da avaliação: {result}")


@cli.group()
def app():
    """Comandos relacionados a parte de webApp"""
    pass

@app.command()
def start_webapp():
    """Inicia o serviço web do aplicativo."""
    click.echo("🔄 Subindo o serviço web...")
    os.system("streamlit run .\\app\\app.py")



if __name__ == '__main__':
    cli()