from dagster import job

from .assets import hello_dagster  # Importe o asset

@job
def meu_primeiro_job():
    hello_dagster()  # Defina o job para executar o asset