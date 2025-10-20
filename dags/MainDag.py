from airflow.decorators import dag
from datetime import datetime
from tasks.Extract import Extract
from tasks.Transform import Transform
from tasks.CreateDatabase import Create_Tables
from tasks.Load import DB_Data_Upload

@dag(
    dag_id='Proyecto2Prueba',
    start_date=datetime(2025, 3, 20),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1
)

def Pipeline_Prueba():

    Dfs = Extract()
    DfsClean = Transform(Dfs)
    Create_Tables()
    DB_Data_Upload(DfsClean)


Pipeline_Prueba()