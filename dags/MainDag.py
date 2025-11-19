from airflow.decorators import dag
from datetime import datetime
from tasks.ExtractFiles import ExtractFiles
from tasks.Transform import Transform
from tasks.CreateDatabase import Create_Tables
from tasks.Load import DB_Data_Upload
from tasks.ExtractContraceptiveAPI import ExtractContraceptiveAPI
from tasks.ExtractATFMAPI import ExtractATFMAPI
from tasks.ExtractInflationAPI import ExtractInflationAPI

@dag(
    dag_id='Proyecto2',
    start_date=datetime(2025, 3, 20),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1
)

def Pipeline_Prueba():

    Dfs = ExtractFiles()
    InflationAPI = ExtractInflationAPI()
    ContraceptiveAPI = ExtractContraceptiveAPI()
    ATFMAPI = ExtractATFMAPI()
    DfsClean = Transform(Dfs, InflationAPI, ContraceptiveAPI, ATFMAPI)
    Create_Tables()
    DB_Data_Upload(DfsClean)


Pipeline_Prueba()