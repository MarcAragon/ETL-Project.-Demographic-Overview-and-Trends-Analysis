from airflow.decorators import task

import pandas as pd

from sqlalchemy import create_engine,inspect, text
from sqlalchemy.orm import sessionmaker

@task
def DB_Data_Upload(Data):

    DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    Engine = create_engine(DB_URL, echo=True)
    Session = sessionmaker(bind=Engine)()
    Inspector = inspect(Engine)
    Conn = Engine.raw_connection()
    Cursor = Conn.cursor()

    CountriesDf = pd.read_csv('/opt/airflow/StagingData/CountriesISOData.csv')
    IndicatorsDf = pd.read_json(Data['LatamIndicators'])
    MarriedDf = pd.read_json(Data['LatamMarriedIndicator'])
    SingleDf = pd.read_json(Data['LatamSingleIndicator']) 

    CountriesDf.drop('Unnamed: 0', axis = 1, inplace=True)
    MarriedDf.insert(0, 'id', range(len(MarriedDf)))
    SingleDf.insert(0, 'id', range(len(SingleDf)))
    IndicatorsDf.insert(0, 'id', range(len(IndicatorsDf)))
    
    def Insertar(Row, Table):
            
        ColumnNames = [Col['name'] for Col in Inspector.get_columns(Table)]
        Placeholder = ", ".join(["%s"] * len(ColumnNames))

        if Table == 'indicatorsdata': 

            TableRow = Session.execute(
                text(f"SELECT 1 FROM {Table} WHERE id = :id"), {'id': Row['id']}
            ).fetchone()

            if TableRow:
                return(print('Fila duplicada saltada'))

            Cursor.execute(f"INSERT INTO {Table} VALUES ({Placeholder})", list(Row))
            Conn.commit() 
            return True

        if Table != 'countries':

            TableRow = Session.execute(
                text(f"SELECT 1 FROM {Table} WHERE survey_id = :survey_id"), {'survey_id': Row['id']}
            ).fetchone()

            if TableRow:
                return(print('Fila duplicada saltada'))

        Cursor.execute(f"INSERT INTO {Table} VALUES ({Placeholder})", list(Row))
        Conn.commit()


#    for _, Row in CountriesDf.iterrows(): #Esta solo se tiene que correr 1 vez
        #Insertar(Row, 'countries')

    for _, Row in IndicatorsDf.iterrows():
        Insertar(Row, 'indicatorsdata')

    for _, Row in MarriedDf.iterrows():
        Insertar(Row, 'marrieddata')

    for _, Row in SingleDf.iterrows():
        Insertar(Row, 'singledata')

    Cursor.close()
    Conn.close()
