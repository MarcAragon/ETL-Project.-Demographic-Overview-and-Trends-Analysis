from airflow.decorators import task
import pandas as pd

@task 
def Transform(Data):
    Df1 = pd.read_json(Data['Df1'])
    Df2 = pd.read_json(Data['Df2'])

    Df1 = Df1.loc[Df1['country_code'] == 'ATG']

    Df1.to_csv('/opt/airflow/StagingData/prueba.csv')
    
    return {
        'Df1': Df1.to_json(),
        'Df2': Df2.to_json()
    }