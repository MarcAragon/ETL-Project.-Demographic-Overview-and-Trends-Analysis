from airflow.decorators import task
import pandas as pd
import os 
from sqlalchemy import create_engine

@task
def ExtractFiles():

    #Local files indcators extraction
    
    FolderDir = '/opt/airflow/SourceData'

    CsvDf = pd.DataFrame()

    for File in os.listdir(FolderDir):
        
        Path = os.path.join(FolderDir, File)

        try:

            IndicatorDf = pd.read_csv(Path, skiprows=3)

            #Transforming the file to long form (thats the API data format)
            IndicatorDf = pd.melt(IndicatorDf, 
                id_vars = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'], 
                var_name = 'Year',
                value_name = IndicatorDf['Indicator Name'][0])
            
            #Dropping duplicate columns (Country name, Code and Year)
            IndicatorDf = IndicatorDf.loc[:, ~IndicatorDf.columns.isin(CsvDf.columns)]
            
            #Fusing all the indicator values in one single dataframe
            CsvDf = pd.concat([CsvDf, IndicatorDf], axis=1)
            
        except Exception as e:
            print(e)

    print(CsvDf.info())

    #UN DB Data extraction

    Engine = create_engine('mysql+pymysql://root:@host.docker.internal:3333/project2')
    MarriedDf = pd.read_sql_table('marrieddata', con=Engine)
    SingleDf = pd.read_sql_table('singledata', con=Engine)
    DivorcedDf = pd.read_sql_table('divorceddata', con=Engine)
    print(SingleDf.info())


    return {
        'CsvIndicators': CsvDf.to_json(),
        'MarriedIndicator': MarriedDf.to_json(),
        'SingleIndicator': SingleDf.to_json(),
        'DivorcedIndictor': DivorcedDf.to_json()
    }


