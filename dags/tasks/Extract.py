from airflow.decorators import task
import pandas as pd
import os 
import requests
from sqlalchemy import create_engine

@task
def Extract():

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
    print(SingleDf.info())

    #API indicators extraction
 
    #Country codes for every api access
    CountryCodes = list(pd.read_csv('/opt/airflow/SourceData/Birth rate, crude (per 1,000 people).csv', skiprows=3)['Country Code'])

    DfInflationApi = pd.DataFrame()

    for Country in CountryCodes:
        print('Pulling data for country:', Country)
        Response = requests.get(f'https://data360api.worldbank.org/data360/data?DATABASE_ID=WB_WDI&INDICATOR=WB_WDI_FP_CPI_TOTL_ZG&REF_AREA={Country}&skip=0')
        
        if Response.status_code != 200:
            print(f"Error en la solicitud: {Response.status_code}")

        else:
            Data = Response.json()
            if Data['value'] == []:
                print('No data found for this country') 

            else:
                CountryData = pd.DataFrame(Data['value'])[['OBS_VALUE', 'COMMENT_TS', 'TIME_PERIOD']]
                CountryData['Country Code'] = Country
                CountryData.rename(columns={'OBS_VALUE': CountryData['COMMENT_TS'].values[0], 'TIME_PERIOD': 'Year'}, inplace=True)
                CountryData.drop('COMMENT_TS', axis=1, inplace=True)
                DfInflationApi = pd.concat([DfInflationApi, CountryData], axis=0)

    DfInflationApi.reset_index(drop=True, inplace=True)
    print(DfInflationApi.info())


    DfATFMApi = pd.DataFrame()

    for Country in CountryCodes:
        print('Pulling data for country:', Country)
        Response = requests.get(f'https://data360api.worldbank.org/data360/data?DATABASE_ID=WB_HNP&INDICATOR=WB_HNP_SP_DYN_SMAM&REF_AREA={Country}&skip=0')
        
        if Response.status_code != 200:
            print(f"Error en la solicitud: {Response.status_code}")

        else:
            Data = Response.json()
            if Data['value'] == []:
                print('No data found for this country') 

            else:
                CountryData = pd.DataFrame(Data['value'])[['OBS_VALUE', 'COMMENT_TS', 'TIME_PERIOD', 'SEX']]
                CountryData['Country Code'] = Country
                CountryData.rename(columns={'OBS_VALUE': CountryData['COMMENT_TS'].values[0], 'TIME_PERIOD': 'Year'}, inplace=True)
                CountryData.drop('COMMENT_TS', axis=1, inplace=True)
                DfATFMApi = pd.concat([DfATFMApi, CountryData], axis=0)

    DfATFMApi.rename(columns={None: 'Age at first marriage'}, inplace=True)
    DfATFMApi.reset_index(drop=True, inplace=True)
    print(DfATFMApi.info())



    DfContraceptiveApi = pd.DataFrame()

    for Country in CountryCodes:
        print('Pulling data for country:', Country)
        Response = requests.get(f'https://data360api.worldbank.org/data360/data?DATABASE_ID=WB_WDI&INDICATOR=WB_WDI_SP_DYN_CONU_ZS&REF_AREA={Country}&skip=0')
        
        if Response.status_code != 200:
            print(f"Error en la solicitud: {Response.status_code}")

        else:
            Data = Response.json()
            if Data['value'] == []:
                print('No data found for this country') 

            else:
                CountryData = pd.DataFrame(Data['value'])[['OBS_VALUE', 'COMMENT_TS', 'TIME_PERIOD']]
                CountryData['Country Code'] = Country
                CountryData.rename(columns={'OBS_VALUE': CountryData['COMMENT_TS'].values[0], 'TIME_PERIOD': 'Year'}, inplace=True)
                CountryData.drop('COMMENT_TS', axis=1, inplace=True)
                DfContraceptiveApi = pd.concat([DfContraceptiveApi, CountryData], axis=0)


    DfContraceptiveApi.reset_index(drop=True, inplace=True)
    print(DfContraceptiveApi.info())


    return {
        'CsvIndicators': CsvDf.to_json(),
        'InflationIndicator': DfInflationApi.to_json(),
        'ATFMIndicator': DfATFMApi.to_json(),
        'ContraceptiveIndicator': DfContraceptiveApi.to_json(),
        'MarriedIndicator': MarriedDf.to_json(),
        'SingleIndicator': SingleDf.to_json()
    }


