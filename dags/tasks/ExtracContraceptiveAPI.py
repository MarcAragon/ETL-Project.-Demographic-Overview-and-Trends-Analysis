from airflow.decorators import task
import pandas as pd
import requests

@task
def ExtractContraceptiveAPI():

    DfContraceptiveApi = pd.DataFrame()

    CountryCodes = list(pd.read_csv('/opt/airflow/SourceData/Birth rate, crude (per 1,000 people).csv', skiprows=3)['Country Code'])

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
        'ContraceptiveIndicator': DfContraceptiveApi.to_json(),
    }