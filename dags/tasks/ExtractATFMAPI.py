from airflow.decorators import task
import pandas as pd
import requests
from kafka import KafkaProducer
import json

@task
def ExtractATFMAPI():

    Producer = KafkaProducer(
        bootstrap_servers=['192.168.1.31:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    #Country codes for every api access
    CountryCodes = list(pd.read_csv('/opt/airflow/SourceData/Birth rate, crude (per 1,000 people).csv', skiprows=3)['Country Code'])
    Indicator = 'ATFMIndicator'

    DfATFMApi = pd.DataFrame()

    for Country in CountryCodes:
        print('Pulling data for country:', Country)
        Response = requests.get(f'https://data360api.worldbank.org/data360/data?DATABASE_ID=WB_HNP&INDICATOR=WB_HNP_SP_DYN_SMAM&REF_AREA={Country}&skip=0')
        #Average age at first marriage
        
        if Response.status_code != 200:
            print(f"Error en la solicitud: {Response.status_code}")

        else:
            Data = Response.json()
            KafkaMessage = {
                'indicator': Indicator
            }

            if Data['value'] == []:
                print('No data found for this country')
                for i in range(65):
                    KafkaMessage['data_value'] = None
                    Producer.send('APITopic', value=KafkaMessage)

            else:
                CountryData = pd.DataFrame(Data['value'])[['OBS_VALUE', 'COMMENT_TS', 'TIME_PERIOD', 'SEX']]

                #Sending the api data to kafka streaming
                for Value in CountryData['OBS_VALUE'].values :
                    KafkaMessage['data_value'] = Value
                    Producer.send('APITopic', value=KafkaMessage)

                CountryData['Country Code'] = Country
                CountryData.rename(columns={'OBS_VALUE': CountryData['COMMENT_TS'].values[0], 'TIME_PERIOD': 'Year'}, inplace=True)
                CountryData.drop('COMMENT_TS', axis=1, inplace=True)
                DfATFMApi = pd.concat([DfATFMApi, CountryData], axis=0)
            
            

    DfATFMApi.rename(columns={None: 'Age at first marriage'}, inplace=True)
    DfATFMApi.reset_index(drop=True, inplace=True)
    print(DfATFMApi.info())

    return {
        'ATFMIndicator': DfATFMApi.to_json(),
    }