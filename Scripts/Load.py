import pandas as pd
import pandas_gbq
from pandas_gbq import to_gbq
from google.oauth2 import service_account

IndicatorsFacts = pd.read_csv('LoadingData\LatamCleanIndicators.csv')

#Dimensional Model Prepping 

CountriesDim = pd.DataFrame()
CountriesDim['country_id'] = [Num for Num in range( len(IndicatorsFacts['country_name'].unique()) )]
CountriesDim['country_name'] = IndicatorsFacts['country_name'].unique()
CountriesDim['country_code'] = IndicatorsFacts['country_code'].unique()

YearsDim = pd.DataFrame()
YearsDim['year_id'] = [Num for Num in range( len(IndicatorsFacts['year'].unique()) )]
YearsDim['year'] = IndicatorsFacts['year'].unique()


IndicatorsFacts = IndicatorsFacts.drop('country_code', axis=1)
IndicatorsFacts['country_name'] = IndicatorsFacts['country_name'].apply(lambda Row: CountriesDim.loc[CountriesDim['country_name'] == Row]['country_id'].values[0])
IndicatorsFacts['year'] = IndicatorsFacts['year'].apply(lambda Row: YearsDim.loc[YearsDim['year'] == int(Row)]['year_id'].values[0])
IndicatorsFacts = IndicatorsFacts.rename(columns={'country_name': 'country_id', 'year': 'year_id'})

#Loading

Tables = {
    'indicatorfacts': IndicatorsFacts,
    'countriesdim': CountriesDim,
    'yearsdim': YearsDim
}

ProjectId = "etl-469721"
DatasetId = "prueba1"
ServiceAccountFile = 'etl-469721-b3349513066c.json'
Credentials = service_account.Credentials.from_service_account_file(ServiceAccountFile)

for Key, Value in Tables.items():
    
    try:
        to_gbq(Value, f"{ProjectId}.{DatasetId}.{Key}", project_id=ProjectId, credentials=Credentials, if_exists='fail') #Fail should be in place until new data is added to the pipeline, or its your first time running this code.
    
    except pandas_gbq.exceptions.TableCreationError:    
        print('Table already exist, no changes made.')