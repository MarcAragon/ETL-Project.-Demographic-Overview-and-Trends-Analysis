import pandas as pd
from sqlalchemy import create_engine

MaritalStatusDf = pd.read_excel("SourceData\\undesa_pd_2019_wmd_marital_status.xlsx", sheet_name="MARITAL_STATUS_BY_AGE", skiprows=2)
MaritalStatusDf = MaritalStatusDf[['Country or area', 'YearEnd', 'Sex', 'MaritalStatus', 'AgeGroup', 'DataValue']]
MarriedDf = MaritalStatusDf.loc[MaritalStatusDf['MaritalStatus'] == 'Married']
SingleDf = MaritalStatusDf.loc[MaritalStatusDf['MaritalStatus'] == 'Single']

MarriedDf.drop('MaritalStatus', axis = 1, inplace = True)
SingleDf.drop('MaritalStatus', axis = 1, inplace = True)

Engine = create_engine('mysql+pymysql://root:@localhost:3333/project2')
MarriedDf.to_sql(name='marrieddata', con=Engine, if_exists='replace', index=False)
SingleDf.to_sql(name='singledata', con=Engine, if_exists='replace', index=False)