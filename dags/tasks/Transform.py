from airflow.decorators import task
import pandas as pd

@task 
def Transform(Dfs, InflationAPI, ContraceptiveAPI, ATFMAPI):
   
    IndicatorsDf = pd.read_json(Dfs['CsvIndicators'])
    DfATFMApi = pd.read_json(ATFMAPI['ATFMIndicator'])
    DfInflationApi = pd.read_json(InflationAPI['InflationIndicator'])
    DfContraceptiveApi = pd.read_json(ContraceptiveAPI['ContraceptiveIndicator'])
    MarriedDf = pd.read_json(Dfs['MarriedIndicator'])
    SingleDf = pd.read_json(Dfs['SingleIndicator'])
    DivorcedDf = pd.read_json(Dfs['DivorcedIndictor'])
    

    #Separating indicators by sex
    DfATFMApiW = DfATFMApi.loc[DfATFMApi['SEX'] == 'F']
    DfATFMApiM = DfATFMApi.loc[DfATFMApi['SEX'] == 'M']

    DfATFMApiW.drop('SEX', axis = 1, inplace = True)
    DfATFMApiM.drop('SEX', axis = 1, inplace = True)

    DfATFMApiW.rename(columns={'Age at first marriage': 'Age at first marriage (Female)'}, inplace = True)
    DfATFMApiM.rename(columns={'Age at first marriage': 'Age at first marriage (Male)'}, inplace = True)

    #Merging the indicators in the same format together

    IndicatorsDf['Country Code'] = IndicatorsDf['Country Code'].astype(str)
    DfATFMApiM['Country Code'] = DfATFMApiM['Country Code'].astype(str)
    DfContraceptiveApi['Country Code'] = DfContraceptiveApi['Country Code'].astype(str)
    DfATFMApiW['Country Code'] = DfATFMApiW['Country Code'].astype(str)
    DfInflationApi['Country Code'] = DfInflationApi['Country Code'].astype(str)

    IndicatorsDf['Year'] = IndicatorsDf['Year'].astype(str)#str type for year helps avoiding potencial errors in the merge
    DfInflationApi['Year'] = DfInflationApi['Year'].astype(str)
    DfATFMApiM['Year'] = DfATFMApiM['Year'].astype(str)
    DfATFMApiW['Year'] = DfATFMApiW['Year'].astype(str)
    DfContraceptiveApi['Year'] = DfContraceptiveApi['Year'].astype(str)

    IndicatorsDf = pd.merge(IndicatorsDf, DfInflationApi, how='left', on=['Country Code', 'Year'])
    IndicatorsDf = pd.merge(IndicatorsDf, DfContraceptiveApi, how='left', on=['Country Code', 'Year'])
    IndicatorsDf = pd.merge(IndicatorsDf, DfATFMApiM, how='left', on=['Country Code', 'Year'])
    IndicatorsDf = pd.merge(IndicatorsDf, DfATFMApiW, how='left', on=['Country Code', 'Year'])

    #Names and ISO codes normalization
    CountriesInfo = pd.read_csv('/opt/airflow/StagingData/CountriesISOData.csv')

    ISONumberToName = dict(zip(CountriesInfo['ISO 3166-1 Numeric Code'], CountriesInfo['ISO 3166 Name']))
    ISOCode3ToName = dict(zip(CountriesInfo['ISO 3166-1 Aplha Code 3'], CountriesInfo['ISO 3166 Name']))
    ISOCode3ToNumber = dict(zip(CountriesInfo['ISO 3166-1 Aplha Code 3'], CountriesInfo['ISO 3166-1 Numeric Code']))

    MarriedDf['Country or area'] = MarriedDf['ISO code'].map(ISONumberToName).values
    MarriedDf = MarriedDf.loc[~MarriedDf['Country or area'].isna()]
    SingleDf['Country or area'] = SingleDf['ISO code'].map(ISONumberToName).values
    SingleDf = SingleDf.loc[~SingleDf['Country or area'].isna()]
    DivorcedDf['Country or area'] = DivorcedDf['ISO code'].map(ISONumberToName).values
    DivorcedDf = DivorcedDf.loc[~DivorcedDf['Country or area'].isna()]

    IndicatorsDf['Country Name'] = IndicatorsDf['Country Code'].map(ISOCode3ToName).values
    IndicatorsDf['Country Code'] = IndicatorsDf['Country Code'].map(ISOCode3ToNumber).values
    IndicatorsDf = IndicatorsDf.loc[~IndicatorsDf['Country Code'].isna()]
    IndicatorsDf['Country Code'] = IndicatorsDf['Country Code'].astype(int)

    #LATAM Filtering
    LatamISO = [
        "ARG","BOL","BRA","CHL","COL","CRI","CUB","DOM","ECU","SLV",
        "GTM","HND","MEX","NIC","PAN","PRY","PER","URY","VEN",
        "BHS","BRB","JAM","TTO","GRD","LCA","VCT","ATG","DMA","KNA","HTI","BLZ","SUR","GUY"
    ]

    LatamISO = pd.Series(LatamISO).map(ISOCode3ToNumber).values

    IndicatorsDf = IndicatorsDf[IndicatorsDf['Country Code'].isin(LatamISO)]
    MarriedDf = MarriedDf[MarriedDf['ISO code'].isin(LatamISO)]
    SingleDf = SingleDf[SingleDf['ISO code'].isin(LatamISO)]
    DivorcedDf = DivorcedDf[DivorcedDf['ISO code'].isin(LatamISO)]

    #Cleaning
    IndicatorsDf = IndicatorsDf.drop(['Indicator Name', 'Indicator Code'], axis=1)
    IndicatorsDf = IndicatorsDf[(IndicatorsDf['Year'] != 'Unnamed: 69') & (IndicatorsDf['Year'] != '2024')] #Almost no data in 2024
    IndicatorsDf['Year'] = IndicatorsDf['Year'].astype(int)
    IndicatorsDf = IndicatorsDf.apply(lambda Column: Column.round(2) if Column.dtype == "float64" or Column.dtype == "int64" else Column)

    print(IndicatorsDf.info())
        
    return {
        'LatamIndicators': IndicatorsDf.to_json(),
        'LatamMarriedIndicator': MarriedDf.to_json(),
        'LatamSingleIndicator': SingleDf.to_json(),
        'LatamDivorcedIndicator': DivorcedDf.to_json()
    }

