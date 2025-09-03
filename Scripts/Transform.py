import pandas as pd

Df = pd.read_csv('StagingData\GlobalWorldBankIndicators.csv')

#LATAM Filtering
LatamIso = [
    "ARG","BOL","BRA","CHL","COL","CRI","CUB","DOM","ECU","SLV",
    "GTM","HND","MEX","NIC","PAN","PRY","PER","URY","VEN",
    "BHS","BRB","JAM","TTO","GRD","LCA","VCT","ATG","DMA","KNA","HTI","BLZ","SUR","GUY"
]

Df = Df[Df['Country Code'].isin(LatamIso)]


#Cleaning
Df = Df.drop(['Indicator Name', 'Indicator Code'], axis=1)
Df = Df[(Df['Year'] != 'Unnamed: 69') & (Df['Year'] != '2024')] #Almost no data in 2024
Df['Year'] = Df['Year'].astype(int)
Df = Df.apply(lambda Column: Column.round(2) if Column.dtype == "float64" or Column.dtype == "int64" else Column)
Df['Country Name'] = Df['Country Name'].apply(lambda Row: 'Bahamas' if Row == "Bahamas, The" else Row)
Df['Country Name'] = Df['Country Name'].apply(lambda Row: 'Venezuela' if Row == "Venezuela, RB" else Row)


#Renaming
Df.columns = ['country_name', 
              'country_code', 
              'year', 
              'births_per_1000', 
              'death_by_injury_percentage', 
              'deaths_per_1000', 
              'births_per_woman', 
              'gini_index', 
              'hospital_beds_per_1000',
              'female_life_expectancy',
              'male_life_expectancy',
              'neonatal_deaths_per_1000',
              'annual_population_growth_percentage',
              'population_in_slums_percentage',
              'prenatal_care_percentage']

print(Df.info())

Df.to_csv('LoadingData/LatamIndicators.csv', index=False)