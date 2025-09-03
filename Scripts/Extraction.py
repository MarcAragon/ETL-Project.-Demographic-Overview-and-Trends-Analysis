import pandas as pd
import os

FolderDir = 'SourceData'

Df = pd.DataFrame()

for File in os.listdir(FolderDir):
    
    Path = os.path.join(FolderDir, File)

    try:
        Source = pd.read_csv(Path, skiprows=3)

        Source = pd.melt(Source, 
            id_vars= ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'], 
            var_name= 'Year',
            value_name= Source['Indicator Name'][0])
        
        #Dropping duplicate columns (Country name, Code and Year)
        Source = Source.loc[:, ~Source.columns.isin(Df.columns)]
        
        Df = pd.concat([Df, Source], axis=1)
        
    except Exception as e:
        print(e)

print(Df)


Df.to_csv('StagingData/GlobalWorldBankIndicators.csv', index=False)
