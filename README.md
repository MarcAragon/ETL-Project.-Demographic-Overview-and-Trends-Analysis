# ETL Project: Demographic Overview and Vital Trends in Latin America: Births, Health and Mortality

## Context

In the current context, government entities and international organizations, play a crucial role in monitoring and analyzing demographic and health indicators that are essential for formulating effective public policies. In particular, the analysis of vital trends, such as births, health, and mortality, has become a priority for Latin American governments seeking to improve the quality of life of their citizens and optimize resources allocated to public health and social well-being.

Our main objective is to provide a clear and accurate view of demographic and vital trends in Latin America, thereby helping public policymakers make evidence-based decisions that promote the well-being and sustainable development of the region.

## Solution


### About the dataset

The World Bank offers a vast historical database covering a wide range of global economic, social, environmental, and financial indicators. Its scope ranges from macroeconomic indicators to sector-specific variables such as health and infrastructure, allowing us to easily access the fields we need for our analysis, these are the ones chosen:

- [Birth rate, crude (per 1,000 people)](https://data.worldbank.org/indicator/SP.DYN.CBRT.IN)
- [Cause of death, by injury (% of total)](https://data.worldbank.org/indicator/SH.DTH.INJR.ZS)
- [Death rate, crude (per 1,000 people)](https://data.worldbank.org/indicator/SP.DYN.CDRT.IN?locations=1W)
- [Fertility rate, total (births per woman)](https://data.worldbank.org/indicator/SP.DYN.TFRT.IN)
- [Gini index](https://data.worldbank.org/indicator/SI.POV.GINI)
- [Hospital beds (per 1,000 people)](https://data.worldbank.org/indicator/SH.MED.BEDS.ZS)
- [Life expectancy at birth, female (years)](https://data.worldbank.org/indicator/SP.DYN.LE00.FE.IN)
- [Life expectancy at birth, male (years)](https://data.worldbank.org/indicator/SP.DYN.LE00.MA.IN)
- [Mortality rate, neonatal (per 1,000 live births)](https://data.worldbank.org/indicator/SH.DYN.NMRT)
- [Population growth (annual %)](http://data.worldbank.org/indicator/SP.POP.GROW)
- [Population living in slums (% of urban population)](https://data.worldbank.org/indicator/EN.POP.SLUM.UR.ZS)
- [Pregnant women receiving prenatal care (%)](https://data.worldbank.org/indicator/SH.STA.ANVC.ZS)

### API integration

The World Data Bank offers a comprehensive and integrated set of curated development data from across the World Bank Group and partners through an API platform, which is useful for automating the implementation in our Data Warehouse of changes in existing data or the presence of new records being added to any indicator. These are the ones chosen:

- [Inflation, consumer prices (annual %)](https://data.worldbank.org/indicator/FP.CPI.TOTL.ZG)
- [Age at first marriage (mean)](https://genderdata.worldbank.org/en/indicator/sp-dyn-smam) 
- [Contraceptive prevalence, any method (% of married women ages 15-49)](https://data.worldbank.org/indicator/SP.DYN.CONU.ZS)


### Extra data used

- [United Nations world marriage data](https://www.un.org/development/desa/pd/data/world-marriage-data)

### Tech stack

**Code Flow Management**: Apache Airflow.

**Data Proccessing & Loading:** Python.

**DB:** PostreSQL. 

**Data Analysis :** Power BI.

### ETL pipeline diagram 

![Link](https://imgur.com/RNMmaGj.jpeg)

### EDA

![Link](https://imgur.com/WGII83n.jpeg)
![Link](https://imgur.com/auXpqrN.jpeg)
![Link](https://imgur.com/INNV4e0.jpeg)

### Transformations

- Separated the 'Age at first marriage' indicator by sex.
- Cleaned out unnecessary data from the api requests.
- Merged all the Data World Group data together.
- Normalized the ISO numeric codes and country names among the data.
- Filtered the Latam records.

### DDM (Dimensional Data Model) 

![Link](https://imgur.com/cYJjZ4s.jpeg)


### Results (Change)

![Link](https://imgur.com/WJqn8r8.jpeg)
![Link](https://imgur.com/wPCdR5D.jpeg)
![Link](https://imgur.com/g0562Za.jpeg)


### Conclusions

- Fertility in South America dropped from more than 6 children per woman in the 1960s to about 2 today.
- The birth rate per 1,000 inhabitants declined steadily, reflecting demographic transition.
- The average Gini index of 49.17 reflects high inequality in the region.
- Female life expectancy exceeds male life expectancy by 5.3 years, confirming a persistent gender gap.
- People’s life expectancy tends to increase when the population is lower.
- The massive increase in contraceptive prevalence is an indicator of the decreasing birth rates and the slower population growth.
- The population between 20 and 29 reporting to be married have been decreasing steadily since the year 2000.
