# ETL Project: Demographic Overview and Vital Trends in Latin America: Births, Health and Mortality

## Context

In the current context, government entities and international organizations, play a crucial role in monitoring and analyzing demographic and health indicators that are essential for formulating effective public policies. In particular, the analysis of vital trends, such as births, health, and mortality, has become a priority for Latin American governments seeking to improve the quality of life of their citizens and optimize resources allocated to public health and social well-being.

Our main objective is to provide a clear and accurate view of demographic and vital trends in Latin America, thereby helping public policymakers make evidence-based decisions that promote the well-being and sustainable development of the region.

## Solution


### About the dataset

The World Bank offers a vast historical database covering a wide range of global economic, social, environmental, and financial indicators. Its scope ranges from macroeconomic indicators to sector-specific variables such as health and infrastructure, allowing us to easily access the fields we need for our analysis, these are the ones chosen:

- Birth rate, crude (per 1,000 people)
- Cause of death, by injury (% of total)
- Death rate, crude (per 1,000 people)
- Fertility rate, total (births per woman)
- Gini index
- Hospital beds (per 1,000 people)
- Life expectancy at birth, female (years)
- Life expectancy at birth, male (years)
- Mortality rate, neonatal (per 1,000 live births)
- Population growth (annual %)
- Population living in slums (% of urban population)
- Pregnant women receiving prenatal care (%)

### ETL pipeline diagram

![Link](https://i.imgur.com/TP8Mx3c.jpeg)


### Tech stack

**Data Proccessing & Loading:** Python.

**DB:** Google BigQuery.

**Data Analysis :** Looker Studio.

### DDM (Dimensional Data Model)

![Link](https://i.imgur.com/M5lUWIR.jpeg)


### [Results](https://lookerstudio.google.com/reporting/c4e76900-fbbf-4807-9c1e-60249957477a)


### Conclusions

- Fertility in South America dropped from more than 6 children per woman in the 1960s to about 2 today.
- The birth rate per 1,000 inhabitants declined steadily, reflecting demographic transition.
- Neonatal mortality decreased sharply in parallel with the expansion of prenatal care.
- Countries with >90% prenatal care coverage show an average life expectancy about 5 years higher.
- Population growth has moderated: natural growth = 19‰ and annual growth = 1.41%.
- In countries with higher prenatal care coverage, births per 1,000 inhabitants are higher than in those with lower coverage.
- The average Gini index (49.17) reflects high inequality in the region.
- Approximately 23.5% of the urban population lives in slums.
- Hospital infrastructure is limited compared to mortality (beds-to-deaths ratio = 0.34).
- Female life expectancy exceeds male life expectancy by 5.3 years, confirming a persistent gender gap.
