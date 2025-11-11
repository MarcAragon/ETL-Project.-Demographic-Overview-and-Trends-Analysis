from airflow.decorators import task
from sqlalchemy import create_engine, Column, Integer, String, Float, inspect, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

@task
def Create_Tables():

    DatabaseURL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow" #5432 exposed port for postgresql db in .yaml
    Engine = create_engine(DatabaseURL, echo=True)
    Base = declarative_base()
    Inspector = inspect(Engine)

    TableName = 'countries'

    if not Inspector.has_table(TableName):

        class Countries(Base):
            __tablename__ = TableName
            
            iso_name = Column(String(200), nullable = False)
            iso_alpha_code = Column(String(3))
            iso_numeric_code = Column(Integer, primary_key=True) 
        
        print(f'Tabla {TableName} creada')
    

    TableName = 'marrieddata'

    if not Inspector.has_table(TableName):

        class MarriedData(Base):
            __tablename__ = TableName
            
            survey_id = Column(Integer, primary_key=True)
            country = Column(String(400), nullable = False)
            iso_code = Column(Integer, ForeignKey('countries.iso_numeric_code'), nullable = False)
            year_end = Column(Integer, nullable = False)
            sex = Column(String(100))
            age_group = Column(String(10))
            data_value = Column(Float, nullable = False)

            MarriedCodes = relationship('Countries')

        print(f'Tabla {TableName} creada')

    TableName = 'singledata'

    if not Inspector.has_table(TableName):

        class SingleData(Base):
            __tablename__ = TableName
            
            survey_id = Column(Integer, primary_key=True)
            country = Column(String(400), nullable = False)
            iso_code = Column(Integer, ForeignKey('countries.iso_numeric_code'), nullable = False)
            year_end = Column(Integer, nullable = False)
            sex = Column(String(100))
            age_group = Column(String(10))
            data_value = Column(Float, nullable = False)

            SingleCodes = relationship('Countries')
        
        print(f'Tabla {TableName} creada')

    TableName = 'divorceddata'

    if not Inspector.has_table(TableName):

        class DivorcedData(Base):
            __tablename__ = TableName
            
            survey_id = Column(Integer, primary_key=True)
            country = Column(String(400), nullable = False)
            iso_code = Column(Integer, ForeignKey('countries.iso_numeric_code'), nullable = False)
            year_end = Column(Integer, nullable = False)
            sex = Column(String(100))
            age_group = Column(String(10))
            data_value = Column(Float, nullable = False)

            DivorcedCodes = relationship('Countries')

        print(f'Tabla {TableName} creada')

    TableName = 'indicatorsdata'

    if not Inspector.has_table(TableName):

        class IndicatorsData(Base):
            __tablename__ = TableName
            
            id = Column(Integer, primary_key=True)
            country_name = Column(String(400), nullable = False) 
            country_code = Column(Integer, ForeignKey('countries.iso_numeric_code'), nullable = False, primary_key=True)
            year = Column(Integer, nullable = False)
            birth_rate_crude_per_1000_people = Column(Float)
            cause_of_death_by_injury_per_of_total = Column(Float)
            death_rate_crude_per_1000_people = Column(Float)
            fertility_rate_total_births_per_woman = Column(Float)
            gini_index = Column(Float)
            hospital_beds_per_1000_people = Column(Float)
            life_expectancy_at_birth_female_years = Column(Float)
            life_expectancy_at_birth_male_years = Column(Float)
            mortality_rate_neonatal_per_1000_live_births = Column(Float)
            population_growth_annual_percentage = Column(Float)
            population_living_in_slums_per_of_urban_population = Column(Float)
            pregnant_women_receiving_prenatal_care_percent = Column(Float)
            inflation_consumer_prices_annual_percent = Column(Float)
            contraceptive_prevalence_any_method_percent_of_married_women_ages_15_to_49 = Column(Float)
            age_at_first_marriage_Male = Column(Float)
            age_at_first_marriage_Female = Column(Float)

            IndicatorCodes = relationship('Countries')
        
        print(f'Tabla {TableName} creada')
    
    Base.metadata.create_all(Engine)

