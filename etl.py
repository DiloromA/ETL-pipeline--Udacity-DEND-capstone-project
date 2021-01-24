import pandas as pd
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import utility

# config = configparser.ConfigParser()
# config.read('config.cfg')

# os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

def process_immigration_data(spark, input_data, output_data):
    """
    Processes immigration data by loading, cleaning, writing it to destionation and conducting data quality checks.
    Args:
    spark - spark session
    input_data - address where to load data from
    output_data - address where to write data to
    """
    # Read in the immigration data here
    immigration = spark.read.parquet(input_data)
    
    # clean the data
    immigration = utility.clean_immigration(immigration)
    
    # create and write immigration_dim table to output data in parquet format
    immigration_fact = utility.create_table(immigration,['arrival_year', 'arrival_month'], 'immigration', output_data)
    
    print('Immigration data has been processed.')
    
    # run data quality check
    utility.quality_checks(immigration_fact, 'immigration')
    
def process_demographics_data(spark, input_data, output_data):
    """
    Processes demographics data by loading, cleaning, writing it to destionation and conducting data quality checks.
    Args:
    spark - spark session
    input_data - address where to load data from
    output_data - address where to write data to
    """
    # Read in the US cities demographics data here
    demographics = spark.read.csv(input_data, sep=';', inferSchema=True, header=True)
    
    #clean the data
    demographics = utility.clean_demographics(demographics)
    
    # create and write demographics_dim table to output data in parquet format
    demographics_dim = utility.create_table(demographics,['state_code', 'city'], 'demographics', output_data)
    
    print('Demographics data has been processed.')
    
    # run data quality check
    utility.quality_checks(demographics_dim, 'demographics')
    
def process_airport_data(spark, input_data, output_data):
    """
    Processes airport data by loading, cleaning, writing it to destionation and conducting data quality checks.
    Args:
    spark - spark session
    input_data - address where to load data from
    output_data - address where to write data to
    """
    # Read in the airport data here
    airport = spark.read.csv(input_data, sep=',', header=True)
    
    # clean the data
    airport = utility.clean_airport(airport)
    
    # create and write airport_dim table to output data in parquet format
    airport_dim = utility.create_table(airport, ['state', 'city'], 'aiport', output_data)
    
    print('Airport data has been processed.')
    
    # run data quality check
    utility.quality_checks(airport_dim, 'aiport')
    
def process_weather_data(spark, input_data, output_data):
    """
    Processes weather data by loading, cleaning, writing it to destionation and conducting data quality checks.
    Args:
    spark - spark session
    input_data - address where to load data from
    output_data - address where to write data to
    """
    # read weather data here
    weather = spark.read.csv(input_data,sep=',', header=True)
    
    # clean the data
    weather = utility.clean_weather(weather)
    
    # create and write weather_dim table to output data in parquet format
    weather_dim = utility.create_table(weather, ['month', 'state'], 'weather', output_data)

    print('Weather data has been processed.')
    
    # run data quality check
    utility.quality_checks(weather_dim, 'weather')
    
def process_time_data(spark, input_data, output_data):
    """
    Processes time data by loading immigration data, creating time dataframe based on immigration dataframe,
    cleaning, writing it to destionation and conductingdata quality checks.
    Args:
    spark - spark session
    input_data - address where to load immigration data from
    output_data - address where to write time data to
    """
    # Read in the immigration data here
    immigration = spark.read.parquet(input_data)
    
    # create the table
    time = utility.create_time_table(immigration)
    
    # create and write time_dim table to output data in parquet format
    time_dim = utility.create_table(time, ['arrival_year', 'arrival_month', 'arrival_week'], 'time', output_data)
    
    print('Time data has been processed.')
    
    # run data quality check
    utility.quality_checks(time_dim, 'time')
    
def main():
    """
    Runs the etl pipleine
    """
    # define source data
    immigration_input_data = 'sas_data'
    demographics_input_data = 'us-cities-demographics.csv'
    airport_input_data = 'airport-codes_csv.csv'
    weather_input_data = 'GlobalLandTemperaturesByState.csv'
    
    # define destination directory where tables will be written
    output_data = 'output/'
    
    # run the etl pipeline
    process_immigration_data(spark, immigration_input_data, output_data)
    process_demographics_data(spark, demographics_input_data, output_data)
    process_airport_data(spark, airport_input_data, output_data)
    process_weather_data(spark, weather_input_data, output_data)
    process_time_data(spark, immigration_input_data, output_data)
            
    print('ETL process is complete.')
    
if __name__ == "__main__":
    main()