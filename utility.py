# Import Libraries
import pandas as pd
from pyspark.sql.functions import udf, monotonically_increasing_id, date_format, col, upper, first, count, when , isnan,  dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, FloatType
import datetime as dt
import utility

import seaborn as sns
import matplotlib.pyplot as plt
import os
import configparser
import plotly.plotly as py
import plotly.graph_objs as go
import requests
requests.packages.urllib3.disable_warnings()

# create a udf to convert date to datetime object
get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

# function to identify columns with missing values and display in a table format
def  view_calculate_missing_vals(df):
    # View columns with missing data
    nulls = pd.DataFrame(data= df.toPandas().isnull().sum(), columns=['values'])
    nulls = nulls.reset_index()
    nulls.columns = ['cols', 'values']

    # calculate % missing values
    nulls['% missing values'] = 100 * nulls['values']/df.count()
    return nulls[nulls['% missing values'] > 0]

def plot_missing_vals(df):
    """Plotting missing values in a Spark dataframe
    
    Args: 
    df - A Spark dataframe
    """
    # create a dataframe with missing values count per column
    nan_count = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
    
    # pivot dataframe
    nan_count = pd.melt(nan_count, var_name='Columns', value_name='values')
    
    # count total records in dataframe
    total = df.count()
    
    # add % missing values column
    nan_count['Percentage of missing values'] = 100 * nan_count['values']/total
    
    plt.rcdefaults();
    plt.figure(figsize=(5,3));
    ax = sns.barplot(x="Columns", y="Percentage of missing values", data=nan_count);
    ax.set_ylim(0, 100);
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90);
    plt.show();
    
 # a udf to parse latitude column
def parse_lat(lat):
    split_lat = lat.strip().split(',')
    return float(split_lat[0])
udf_parse_lat = udf(lambda x: parse_lat(x), FloatType())

# a udf to parse longitude column
def parse_long(long):
    split_long = long.strip().split(',')
    return float(split_long[1])
udf_parse_long = udf(lambda x: parse_long(x), FloatType())

# a udf to parse state column
def parse_state(state):
    return state.strip().split('-')[-1]
udf_parse_state = udf(lambda x: parse_state(x), StringType())

#  a function to write tables in parquet
def create_table(df, partition_columns, directory_name, output_data):
    """This function creates a table from  different  data sources and writes
    them to destination (output_data directory) in parquet format.
    
    Args:
    df - spark dataframe 
    partition_columns - list of columns to use as partition key for each dimension table
    directory_name - name of the directory to place dimension table under output_data directory
    return spark dataframe 
    """
    # write dimension to parquet file
    df.write.parquet(output_data + directory_name, partitionBy=partition_columns, mode="overwrite")
    
    return df

# a function to clean immigration table
def clean_immigration(immigration):
    # list of columns with missing values that need to be dropped
    cols = ['occup', 'entdepu', 'insnum']
    
    # drop columns
    immigration = immigration.drop(*cols)
    
    # rename columns to align with data model
    immigration = immigration.withColumnRenamed('cicid','record_id') \
        .withColumnRenamed('i94cit','country_birth_code') \
        .withColumnRenamed('i94res', 'country_residence_code') \
        .withColumnRenamed('i94port', 'admission_port') \
        .withColumnRenamed('arrdate', 'arrival_date') \
        .withColumnRenamed('i94mode', 'transportation_mode') \
        .withColumnRenamed('i94addr', 'state_code') \
        .withColumnRenamed('depdate', 'departure_date') \
        .withColumnRenamed('i94bir', 'age') \
        .withColumnRenamed('i94visa', 'visa_code') \
        .withColumnRenamed('dtadfile', 'date_file_added') \
        .withColumnRenamed('visapost', 'visa_post') \
        .withColumnRenamed('occup', 'occupation') \
        .withColumnRenamed('entdepa', 'arrival_flag') \
        .withColumnRenamed('entdepd', 'departure_flag') \
        .withColumnRenamed('entdepu', 'update_flag') \
        .withColumnRenamed('matflag', 'match_flag') \
        .withColumnRenamed('biryear', 'birth_year') \
        .withColumnRenamed('dtaddto', 'allowed_stay_until') \
        .withColumnRenamed('gender', 'gender') \
        .withColumnRenamed('admnum', 'admission_number') \
        .withColumnRenamed('fltno', 'flight_number') \
        .withColumnRenamed('visatype', 'visa_type') \
        .withColumnRenamed('insnum', 'ins_number')  \
        .withColumnRenamed('i94yr', 'arrival_year') \
        .withColumnRenamed('i94mon', 'arrival_month')
    
    # cast float data types to int and dates to datetime format
    immigration = immigration.withColumn('record_id', immigration['record_id'].cast(IntegerType())) \
                        .withColumn('arrival_year', immigration['arrival_year'].cast(IntegerType())) \
                        .withColumn('arrival_month', immigration['arrival_month'].cast(IntegerType())) \
                        .withColumn('country_birth_code', immigration['country_birth_code'].cast(IntegerType())) \
                        .withColumn('country_residence_code', immigration['country_residence_code'].cast(IntegerType())) \
                        .withColumn('birth_year', immigration['birth_year'].cast(IntegerType())) \
                        .withColumn("arrival_date", get_datetime(immigration['arrival_date'])) \
                        .withColumn("departure_date", get_datetime(immigration['departure_date']))
    return immigration

# a function to clean demographics dataframe
def clean_demographics(demographics):
    # drop rows with missing values
    subset_cols = [
        'Male Population',
        'Female Population',
        'Number of Veterans',
        'Foreign-born',
        'Average Household Size'
    ]
    
    # drop rows with missing values
    demographics = demographics.dropna(subset=subset_cols)
    
    # rename columns to lower case
    demographics = demographics.withColumnRenamed('City','city') \
        .withColumnRenamed('State','state') \
        .withColumnRenamed('Median Age','median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('Average Household Size', 'avg_household_size') \
        .withColumnRenamed('State Code', 'state_code') \
        .withColumnRenamed('Race', 'race') \
        .withColumnRenamed('Count', 'count') 

    # lets add an id column
    demographics = demographics.withColumn('id', monotonically_increasing_id())
    return demographics

# function to clean aiport dataframe
def clean_airport(df):
    # drop iata_code column
    df = df.drop('iata_code')
    
    # parsing latitude, longitude and state columns
    df = df.withColumn("airport_latitude", udf_parse_lat("coordinates")) \
                        .withColumn("airport_longitude", udf_parse_long("coordinates")) \
                        .withColumn("state", udf_parse_state("iso_region")) \
                        .withColumnRenamed("municipality", "city") \
                        .withColumnRenamed("ident", "id") \
                        .filter("iso_country == 'US'") \
                        .drop("coordinates", "gps_code", "local_code", "continent", "iso_region", "iso_country")
    return df

# a function to clean weather dataframe
def clean_weather(df):
    # filtering the data for only United States 
    df = df.filter("Country == 'United States'")
    
    # list of columns with null values
    subset_cols = ['AverageTemperature', 'AverageTemperatureUncertainty']
    
    # drop rows with missing values
    df = df.dropna(subset=subset_cols)
    
    # rename columns to align with model, add id column and drop country column
    df = df.withColumnRenamed('dt', 'datetime') \
                 .withColumnRenamed('AverageTemperature', 'avg_temp') \
                 .withColumnRenamed('AverageTemperatureUncertainty', 'avg_temp_uncertainty') \
                 .withColumnRenamed('State', 'state')\
                 .withColumn('id', monotonically_increasing_id()) \
                 .drop('Country')
    # creating a year column based on datetime column
    df = df.withColumn('year', year('datetime')) \
                .withColumn('month', month('datetime'))
    
    df = df.filter("year = 2013").drop('year')
    return df

# a function to create time table 
def create_time_table(immigration):
    # create initial calendar df from arrdate column
    time = immigration.withColumn('arrival_date', get_datetime(immigration['arrdate'])).select('arrival_date').distinct()
    
    # expand df by adding other time columns
    time = time.withColumn('arrival_day', dayofmonth('arrival_date')) \
        .withColumn('arrival_week', weekofyear('arrival_date')) \
        .withColumn('arrival_month', month('arrival_date')) \
        .withColumn('arrival_year', year('arrival_date')) \
        .withColumn('arrival_weekday', dayofweek('arrival_date')) \
        .withColumn('id', monotonically_increasing_id())
    
    return time

# a function to check for data quality
def quality_checks(df, table):
    """Count checks on the tables to ensure completeness of data.
    Args:
    df - A Spark dataframe to check row counts 
    """
    total_count = df.count()

    if total_count == 0:
        print(f"Data quality check failed for {table} table  with zero records.")
    else:
        print(f"Data quality check passed for {table} table with {total_count:,} records.")
    return 0


