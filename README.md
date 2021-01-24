# ETL pipeline -Udacity DEND capstone project

#### Project Summary
The project goal is to build an ETL pipeline by gather desparate data in different sources, and transform and load to Amazon S3 cluster. In this project, I will create a clean dataset that consists of the US immigration data, US state demographics, weather information and airport data. The result dataset could be used for further analysis of identifying trends in travel and immigration to the US. 

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

#### File structure
- Capstone Project.ipynb - where you can find etl steps in Jupyter notebok
- etl.py - where you can find etls steps in precise Python code
- utility.py - where you can find utility functions to support the etl process in Python code

#### Instructions
In order to run the project, download datasets from soruces listed below. I have read immigration data to local sas_data file. Code to write in parquet is provided in Capstone Project.ipynb (commented out). You will also need to create local output directory if you choose to write processed data in your local directory.If you either reading or writing to Amazon S3, create a config file and add your credentials (code is provided in etl.py-commented out) and update input_data and output_data variables to point to relevant addressed in etl.py main function.

#### Scope 
In this project, I will create a clean dataset that consists of the US immigration data, US state demographics, weather information and airport data. The result dataset could be used for further analysis of identifying trends in travel and immigration to the US. I used Apache Spark to process the data. I also used Python, Pandas, Matplotlib and Pyspark. This version of the project reads and writes to local directory. However, it can easily be updated to read data from Amazon S3 and write back the processed data to Amazon S3. To do so, will need to point input_data and output_data variables to proper address on S3. Whole project can be run through this note book or etl.py.

#### Describe and Gather Data 
*I94 Immigration Data:* This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.

*World Temperature Data:* This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data). I used temperature data *by State* version.

*U.S. City Demographic Data:* This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

*Airport Code Table:* This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data).

#### Conceptual Data Model
For my data model, I have chonse the Star Schema, where immigration table will serve as a fact table and all other dimension tables share common key with immigration table. I chose the Star Schema because it makes the query performance faster and easier for data analyst to work with. 

Demographics dimension table is connected to immigration table through `state_code` column. Airport dimension table is connected to immigration table through `state_code` column. Time dimension table is connected to immigration table trough `arrival_date` column. 

Weather dimension table is connected to immigration table through `state` column. Initially, I used 'GlobalWeatherTrendsByCity' data. However, I realized that I would not be able to connect the weather data with the fact table. Immigration and weather by city data did not share a common key. The second potential option was to use a Snowflake Schema to connect city with city column in Airport or Demographics data. However, this option was not practical either. Weather by city data did not have state column. This would make it impossible to identify a city, as there are many city names are repeated within different states. 

#### Mapping Out Data Pipelines
A list steps necessary to pipeline the data into the chosen data model

1. Turn on Spark
2. Load immigration data
2. Clean immigration data
3. Create a table and write immigration data to destination in parquet format
4. Check for data quality in immigration table
5. Load demographics data
6. Clean demographics data
7. Create a table and write demographics data to destination in parquet format
8. Chek for data quality in demographics data
9. Load airport data
10. Clean airport data
11. Create a table and write airport data to destination in parquet format
12. Check for data quality in airport data
13. Load weather data
14. Clean weather data
15. Create a table and write weather data to destination in parquet format
16. Check for data quality in weather data
17. Load immigration data
18. Create time dataframe based on immigration table\'s arrival_date column
19. Create a table and write time data to destination in parquet format
20. Check for data quality in time data

#### Data dictionary 
Below is a data dictionary for my data model. For each field, I provided a brief description of what the data is and where it came from. 

*Immigration - fact table* - Data comes from he [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html).
- record_id: Immigration record id
- arrival_year: Arrival date of visitors
- arrival_month: Arrival month
- country_birth_code: Birth country of visitors- defined in country codes
- country_residence_code: Residency country of visitors - defined in country codes
- admission_port: Port that admitted visitors
- arrival_date: Arrival date
- transportation_mode: Type of transportation used to arrive to the US
- state_code: State a visitor visiting
- age: Age of visitors
- visa_code: Visa code
- count: Aggerate count
- date_file_added: Date when the i94 added to the files
- visa_post: Port that issued visa 
- arrival_flag: Admitted or paroled into the U.S.
- departure_flag: Departed, lost I-94 or is deceased
- match_flag: Either apprehended, overstayed, adjusted to perm residence
- birth_year: Year of visitors births
- allowed_stay_until: Date until a visitor allowed to stay in the US
- gender: Gender of visitor
- airline: Airline used to arrive to the US
- admission_number: Admission number
- flight_number: Flight number taken to arrive to the US
- visa_type: Type of visa a visitor arrived to the US with

*Demographics - dimension table* - Data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
- id: Unique id
- city: Name of a US city
- state: Name of a US state
- median_age: Median age in a particular US city
- male_population: Number of male population
- female_population: Number of female population
- total_population: Number of total population
- number_of_veterans: Number of veterans
- foreign_born: Number of city residents born in a foreign country
- avg_household_size: Size of average household in a particular US city
- state_code: Two letter abbreviation used for US states
- race: Race of city residents
- count: Aggregate count

*Airport - dimension table*
- id: Unique id
- type: Type of an airport
- name: Name of an airport
- elevation_ft: Elevation of an airport measured in US feet
- city: Name of a city where an airport is located
- airport_latitude: Latitude coordinates
- airport_longitude: Longitude coordinates
- state: State where an airport is located

*Weather - dimension table* - Data comes from [Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
- datetime: Date when the weather recorded
- avg_temp: Average temperatures for a given US state
- avg_temp_uncertainty: Average temperature uncertainty for a given US state
- state: US state name
- id: Unique id
- month: Month when the weather recorded

*Time - dimension table* - This dimension is created based on immigration data\'s `arrival_date` column
- id: Unique id
- arrival_date: date of arrival
- arrival_day: Day of arrival 
- arrival_week: Week of arrival 
- arrival_month: Month of arrivel 
- arrival_year: Year of arrival
- arrival_weekday: Week day of arrival# ETL-pipeline--Udacity-DEND-capstone-project
