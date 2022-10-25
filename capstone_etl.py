#Import Librarires
import pandas as pd
import os
import configparser
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from datetime import datetime, timedelta, date
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear


# Read Credentials
config = configparser.ConfigParser()
config.read('conf.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAVFTAEVLOUKR2IUU4'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'


"""
Creating a Apache spark session on AWS to process the input data.
Output: Spark session.
"""
def createSparkSession():
    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .enableHiveSupport().getOrCreate()
#     #importing pandas to a session. It is not installed by default
#     sc.install_pypi_package("pandas==0.25.1")
#     import pandas as pd
    
    return spark


"""
This function converts SAS dates into datetime
"""
def convertSasDate(d):
    if(type(d)==int or type(d)==float):
        d = d
    else:
        d = 0
    return(pd.to_timedelta(d, unit='D') + pd.Timestamp('1960-1-1'))



"""
Processing CIC immigration data to create an Immigration fact table
"""
def createImmigrationTable(spark, inputPath):
    df_immg = spark.read.parquet(inputPath)
    
    # Cleanining immigration data
    convertSasDateUDF = udf(lambda x: convertSasDate(x),T.DateType())
    df_immg=df_immg.na.drop(subset=['i94port'])
    df_immg = df_immg.withColumn('arrdate',convertSasDateUDF(df_immg.arrdate))

    
    # Extract Immigration data
    df_immg.createOrReplaceTempView("immigration_stage")
    immigration = spark.sql("""
        SELECT 
            cicid,
            I94mon,
            i94port,
            arrdate,
            i94addr,
            CASE
                WHEN i94visa = 1 THEN 'Business'
                WHEN i94visa = 2 THEN 'Pleasure'
                WHEN i94visa = 3 THEN 'Student'
                ELSE 'Not Defined'
            END as visa,
            biryear,
            gender,
            visatype
        FROM immigration_stage 
        LIMIT 1000000
    """)
    
    return immigration


"""
Processing Airport data to create a dimensional table airports
"""
def createAirportsTable(spark, inputPath):
    # Extract airport data
    df_airports = spark.read.format('csv').options(header='true').load(inputPath)
    #cleaning airport data
    df_airports=df_airports.na.drop(subset=['iata_code'])
    df_airports.createOrReplaceTempView("airports_stage")
    airports = spark.sql("""
        SELECT 
            *
        FROM airports_stage 
        where iata_code is not null
    """)
    
    return airports


"""
Processing demografic data to create a dimensional table demografy
"""
def create_deografic_table(spark, inputPathDemograf, inputPathIata):
    # Extract Demografic Data
    df_demograf = spark.read.format('csv').options(header='true', sep=';',inferSchema=True).load(inputPathDemograf)
    df_iateCodes = spark.read.format('csv').options(header='true').load(inputPathIata)
    # Cleaning Demografic data
    df_demograf=df_demograf.na.drop(subset=['City','State'])
    #remove City-State duplicates
    df_demograf = df_demograf.drop('Race','Count')
    df_demograf = df_demograf.distinct()
    df_iateCodes.createOrReplaceTempView("iata_codes")
    df_demograf.createOrReplaceTempView("demograf_stage")
    demografy = spark.sql("""
        SELECT 
            demograf.City as city,
            demograf.State as state,
            demograf.`Median Age` as median_age,
            demograf.`Male Population` as male_population,
            demograf.`Female Population` as female_population,
            demograf.`Total Population` as total_population,
            demograf.`Number of Veterans` as num_of_veterans,
            demograf.`Foreign-born` as foreign_born,
            demograf.`Average Household Size` as avg_household_size,
            demograf.`State Code` as state_code,    
            iata.Code as code
        FROM demograf_stage AS demograf
        LEFT JOIN iata_codes AS iata
        ON demograf.city=iata.City and demograf.`State Code`=iata.State
    """)
    
    return demografy


"""
Load processed data to the S3 data lake as a set of parquete files
"""
def loadDataToDatalake(immigrationData,airportsData,demografyData,outputPath):
    demografyData.write.mode('overwrite').partitionBy('State').parquet(outputPath+'demografy.parquet')
    airportsData.write.mode('overwrite').parquet(outputPath+'airports.parquet')
    immigrationData.write.mode('overwrite').partitionBy('I94mon').parquet(outputPath+'immigration.parquet')
    

    
"""
Run quality checks
"""
def qualityCheck(spark,immigrationData,airportsData,demografyData):
    demografyData.createOrReplaceTempView("demografy")
    airportsData.createOrReplaceTempView("airports")
    immigrationData.createOrReplaceTempView("immigration")

    print("Is immmigration empty?")
    spark.sql("""
        Select 
           count(*)
        FROM
            immigration
    """).show()

    print("Is airports empty?")
    spark.sql("""
        Select 
           count(*)
        FROM
            airports
    """).show()

    print("Is demografy empty?")
    spark.sql("""
        Select 
           count(*)
        FROM
            demografy
    """).show()

    print("tables can join?")
    spark.sql("""
        Select 
            i.i94port,
            i.visa,
            a.iata_code,
            a.municipality,
            d.code,
            d.city,
            d.state
        FROM
            immigration i,
            airports a,
            demografy d
        where i.i94port=a.iata_code and i.i94port=d.code 
        and i94port is not null
        LIMIT 10
    """).show()

    print("Tables can join count")
    spark.sql("""
        Select 
            count(*)
        FROM
            immigration i,
            airports a,
            demografy d
        where i.i94port=a.iata_code and i.i94port=d.code 
        and i.i94port is not null
    """).show()
    
    
def main():
    spark = createSparkSession()
    
    #reading data
    immigr_datapath = 's3a://udac-capstone-input/immigration.parquet'
    airport_datapath = 's3a://udac-capstone-input/airport-codes_csv.csv'
    demograf_datapath = 's3a://udac-capstone-input/us-cities-demographics.csv'
    iata_datapath = 's3a://udac-capstone-input/IATA_codes.csv'
    s3_output = 's3a://udac-capstone-output/'
    
    #processing and creating data tables
    immigration = createImmigrationTable(spark,immigr_datapath)
    airports = createAirportsTable(spark,airport_datapath)
    demografy = create_deografic_table(spark,demograf_datapath,iata_datapath)
    
    #loading data to a data lake
    loadDataToDatalake(immigration,airports,demografy,s3_output)
    
    #running quality checks
    qualityCheck(spark,immigration,airports,demografy)
    
    
if __name__ == "__main__":
    main()