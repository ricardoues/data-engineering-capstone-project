import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, unix_timestamp, to_date, lit
import pandas as pd
from pyspark.sql.types import IntegerType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DateType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import from_unixtime            
import pyspark.sql.functions as F
import sqlite3
from sqlite3 import Error


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def load_data(spark):
    """ Returns pyspark dataframs containing information about taxi trip and covid data in New York City. 
    
    """
    df_spark_yellow_taxi = spark.read.csv("s3a://ricrio/yellow_tripdata_*", header=True)

    df_spark_green_taxi = spark.read.csv("s3a://ricrio/green_tripdata_*", header=True)

    df_spark_uber_lyft_taxi = spark.read.csv("s3a://ricrio/fhvhv_tripdata_*", header=True)

    df_spark_covid = spark.read.csv("s3://ricrio/case-hosp-death.csv", header=True)

    return df_spark_yellow_taxi, df_spark_green_taxi, df_spark_uber_lyft_taxi, df_spark_covid


def clean_data(spark, df_spark_yellow_taxi, df_spark_green_taxi, df_spark_uber_lyft_taxi, df_spark_covid):
    """ Returns clean pyspark dataframes. 

    """
    
    df_spark_yellow_taxi_count = df_spark_yellow_taxi.select("tpep_pickup_datetime")
    df_spark_yellow_taxi_count = df_spark_yellow_taxi_count.withColumn("tpep_pickup_datetime", df_spark_yellow_taxi_count["tpep_pickup_datetime"].cast(TimestampType()))
    df_spark_yellow_taxi_count = df_spark_yellow_taxi_count.withColumn("tpep_pickup_datetime", df_spark_yellow_taxi_count["tpep_pickup_datetime"].cast(DateType()))
    df_spark_yellow_taxi_count =  df_spark_yellow_taxi_count.groupBy('tpep_pickup_datetime').count()

    df_spark_green_taxi_count = df_spark_green_taxi.select("lpep_pickup_datetime")
    df_spark_green_taxi_count = df_spark_green_taxi_count.withColumn("lpep_pickup_datetime", df_spark_green_taxi_count["lpep_pickup_datetime"].cast(TimestampType()))
    df_spark_green_taxi_count = df_spark_green_taxi_count.withColumn("lpep_pickup_datetime", df_spark_green_taxi_count["lpep_pickup_datetime"].cast(DateType()))
    df_spark_green_taxi_count =  df_spark_green_taxi_count.groupBy('lpep_pickup_datetime').count()

    df_spark_uber_taxi = df_spark_uber_lyft_taxi.where(col("hvfhs_license_num") == "HV0003").select("pickup_datetime")    
    df_spark_uber_taxi_count = df_spark_uber_taxi.withColumn("pickup_datetime", df_spark_uber_taxi["pickup_datetime"].cast(TimestampType()))
    df_spark_uber_taxi_count = df_spark_uber_taxi_count.withColumn("pickup_datetime", df_spark_uber_taxi_count["pickup_datetime"].cast(DateType()))     
    df_spark_uber_taxi_count =  df_spark_uber_taxi_count.groupBy('pickup_datetime').count() 

    df_spark_lyft_taxi = df_spark_uber_lyft_taxi.where(col("hvfhs_license_num") == "HV0005").select("pickup_datetime")
    df_spark_lyft_taxi_count = df_spark_lyft_taxi.withColumn("pickup_datetime", df_spark_lyft_taxi["pickup_datetime"].cast(TimestampType()))
    df_spark_lyft_taxi_count = df_spark_lyft_taxi_count.withColumn("pickup_datetime", df_spark_lyft_taxi_count["pickup_datetime"].cast(DateType()))
    df_spark_lyft_taxi_count =  df_spark_lyft_taxi_count.groupBy('pickup_datetime').count()
    
    df_spark_covid_count = df_spark_covid.select(["CASE_COUNT", "DATE_OF_INTEREST"])
    df_spark_covid_count = df_spark_covid_count.withColumn("DATE_OF_INTEREST", from_unixtime(unix_timestamp('DATE_OF_INTEREST', 'MM/dd/yyy')) )
    df_spark_covid_count = df_spark_covid_count.withColumn("CASE_COUNT", df_spark_covid_count["CASE_COUNT"].cast(IntegerType()))
    df_spark_covid_count = df_spark_covid_count.withColumn("DATE_OF_INTEREST", df_spark_covid_count["DATE_OF_INTEREST"].cast(TimestampType()))
    df_spark_covid_count = df_spark_covid_count.withColumn("DATE_OF_INTEREST", df_spark_covid_count["DATE_OF_INTEREST"].cast(DateType())) 

    # good reference https://stackoverflow.com/questions/31407461/datetime-range-filter-in-pyspark-sql

    dates = ("2020-02-29",  "2020-06-30")
    date_from, date_to = [to_date(lit(s)).cast(DateType()) for s in dates]

    df_spark_yellow_taxi_count = df_spark_yellow_taxi_count.where( (df_spark_yellow_taxi_count.tpep_pickup_datetime > date_from) & (df_spark_yellow_taxi_count.tpep_pickup_datetime < date_to) )
    df_spark_green_taxi_count = df_spark_green_taxi_count.where( (df_spark_green_taxi_count.lpep_pickup_datetime > date_from) & (df_spark_green_taxi_count.lpep_pickup_datetime < date_to) )
    df_spark_uber_taxi_count = df_spark_uber_taxi_count.where( (df_spark_uber_taxi_count.pickup_datetime > date_from) & (df_spark_uber_taxi_count.pickup_datetime < date_to) )
    df_spark_lyft_taxi_count = df_spark_lyft_taxi_count.where( (df_spark_lyft_taxi_count.pickup_datetime > date_from) & (df_spark_lyft_taxi_count.pickup_datetime < date_to) )
    df_spark_covid_count = df_spark_covid_count.where( (df_spark_covid_count.DATE_OF_INTEREST > date_from)  & ( df_spark_covid_count.DATE_OF_INTEREST < date_to) ) 

    yellow = df_spark_yellow_taxi_count.toPandas()
    green = df_spark_green_taxi_count.toPandas()
    uber = df_spark_uber_taxi_count.toPandas()
    lyft = df_spark_lyft_taxi_count.toPandas() 
    covid = df_spark_covid_count.toPandas()     
 
    return yellow, green, uber, lyft, covid 


def save_data(yellow, green, uber, lyft, covid):
    """ Saves cleaned data to a PostgreSQL database. 

    """
 
 
    try: 
        yellow.columns = ['DATE_OF_INTEREST', 'count']
        green.columns = ['DATE_OF_INTEREST', 'count']
        uber.columns = ['DATE_OF_INTEREST', 'count']
        lyft.columns = ['DATE_OF_INTEREST', 'count']

        yellow['economic_item'] = 'Yellow taxi'
        green['economic_item'] = 'Green taxi'
        uber['economic_item'] = 'Uber taxi'
        lyft['economic_item'] = 'Lyft taxi'
        covid['event'] = 'COVID-19'
      
        # we will combine the taxi data.   
        combined_data = pd.concat([yellow, green, uber, lyft], ignore_index=True)  

        # we will join the taxi data with the covid data.
        final_data = pd.merge(combined_data, covid, on='DATE_OF_INTEREST', how='inner')
        final_data.columns = ["date_of_interest", "number_of_taxis", "economic_item", "covid_cases", "event"] 

        if not (yellow.shape[0] == green.shape[0] == uber.shape[0] == lyft.shape[0] == covid.shape[0]):
           raise Exception("The number of rows in the dataframes that represents taxi and covid data  must be equals.")

        if yellow.shape[0] == 0:
           raise Exception("The number of rows must be greater than zero") 

        conn = sqlite3.connect('taxi_covid_nyc.db')
      
        conn.execute('''
         CREATE TABLE IF NOT EXISTS 
         economic_item(name varchar(100) not null primary key,
                       description text default null ); 
         ''')

        conn.execute("INSERT INTO economic_item(name) \
         VALUES ('Yellow taxi')");


        conn.execute("INSERT INTO economic_item(name) \
         VALUES ('Green taxi')");


        conn.execute("INSERT INTO economic_item(name) \
         VALUES ('Uber taxi')");


        conn.execute("INSERT INTO economic_item(name) \
         VALUES ('Lyft taxi')");

        conn.execute('''
         CREATE TABLE IF NOT EXISTS 
         day_of_interest( day text not null primary key, comment text default null  ); 
        ''')
          
        for ind in covid.index:

           day_of_interest = covid['DATE_OF_INTEREST'][ind]

           conn.execute("INSERT INTO day_of_interest(day) \
            VALUES ('{}')".format(day_of_interest)); 

        conn.execute('''
        CREATE TABLE IF NOT EXISTS event(
        name varchar(100) not null primary key, 
        description text  default null  );                                                  
        ''')
 

        conn.execute("INSERT INTO event(name) \
         VALUES ('COVID-19')");

     
        conn.execute('''
        CREATE TABLE IF NOT EXISTS 
        economic_item_event_day(
        name_economic_item varchar(100) not null,
        day_of_interest date not null,  
        name_event varchar(100) not null, 
        count_economic_item integer not null, 
        count_event integer not null,  
        PRIMARY KEY(name_economic_item, day_of_interest, name_event), 
        FOREIGN KEY(name_economic_item) REFERENCES economic_item(name),  
        FOREIGN KEY(day_of_interest)     REFERENCES day_of_interest(day), 
        FOREIGN KEY(name_event)  REFERENCES event(name) ); 

        ''')


        for ind in final_data.index:
           day_of_interest = final_data['date_of_interest'][ind]
           number_of_taxis = final_data['number_of_taxis'][ind]  
           economic_item = final_data['economic_item'][ind]  
           covid_cases = final_data['covid_cases'][ind]  
           event = final_data['event'][ind]  

           
           conn.execute("INSERT INTO economic_item_event_day(name_economic_item, day_of_interest, name_event, count_economic_item, count_event) \
            VALUES ('{}', '{}', '{}', '{}', '{}')".format(economic_item, day_of_interest, event, number_of_taxis, covid_cases )) 

           conn.commit() 

    except Error as e:
        print(e)       
    finally: 
        if conn:
            conn.close()
    
   

def main():
    spark = create_spark_session()
    df_spark_yellow_taxi, df_spark_green_taxi, df_spark_uber_lyft_taxi, df_spark_covid=load_data(spark) 
    yellow, green, uber, lyft, covid = clean_data(spark, df_spark_yellow_taxi, df_spark_green_taxi, df_spark_uber_lyft_taxi, df_spark_covid)

    save_data(yellow, green, uber, lyft, covid) 
    print("ETL pipeline completed")  

    spark.stop()

if __name__ == "__main__":
    main()
