from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from datetime import datetime
import sys
import configparser


config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
data_lake_name=config.get("general","data_lake_name") 

## send this argument in the job configuration
#username=sys.argv[1]
username='dipti.dash'
print("Running job as Username: ",username)

## Updating the data type to date
spark = SparkSession \
    .builder \
    .appName("WEATHER ETL") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()


raw_df = spark.read.format("iceberg") \
    .load("demo_raw.bronze_weather_iceberg_data")

## Updating the data type to date
clean_df = raw_df \
    .filter(col("TEMP").isNotNull()) \
    .withColumn("date", to_date(col("DATE"), "yyyy-MM-dd")) \
    .select(
    "STATION",
    "DATE",
    "LATITUDE",
    "LONGITUDE",
    "ELEVATION",
    "NAME",
    "TEMP",
    "TEMP_ATTRIBUTES",
    "DEWP",
    "DEWP_ATTRIBUTES",
    "SLP",
    "SLP_ATTRIBUTES",
    "STP",
    "STP_ATTRIBUTES",
    "VISIB",
    "VISIB_ATTRIBUTES",
    "WDSP",
    "WDSP_ATTRIBUTES",
    "MXSPD",
    "GUST",
    "MAX",
    "MAX_ATTRIBUTES",
    "MIN" ,
    "MIN_ATTRIBUTES",
    "PRCP",
    "PRCP_ATTRIBUTES",
    "SNDP",
    "FRSHTT" 
    )

clean_df.writeTo("demo_raw.silver_weather_iceberg_data").append()


