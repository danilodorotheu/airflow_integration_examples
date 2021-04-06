from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("ETL processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file forex_rates.json from the HDFS
df = spark.read.option("header", True).csv('hdfs://namenode:9000/movies/movies.csv')

# selecting only columns that will be loaded into hive table
movies = df.select('title', 'genres')

# Export the dataframe into the Hive table forex_rates
movies.write.mode("append").insertInto("TBP_MOVIES")