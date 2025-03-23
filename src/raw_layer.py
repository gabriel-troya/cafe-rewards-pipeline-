from pyspark.sql import SparkSession
import os
from config import RAW_DATA_DIR, SPARK_CONFIG

def create_spark_session():
    """Create and return a Spark session with configured settings."""
    spark = SparkSession.builder
    for key, value in SPARK_CONFIG.items():
        spark = spark.config(key, value)
    
    return spark.getOrCreate()

def read_csv_file(spark, file_path):
    """Read a CSV file and return a Spark DataFrame."""
    return spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

def ingest_raw_data():
    """Ingest all raw data files and return DataFrames."""
    spark = create_spark_session()
    
    # Read the data files
    offers_df = read_csv_file(spark, os.path.join(RAW_DATA_DIR, 'offers.csv'))
    customers_df = read_csv_file(spark, os.path.join(RAW_DATA_DIR, 'customers.csv'))
    events_df = read_csv_file(spark, os.path.join(RAW_DATA_DIR, 'events.csv'))
    
    # Print basic statistics and schema
    print("Offers dataset:")
    offers_df.printSchema()
    print(f"Number of offers: {offers_df.count()}")
    
    print("Customers dataset:")
    customers_df.printSchema()
    print(f"Number of customers: {customers_df.count()}")
    
    print("Events dataset:")
    events_df.printSchema()
    print(f"Number of events: {events_df.count()}")
    
    return offers_df, customers_df, events_df
