from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
from config import TRUSTED_DATA_DIR

def clean_offers_data(offers_df):
    """Clean and transform the offers data."""
    # Drop unnecessary columns (if any)
    # Handle missing values
    # Convert data types if needed
    cleaned_df = offers_df.na.drop()
    
    # Add any additional transformations
    
    return cleaned_df

def clean_customers_data(customers_df):
    """Clean and transform the customers data."""
    # Handle missing values
    # Convert data types if needed
    cleaned_df = customers_df.na.drop()
    
    # Calculate age from birth year if needed
    if "birthYear" in customers_df.columns:
        current_year = 2024
        cleaned_df = cleaned_df.withColumn("age", F.lit(current_year) - F.col("birthYear"))
    
    return cleaned_df

def clean_events_data(events_df):
    """Clean and transform the events data."""
    # Handle missing values
    # Convert timestamps to proper format
    # Drop unnecessary columns
    
    cleaned_df = events_df.na.drop()
    
    # Convert string timestamps to timestamp type if needed
    if "timestamp" in events_df.columns:
        cleaned_df = cleaned_df.withColumn(
            "timestamp", 
            F.to_timestamp(F.col("timestamp"))
        )
    
    return cleaned_df

def create_trusted_data(offers_df, customers_df, events_df):
    """Create trusted versions of all datasets."""
    # Clean individual datasets
    trusted_offers = clean_offers_data(offers_df)
    trusted_customers = clean_customers_data(customers_df)
    trusted_events = clean_events_data(events_df)
    
    # Save trusted datasets
    trusted_offers.write.mode("overwrite").parquet(os.path.join(TRUSTED_DATA_DIR, "offers"))
    trusted_customers.write.mode("overwrite").parquet(os.path.join(TRUSTED_DATA_DIR, "customers"))
    trusted_events.write.mode("overwrite").parquet(os.path.join(TRUSTED_DATA_DIR, "events"))
    
    return trusted_offers, trusted_customers, trusted_events
