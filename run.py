import argparse
import os
from src.raw_layer import ingest_raw_data
from src.trusted_layer import create_trusted_data
from src.refined_layer import create_offer_completion_metrics
from src.analytics import most_effective_channel, age_distribution_comparison, average_completion_time
from config import RAW_DATA_DIR, TRUSTED_DATA_DIR, REFINED_DATA_DIR

def create_directories():
    """Create necessary directories if they don't exist."""
    for directory in [RAW_DATA_DIR, TRUSTED_DATA_DIR, REFINED_DATA_DIR]:
        os.makedirs(directory, exist_ok=True)

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Run the Café Rewards data pipeline.')
    parser.add_argument('--layer', choices=['raw', 'trusted', 'refined', 'all'], 
                        default='all', help='Specify which layer to process')
    parser.add_argument('--analytics', action='store_true', 
                        help='Run analytical questions')
    
    args = parser.parse_args()
    
    create_directories()
    
    if args.layer in ['raw', 'all']:
        print("Processing raw layer...")
        offers_df, customers_df, events_df = ingest_raw_data()
    
    if args.layer in ['trusted', 'all']:
        print("Processing trusted layer...")
        if 'offers_df' not in locals():
            # If raw layer wasn't processed, load from files
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("Cafe Rewards").getOrCreate()
            offers_df = spark.read.csv(os.path.join(RAW_DATA_DIR, 'offers.csv'), header=True, inferSchema=True)
            customers_df = spark.read.csv(os.path.join(RAW_DATA_DIR, 'customers.csv'), header=True, inferSchema=True)
            events_df = spark.read.csv(os.path.join(RAW_DATA_DIR, 'events.csv'), header=True, inferSchema=True)
        
        trusted_offers, trusted_customers, trusted_events = create_trusted_data(offers_df, customers_df, events_df)
    
    if args.layer in ['refined', 'all']:
        print("Processing refined layer...")
        if 'trusted_offers' not in locals():
            # If trusted layer wasn't processed, load from files
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("Cafe Rewards").getOrCreate()
            trusted_offers = spark.read.parquet(os.path.join(TRUSTED_DATA_DIR, "offers"))
            trusted_customers = spark.read.parquet(os.path.join(TRUSTED_DATA_DIR, "customers"))
            trusted_events = spark.read.parquet(os.path.join(TRUSTED_DATA_DIR, "events"))
        
        channel_metrics, age_metrics, time_metrics = create_offer_completion_metrics(
            trusted_offers, trusted_events, trusted_customers
        )
    
    if args.analytics or args.layer == 'all':
        print("Running analytics...")
        print("\nQuestion 1: Most effective marketing channel")
        most_effective_channel()
        
        print("\nQuestion 2: Age distribution comparison")
        age_distribution_comparison()
        
        print("\nQuestion 3: Average completion time")
        average_completion_time()
    
    print("Pipeline execution completed successfully!")

if __name__ == "__main__":
    main()
