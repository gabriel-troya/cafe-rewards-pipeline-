from pyspark.sql import SparkSession, functions as F
import os
from config import REFINED_DATA_DIR, SPARK_CONFIG
import matplotlib.pyplot as plt
import pandas as pd

def create_spark_session():
    """Create and return a Spark session with configured settings."""
    spark = SparkSession.builder
    for key, value in SPARK_CONFIG.items():
        spark = spark.config(key, value)
    
    return spark.getOrCreate()

def most_effective_channel():
    """Determine the most effective marketing channel by offer completion rate."""
    spark = create_spark_session()
    
    # Load channel metrics
    channel_metrics = spark.read.parquet(os.path.join(REFINED_DATA_DIR, "channel_metrics"))
    
    # Find channel with highest completion rate
    result = channel_metrics.orderBy(F.desc("completion_rate"))
    
    # Convert to pandas for visualization
    pandas_df = result.toPandas()
    
    # Create bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(pandas_df['channel'], pandas_df['completion_rate'])
    plt.title('Offer Completion Rate by Marketing Channel')
    plt.xlabel('Channel')
    plt.ylabel('Completion Rate (%)')
    plt.savefig('channel_completion_rate.png')
    
    best_channel = pandas_df.iloc[0]['channel']
    best_rate = pandas_df.iloc[0]['completion_rate']
    
    print(f"The most effective marketing channel is {best_channel} with a completion rate of {best_rate:.2f}%")
    
    return result

def age_distribution_comparison():
    """Compare age distribution between customers who completed offers vs. those who didn't."""
    spark = create_spark_session()
    
    # Load age metrics
    age_metrics = spark.read.parquet(os.path.join(REFINED_DATA_DIR, "age_metrics"))
    
    # Calculate completed vs not completed by age
    result = age_metrics.withColumn(
        "not_completed", 
        F.col("total_offers") - F.col("completed_offers")
    )
    
    # Convert to pandas for visualization
    pandas_df = result.toPandas()
    
    # Create stacked bar chart
    plt.figure(figsize=(12, 8))
    
    completed = pandas_df['completed_offers']
    not_completed = pandas_df['not_completed']
    age_groups = pandas_df['age']
    
    plt.bar(age_groups, completed, label='Completed')
    plt.bar(age_groups, not_completed, bottom=completed, label='Not Completed')
    
    plt.title('Offer Completion by Age Group')
    plt.xlabel('Age')
    plt.ylabel('Number of Offers')
    plt.legend()
    plt.savefig('age_distribution.png')
    
    return result

def average_completion_time():
    """Calculate the average time taken to complete an offer after receiving it."""
    spark = create_spark_session()
    
    # Load time metrics
    time_metrics = spark.read.parquet(os.path.join(REFINED_DATA_DIR, "time_metrics"))
    
    # Calculate average time to completion
    avg_time = time_metrics.agg(F.avg("time_to_completion_hours").alias("avg_hours")).collect()[0]["avg_hours"]
    
    print(f"Average time to complete an offer: {avg_time:.2f} hours")
    
    # Get distribution of completion times
    time_distribution = time_metrics.groupBy(
        F.floor("time_to_completion_hours").alias("hour_bin")
    ).count().orderBy("hour_bin")
    
    # Convert to pandas for visualization
    pandas_df = time_distribution.toPandas()
    
    # Create histogram
    plt.figure(figsize=(12, 6))
    plt.bar(pandas_df['hour_bin'], pandas_df['count'])
    plt.axvline(x=avg_time, color='r', linestyle='--', label=f'Average ({avg_time:.2f} hours)')
    plt.title('Distribution of Time to Offer Completion')
    plt.xlabel('Hours to Complete')
    plt.ylabel('Count')
    plt.legend()
    plt.savefig('completion_time_distribution.png')
    
    return avg_time

