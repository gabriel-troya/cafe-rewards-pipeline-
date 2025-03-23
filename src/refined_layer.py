from pyspark.sql import functions as F
import os
from config import REFINED_DATA_DIR

def create_offer_completion_metrics(trusted_offers, trusted_events, trusted_customers):
    """Create metrics related to offer completion."""
    # Join events with offers and customers
    # Filter for relevant event types (offer received, offer completed)
    # Calculate completion rates
    
    # Identify events where offers were received
    received_offers = trusted_events.filter(F.col("event") == "offer received")
    
    # Identify events where offers were completed
    completed_offers = trusted_events.filter(F.col("event") == "offer completed")
    
    # Join to calculate completion metrics
    offer_journey = received_offers.alias("received").join(
        completed_offers.alias("completed"),
        (F.col("received.customer_id") == F.col("completed.customer_id")) &
        (F.col("received.offer_id") == F.col("completed.offer_id")),
        "left_outer"
    )
    
    # Calculate completion rate by channel
    channel_metrics = offer_journey.join(
        trusted_offers,
        offer_journey["received.offer_id"] == trusted_offers["offer_id"]
    ).groupBy("channel").agg(
        F.count("received.offer_id").alias("total_offers"),
        F.count("completed.offer_id").alias("completed_offers"),
        (F.count("completed.offer_id") / F.count("received.offer_id") * 100).alias("completion_rate")
    )
    
    # Calculate age-based metrics
    age_metrics = offer_journey.join(
        trusted_customers,
        offer_journey["received.customer_id"] == trusted_customers["customer_id"]
    ).groupBy("age").agg(
        F.count("received.offer_id").alias("total_offers"),
        F.count("completed.offer_id").alias("completed_offers"),
        (F.count("completed.offer_id") / F.count("received.offer_id") * 100).alias("completion_rate")
    )
    
    # Calculate time to completion
    time_metrics = received_offers.alias("received").join(
        completed_offers.alias("completed"),
        (F.col("received.customer_id") == F.col("completed.customer_id")) &
        (F.col("received.offer_id") == F.col("completed.offer_id")),
        "inner"
    ).withColumn(
        "time_to_completion_hours",
        (F.unix_timestamp("completed.timestamp") - F.unix_timestamp("received.timestamp")) / 3600
    )
    
    # Save refined datasets
    channel_metrics.write.mode("overwrite").parquet(os.path.join(REFINED_DATA_DIR, "channel_metrics"))
    age_metrics.write.mode("overwrite").parquet(os.path.join(REFINED_DATA_DIR, "age_metrics"))
    time_metrics.write.mode("overwrite").parquet(os.path.join(REFINED_DATA_DIR, "time_metrics"))
    
    return channel_metrics, age_metrics, time_metrics
