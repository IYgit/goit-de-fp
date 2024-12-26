from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp
import os

def get_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def process_silver_to_gold():
    """Об'єднує таблиці та виконує агрегацію."""
    spark = get_spark_session("SilverToGold")

    athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
    athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")
    joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "bio_country_noc") \
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
            current_timestamp().alias("timestamp")
        )
        
    aggregated_df.show()

    output_path = "/tmp/gold/avg_stats"
    os.makedirs(output_path, exist_ok=True)
    aggregated_df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")
    spark.stop()

if __name__ == "__main__":
    process_silver_to_gold()
