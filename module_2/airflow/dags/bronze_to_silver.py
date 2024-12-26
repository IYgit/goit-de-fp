# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 19:58:24 2024

@author: Ivan Yanchyk
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
import os

def get_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def clean_text(df):
    """Очищає текстові колонки."""
    for column in df.columns:
        if df.schema[column].dataType == "string":
            df = df.withColumn(column, trim(lower(col(column))))
    return df

def process_bronze_to_silver(table):
    """Очищає текстові дані та видаляє дублікати."""
    spark = get_spark_session("BronzeToSilver")

    input_path = f"/tmp/bronze/{table}"
    output_path = f"/tmp/silver/{table}"

    df = spark.read.parquet(input_path)
    
    # Очищення текстових даних та видалення дублікованих рядків
    df = clean_text(df)
    df = df.dropDuplicates()
    
    df.show()

    # Збереження очищених даних у форматі Parquet
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")
    spark.stop()

if __name__ == "__main__":
    tables = ["athlete_bio", "athlete_event_results"]
    for table in tables:
        process_bronze_to_silver(table)
