# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 19:57:34 2024

@author: Ivan Iyanchyk
"""

from pyspark.sql import SparkSession
import os
import requests
from pyspark.sql.functions import col, trim, lower

def get_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def download_data(file):
    """Завантажує файл CSV із сервера."""
    url = f"https://ftp.goit.study/neoversity/{file}.csv"
    response = requests.get(url, timeout=10)

    if response.status_code == 200:
        with open(f"{file}.csv", 'wb') as f:
            f.write(response.content)
        print(f"File {file}.csv downloaded successfully.")
    else:
        raise Exception(f"Failed to download {file}. Status code: {response.status_code}")

def process_landing_to_bronze(table):
    """Конвертує CSV-файл у Parquet."""
    spark = get_spark_session("LandingToBronze")

    local_path = f"{table}.csv"
    output_path = f"/tmp/bronze/{table}"

    # Читання CSV-файлу в PySpark DataFrame
    df = spark.read.csv(local_path, header=True, inferSchema=True)
    
    df.show()
    
    # Збереження у форматі Parquet
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")
    spark.stop()

if __name__ == "__main__":
    tables = ["athlete_bio", "athlete_event_results"]
    for table in tables:
        download_data(table)
        process_landing_to_bronze(table)
