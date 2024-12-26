# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 17:29:25 2024

@author: Asus
"""

from configs import kafka_config, db_config, db_properties, tables
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, LongType
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення сесії Spark
spark = SparkSession.builder \
    .appName("Kafka Consumer Example") \
    .getOrCreate()

# Визначення схеми для JSON повідомлень
schema = StructType() \
    .add("sport", StringType()) \
    .add("medal", StringType()) \
    .add("sex", StringType()) \
    .add("country_noc", StringType()) \
    .add("avg_height", StringType()) \
    .add("avg_weight", StringType()) \
    .add("calculation_time", StringType())

# Зчитування даних з топіка Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers[0]) \
    .option("subscribe", 'aggregated_results_ivan_y') \
    .load()

# Перетворення повідомлень Kafka з байтів в строки
messages_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

# Парсинг JSON даних
parsed_df = messages_df.withColumn("data", from_json("value", schema)).select("data.*")

# Виведення даних
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Очікування завершення
query.awaitTermination()
