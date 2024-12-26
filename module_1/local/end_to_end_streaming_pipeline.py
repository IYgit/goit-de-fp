# -*- coding: utf-8 -*-
"""
Created on Thu Dec 19 13:28:24 2024

@author: Ivan Yanchyk
"""

from configs import kafka_config, db_config, db_properties, tables
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, to_json, struct
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, LongType
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'


# Створити сесію Spark
spark = SparkSession.builder \
    .appName("ML Data Streaming Pipeline") \
    .config("spark.jars", db_config["mysql-connector"]) \
    .getOrCreate()
    
print("SparkSession успішно створена.")

# Зчитати фізичні дані атлетів
athlete_bio_df = spark.read \
    .jdbc(url=db_config["jdbc_url"], table=tables["athletes"], properties=db_properties)

print("Фізичні дані атлетів успішно зчитані.")

# Фільтрація невірних значень
athlete_bio_filtered_df = athlete_bio_df.filter(
    col("height").cast("float").isNotNull() &
    col("weight").cast("float").isNotNull()
)

print("Дані успішно відфільтровані.")

# athlete_bio_filtered_df.show()


# # Зчитування результатів змагань з бази даних
athlete_event_results_df = spark.read \
    .jdbc(url=db_config["jdbc_url"], table=tables["results"], properties=db_properties)

print("Результати змагань успішно зчитані.")
# athlete_event_results_df.show()

# Підготовка даних для Kafka
kafka_ready_df = athlete_event_results_df.selectExpr(
    "CAST(result_id AS STRING) AS key",  # Використовуйте унікальне поле як ключ
    "to_json(struct(*)) AS value"       # Усі колонки перетворюються в JSON
)

print("Дані для запису в кафка успішно підготовлені.")

# kafka_ready_df.show(10, truncate=False)

# Запис у Kafka
kafka_ready_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
    .option("topic", kafka_config['event_results']) \
    .save()   
    
print("Дані в кафка успішно записані.")   

# схема для парсингу JSON
schema = StructType() \
    .add("edition", StringType()) \
    .add("edition_id", IntegerType()) \
    .add("country_noc", StringType()) \
    .add("sport", StringType()) \
    .add("event", StringType()) \
    .add("result_id", LongType()) \
    .add("athlete", StringType()) \
    .add("athlete_id", IntegerType()) \
    .add("pos", StringType()) \
    .add("medal", StringType()) \
    .add("isTeamSport", StringType())


# Зчитування з Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
    .option("subscribe", kafka_config['event_results']) \
    .load()
    
print("Дані з кафка успішно зчитані.")

# Перетворення Kafka повідомлень
results_df = kafka_stream_df.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json("value", schema)) \
    .select("data.*")
    
print("Дані з JSON формату успішно перетворені в dataframe.")

# Об’єднання даних
joined_df = results_df.join(
    athlete_bio_filtered_df, on="athlete_id", how="inner"
).select(
    results_df["sport"],
    results_df["medal"],
    athlete_bio_filtered_df["sex"],  # Додаємо sex з athlete_bio_filtered_df
    results_df["country_noc"],
    athlete_bio_filtered_df["height"],
    athlete_bio_filtered_df["weight"]
)
    
print("Дані успішно об'єднані по athlete_id")

# Обчислення середніх показників
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg(col("height").cast("float")).alias("avg_height"),
        avg(col("weight").cast("float")).alias("avg_weight"),
        current_timestamp().alias("calculation_time")
    )

print("Середні показники успішно обчислені.")

# Функція збереження
def save_to_kafka_and_db(batch_df, batch_id):
    # Запис у Kafka
    batch_df.selectExpr("to_json(struct(*)) AS value").write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
        .option("topic", kafka_config['aggregated_results']) \
        .save()

    # Запис у базу даних
    batch_df.write.jdbc(
        url=db_config["jdbc_url_ivan"],
        table=kafka_config['aggregated_results'],
        mode="append",
        properties=db_properties
    )

# Старт стриму
query = aggregated_df.writeStream \
    .outputMode("update") \
    .foreachBatch(save_to_kafka_and_db) \
    .start()
    
print("Агреговані дані успішно зберігаються в kafka та базу даних.")

query.awaitTermination()


