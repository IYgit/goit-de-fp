# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 21:44:42 2024

@author: Ivan Yanchyk
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from landing_to_bronze import process_landing_to_bronze, download_data
from bronze_to_silver import process_bronze_to_silver
from silver_to_gold import process_silver_to_gold
import sys

sys.path.append('/home/airflow/.local/lib/python3.7/site-packages')


# Створення DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'tags': ['etl', 'pipeline', 'final_project']  # Виправлення у тегах
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Повний ETL-процес',
    schedule_interval=None,  # Запуск вручну
    catchup=False,
) as dag:

    # Завантаження даних для athlete_bio
    download_athlete_bio_task = PythonOperator(
        task_id='download_athlete_bio',
        python_callable=download_data,
        op_args=['athlete_bio'],
    )

    # Завантаження даних для athlete_event_results
    download_athlete_event_results_task = PythonOperator(
        task_id='download_athlete_event_results',
        python_callable=download_data,
        op_args=['athlete_event_results'],
    )

    # Обробка Landing to Bronze для athlete_bio
    process_landing_to_bronze_athlete_bio_task = PythonOperator(
        task_id='process_landing_to_bronze_athlete_bio',
        python_callable=process_landing_to_bronze,
        op_args=['athlete_bio'],
    )

    # Обробка Landing to Bronze для athlete_event_results
    process_landing_to_bronze_athlete_event_results_task = PythonOperator(
        task_id='process_landing_to_bronze_athlete_event_results',
        python_callable=process_landing_to_bronze,
        op_args=['athlete_event_results'],
    )

    # Обробка Bronze to Silver для athlete_bio
    process_bronze_to_silver_athlete_bio_task = PythonOperator(
        task_id='process_bronze_to_silver_athlete_bio',
        python_callable=process_bronze_to_silver,
        op_args=['athlete_bio'],
    )

    # Обробка Bronze to Silver для athlete_event_results
    process_bronze_to_silver_athlete_event_results_task = PythonOperator(
        task_id='process_bronze_to_silver_athlete_event_results',
        python_callable=process_bronze_to_silver,
        op_args=['athlete_event_results'],
    )

    # Обробка Silver to Gold
    process_silver_to_gold_task = PythonOperator(
        task_id='process_silver_to_gold',
        python_callable=process_silver_to_gold,
        op_args=[],
    )
   
    download_tasks = [download_athlete_bio_task, download_athlete_event_results_task]
    process_landing_to_bronze_tasks = [
        process_landing_to_bronze_athlete_bio_task,
        process_landing_to_bronze_athlete_event_results_task,
    ]
    process_bronze_to_silver_tasks = [
        process_bronze_to_silver_athlete_bio_task,
        process_bronze_to_silver_athlete_event_results_task,
    ]
    
    # Етап 1: Завантаження даних
    for download_task, landing_task in zip(download_tasks, process_landing_to_bronze_tasks):
        download_task >> landing_task
    
    # Етап 2: Landing to Bronze
    for landing_task, silver_task in zip(process_landing_to_bronze_tasks, process_bronze_to_silver_tasks):
        landing_task >> silver_task
    
    # Етап 3: Bronze to Silver
    for silver_task in process_bronze_to_silver_tasks:
        silver_task >> process_silver_to_gold_task


