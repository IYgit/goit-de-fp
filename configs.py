# -*- coding: utf-8 -*-
"""
Created on Thu Dec 19 22:29:57 2024

@author: Ivan Yanchyk
"""

kafka_config = {
    "bootstrap_servers": "localhost:9092",
    "username": '',
    "password": '',
    "security_protocol": 'PLAINTEXT',
    "sasl_mechanism": '',
    "event_results": 'athlete_event_results',
    "aggregated_results": 'aggregated_results'
}

db_config = {
    "mysql-connector": "./mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar",
    "host": "217.61.57.46",
    "port": "3306",
    "jdbc_url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "jdbc_url_ivan": "jdbc:mysql://217.61.57.46:3306/ivan_y",
    "db_olimpic": "olympic_dataset",
    "db_ivan": "ivan_y"
    }

db_properties = {
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver"
}

tables = {
    "athletes": "athlete_bio",
    "results": "results"
    }