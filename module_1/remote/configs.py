# -*- coding: utf-8 -*-
"""
Created on Thu Dec 19 22:29:57 2024

@author: Ivan Yanchyk
"""

from typing import List  # для типу List
import os  # для роботи з оточенням та змінними середовища
from dataclasses import dataclass  # для використання декоратора @dataclass


@dataclass
class KafkaConfig:
    """Kafka configuration class"""
    bootstrap_servers: List[str]
    username: str
    password: str
    security_protocol: str
    sasl_mechanism: str
    topic: str
    max_offsets_per_trigger: int

    @classmethod
    def from_env(cls) -> 'KafkaConfig':
        """Create configuration from environment variables."""
        return cls(
            bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "77.81.230.104:9092")],
            username=os.getenv("KAFKA_USERNAME", "admin"),
            password=os.getenv("KAFKA_PASSWORD", "VawEzo1ikLtrA8Ug8THa"),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            topic=os.getenv("KAFKA_TOPIC", "greenmoon_end_enriched_athlete_avg"),
            max_offsets_per_trigger=int(os.getenv("KAFKA_MAX_OFFSETS", "50"))
        )

kafka_config = kafka_config = KafkaConfig.from_env()


db_config = {
    "mysql-connector": "../mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar",
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