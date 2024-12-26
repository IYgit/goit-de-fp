# -*- coding: utf-8 -*-
"""
Created on Thu Dec 19 22:30:51 2024

@author: Asus
"""

from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config
from colorama import Fore, Style, init

# Ініціалізація кольорового логування
init(autoreset=True)

# Створення клієнта Kafka
try:
    print(f"{Fore.CYAN}Connecting to Kafka Admin Client...")
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config.bootstrap_servers[0],  # Звернення до першого елемента списку
        security_protocol=kafka_config.security_protocol,
        sasl_mechanism=kafka_config.sasl_mechanism,
        sasl_plain_username=kafka_config.username,
        sasl_plain_password=kafka_config.password
    )
    print(f"{Fore.GREEN}Connected to Kafka Admin Client successfully.")
except Exception as e:
    print(f"{Fore.RED}Failed to connect to Kafka Admin Client: {e}")
    exit(1)


num_partitions = 2
replication_factor = 1

# Створення топіків
new_topics = [
    NewTopic(name='athlete_event_results_ivan_y', num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(name='aggregated_results_ivan_y', num_partitions=num_partitions, replication_factor=replication_factor)
]

try:
    print(f"{Fore.CYAN}Creating topics: {'athlete_event_results_ivan_y'}...")
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"{Fore.GREEN}Topics '{'athlete_event_results_ivan_y'}' and  '{'aggregated_results_ivan_y'}' created successfully.")
except Exception as e:
    if "TopicExistsException" in str(e):
        print(f"{Fore.YELLOW}Topics already exist: {e}")
    else:
        print(f"{Fore.RED}An error occurred while creating topics: {e}")
finally:
    print(f"{Fore.CYAN}Closing Kafka Admin Client...")
    admin_client.close()
    print(f"{Fore.GREEN}Kafka Admin Client closed successfully.")
