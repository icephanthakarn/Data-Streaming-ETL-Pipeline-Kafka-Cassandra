# app/producer.py

import pandas as pd
from kafka import KafkaProducer
import json
import time
import os

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print(f'Message published successfully to {topic_name}.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        # !!! เปลี่ยนจาก localhost เป็นชื่อ service 'kafka' และ port ภายใน Docker
        _producer = KafkaProducer(bootstrap_servers=['kafka:29092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

if __name__ == "__main__":
    # Path ภายใน Container
    movies_file = '/data/the-movies-dataset/movies_metadata.csv'
    ratings_file = '/data/the-movies-dataset/ratings_small.csv'

    movies_topic = 'movies_metadata'
    ratings_topic = 'movie_ratings'
    
    kafka_producer = connect_kafka_producer()

    if kafka_producer is not None:
        # --- Process and send movies data ---
        movies_df = pd.read_csv(movies_file, usecols=['id', 'title', 'release_date', 'vote_average', 'vote_count'], dtype={'id': 'str'})
        movies_df = movies_df[movies_df['id'].str.isnumeric()]
        movies_df['id'] = movies_df['id'].astype(int)

        for index, row in movies_df.iterrows():
            movie_data = row.to_dict()
            publish_message(kafka_producer, movies_topic, str(movie_data['id']), json.dumps(movie_data))
            time.sleep(0.01)

        # --- Process and send ratings data ---
        ratings_df = pd.read_csv(ratings_file)
        for index, row in ratings_df.iterrows():
            rating_data = row.to_dict()
            publish_message(kafka_producer, ratings_topic, str(rating_data['userId']), json.dumps(rating_data))
            time.sleep(0.005)

        if kafka_producer is not None:
            kafka_producer.close()