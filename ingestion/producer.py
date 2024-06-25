from kafka import KafkaProducer
import json
import requests

# producer instance
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v:json.dumps(v).encode('utf-8')
)

# fetching data
def fetch_data(api_url, api_key, symbol):
    params = {'api_key': api_key,
              'symbol' : symbol,
              'interval': '1min',
              'function': 'TIME_SERIES_INTRADAY'
    }
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    data = response.json()
    return data

# send data to kafka topic
def send_to_kafka(topic, data):
    producer.send(topic, data)
    producer.flush()
    print(f"Data sent to {topic}")

api_url = 'https://www.alphavantage.co/query'
api_key ='HKOJGQE8FIL1CACE'
symbol = 'AAPL'
data = fetch_data(api_url, api_key, symbol)
send_to_kafka('movies', data)