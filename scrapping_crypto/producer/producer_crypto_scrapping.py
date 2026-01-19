from calendar import calendar
from kafka import KafkaProducer
import json
import time
from zoneinfo import ZoneInfo
from datetime import datetime
import requests

import os, json

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092")

producer = KafkaProducer(
    bootstrap_servers=[bootstrap],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


API_KEY = "58djznrh1npgxnwo72is"
URL = "https://api.freecryptoapi.com/v1/getData?symbol="
ListCrypto = ["BTC", "ETH", "SOL", "ADA", "XRP"]

def send_crypto_price():
    while True:
        for symbol in ListCrypto:
            response = requests.get(URL + symbol, headers={"Authorization": f"Bearer {API_KEY}"})
            data = response.json()
            dt = datetime.now(tz=ZoneInfo("Europe/Paris"))
            scraping_event = {
            'timestamp': dt.strftime("%Y-%m-%d %H:%M:%S"),
            'symbols': data["symbols"][0]["symbol"], 
            'price': data["symbols"][0]["last"], 
            }
            print(scraping_event)
            time.sleep(5)
            producer.send('crypto_prices', scraping_event)
            producer.flush()



send_crypto_price()