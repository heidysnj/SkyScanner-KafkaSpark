from kafka import KafkaProducer
import requests
import json
from time import sleep

producer = KafkaProducer(bootstrap_servers=['localhost:9098'])
url = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/autosuggest/v1.0/UK/GBP/en-GB/"

querystring = {"query":"La", "country":"UK", "currency":"GBP", "locale":"en-GB" }

headers = {
    'x-rapidapi-key': "071754d5fcmsh29eabc6f643d6fdp1e72cfjsn72d707243811",
    'x-rapidapi-host': "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

dumped = json.dumps(response.json())

producer.send('skyScanner', dumped.encode('utf-8'))

producer.flush()

print('Execute Ok')