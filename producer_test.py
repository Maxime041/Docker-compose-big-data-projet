import requests
import json
import time
from kafka import KafkaProducer
1
API_URL = "https://api.open-meteo.com/v1/forecast"
LAT, LON = 48.85, 2.35
KAFKA_TOPIC = "weather_transformed"
KAFKA_BROKER = "kafka:29092"

def fetch_weather():
    params = {
        "latitude": LAT,
        "longitude": LON,
        "current_weather": "true"
    }
    response = requests.get(API_URL, params=params, timeout=5)
    response.raise_for_status()
    return response.json().get("current_weather", {})


def transform_weather(record: dict) -> dict:
    # Convertir Celsius en Fahrenheit
    if "temperature" in record: 
        record["temp_f"] = record["temperature"] * 9/5 + 32 
    # Alerte vent fort
    record["high_wind_alert"] = record.get("windspeed", 0) > 10
    return record

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    print("Weather streaming producer started...")


    while True:
        try:
            weather = fetch_weather()
            
            if weather:
                transformed = transform_weather(weather)
                producer.send(KAFKA_TOPIC, transformed)
                producer.flush()
                print("Sent to weather_transformed:", transformed)
        except Exception as e:
            print("Error fetching or sending weather:", e)
        time.sleep(30) # fetch every 30 seconds

if __name__ == "__main__":
    main()