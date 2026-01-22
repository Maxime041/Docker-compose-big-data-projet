import json
from kafka import KafkaConsumer

KAFKA_TOPIC = "weather_transformed"
KAFKA_BROKER = "kafka:29092"

def main():
    print(f"Écoute du topic '{KAFKA_TOPIC}' sur {KAFKA_BROKER}...")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest', 
            enable_auto_commit=True,
            group_id='weather-group-1',  
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("Consumer connecté ! En attente de messages...")
        
        for message in consumer:
            data = message.value
            print(f"Reçu : {data}")
            
            if data.get('high_wind_alert'):
                print("ALERTE : Vent fort détecté !")

    except KeyboardInterrupt:
        print("Arrêt du consumer.")
    except Exception as e:
        print(f"Erreur Kafka : {e}")

if __name__ == "__main__":
    main()