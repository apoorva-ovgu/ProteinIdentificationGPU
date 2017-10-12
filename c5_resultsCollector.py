from kafka import KafkaConsumer

try:
    consumer = KafkaConsumer('results',bootstrap_servers=['localhost:9092'])
    for msg in consumer:
        print(msg)
except Exception as e:
    print("Exception in Kafka consumer in scoreCollector: "+ e.message)
finally:
    KafkaConsumer.close()