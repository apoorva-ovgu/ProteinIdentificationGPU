from kafka import KafkaConsumer

try:
    consumer = KafkaConsumer('xtandemtest',bootstrap_servers=['localhost:9092'])
    for msg in consumer:
        print('Message from Consumer: '+ str(msg))
    KafkaConsumer(consumer_timeout_ms=10000) #10 seconds
except Exception as e:
    print("Leider exception in Kafka consumer: "+ e.message)
