from kafka import KafkaProducer

try:
    producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    future = producer.send('aptop', b'raw_bytes')
    for i in range(1,1000):
        producer.send('aptop', "Sending "+ str(i))

except Exception as e:
    print("Leider exception in Kafka producer: "+ e.message)