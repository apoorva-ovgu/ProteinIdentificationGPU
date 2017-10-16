from kafka import KafkaConsumer

try:
    consumer = KafkaConsumer('results'
                             ,bootstrap_servers=['localhost:9092']
                             , group_id='apoorva-thesis')
except Exception as e:
    print("Exception in Kafka consumer in scoreCollector: " + e.message)
finally:
    print "Listening to all results!"

for msg in consumer:
    if "__init__" not in msg:
        print "Result for ",msg.key," is: ",msg.value


