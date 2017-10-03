import os
from kafka import KafkaProducer

mgf_location = os.path.join(os.path.dirname(__file__), 'datafiles')

try:
    producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    #Send to topic, and send raw bytes
    future = producer.send('xtandemtest', b'raw_bytes')

    for file in os.listdir(mgf_location):
        if file.endswith(".mgf"):
            for line in open(mgf_location + "/" + file, 'U'):
                line.rstrip('\n')

                producer.send("xtandemtest",line)
                print("sent "+line)

except Exception as e:
    print("Leider exception in Kafka producer: "+ e.message)
