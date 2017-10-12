from cassandra.cluster import Cluster
from kafka import KafkaProducer
from kafka import KafkaConsumer

receivedPairs = []
currScore = 0.0

def readFromPairBuilder():
    try:
        consumer = KafkaConsumer('pairs',bootstrap_servers=['localhost:9092'], consumer_timeout_ms=5000)
        for msg in consumer:
            receivedLine = msg.value.decode("utf-8")
            receivedPairs.append(receivedLine)
    except Exception as e:
        print("Exception in Kafka consumer in scorer: "+ e.message)
    finally:
        KafkaConsumer.close(consumer, True)

def calculateScore():
    return 0

def sendScores():
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
        producer.send("scores", currScore.encode('utf-8'))
    except Exception as e:
        print("Exception in Kafka producer in scorer: " + e.message)