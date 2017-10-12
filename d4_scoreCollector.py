import uuid
from cassandra.cluster import Cluster
from kafka import KafkaConsumer

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('xtandem')

def storeScores(esid, tsid,score):
    session.execute(
        """INSERT INTO xtandem.psm (exp_spectrum_uid, theo_spectrum_uid, score)
        VALUES (%s, %s, %s)""",
        (uuid.uuid1(), esid, tsid,score)
    )

def collect(spectrumline):
    print ("Received",spectrumline.value)

try:
    consumer = KafkaConsumer('scores',bootstrap_servers=['localhost:9092'])
    for msg in consumer:
        collect(msg)
except Exception as e:
    print("Exception in Kafka consumer in scoreCollector: "+ e.message)
finally:
    KafkaConsumer.close()
