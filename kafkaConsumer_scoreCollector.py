import uuid
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
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

def collect(spectrumline,pos):
    print "Received",spectrumline.value

try:
    consumer = KafkaConsumer('xtandemtest',bootstrap_servers=['localhost:9092'])
    for msg in consumer:
        if "BEGIN IONS" in str(msg.value):
            collect(msg,"start")
        elif "END IONS" in str(msg.value):
            collect(msg,"end")
        else:
            if str(msg.value).lstrip() is not "":
                collect(msg,"contents")

    #KafkaConsumer(consumer_timeout_ms=1000)
except Exception as e:
    print("Leider exception in Kafka consumer: "+ e.message)
