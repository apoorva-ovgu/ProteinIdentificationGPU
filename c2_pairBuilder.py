import uuid
import re
import itertools
import datetime

from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import SimpleClient

flag_compareAll = True
flag_dummyScore = True
fastaSpectrumIDs = []

p = re.compile('\d+.\d+\t\d+\t\d\+')
mgf_id = "Not Set"

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('xtandem')


def filldb(mgf_id, protein_uid, sequence):
    session.execute(
        """INSERT INTO xtandem.peptide (id, protein_uid, sequence) 
        VALUES (%s, %s, %s)""",
        (mgf_id, protein_uid, sequence)
    )


def table_contents(toselect, table_name):
    tempArray = []

    query = "SELECT " + toselect + " FROM " + table_name + ";"
    select_results = session.execute(query)

    if "*" not in toselect:
        for row in select_results:
            stringRes = str(eval("row." + toselect))
            tempArray.append(stringRes)
    else:
        for row in select_results:
            tempArray.append(row)

    return tempArray


def createPairs(id):
    mgfSpectrumIDs = []
    mgfSpectrumIDs.append(id)
    pairs = []
    if flag_compareAll is True:
        for element in itertools.product(mgfSpectrumIDs, fastaSpectrumIDs):
            pairs.append(element)
    return pairs


def sendPairs(pairs, time):
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
        loadBalancer = 2
        for couple in pairs:
            if loadBalancer == 2:
                loadBalancer = 1
            else:
                loadBalancer = 2
            couple = couple + (loadBalancer,) + (time,)
            producer.send("pairs", str(couple).encode('utf-8'))
            print("Sent ", couple)
    except Exception as e:
        print("Exception in Kafka producer in pairbuilder: " + e.message)


def readFromMgfProducer(msg):
    receivedLine = msg.value.decode("utf-8")
    receivedSpectrum = receivedLine.split("endMGFID")
    print("Received spectrum for ID ",receivedSpectrum[0])
    filldb(uuid.uuid1(),uuid.UUID('{'+receivedSpectrum[0]+'}'),receivedSpectrum[1])
    return receivedSpectrum[0]


def readFromFastaDB():
    resultsFromCass = table_contents("id", "xtandem.protein")
    for eachID in resultsFromCass:
        fastaSpectrumIDs.append(eachID)

consumer = KafkaConsumer('UIDSandMGF', bootstrap_servers=['localhost:9092'],group_id='apoorva-thesis')
try:
    session.execute("""TRUNCATE table xtandem.peptide  """)
    readFromFastaDB()
    print("Consumer is ready to listen!")
    for msg in consumer:
        a = datetime.datetime.now()
        id = readFromMgfProducer(msg)
        pairs = createPairs(id)
        b = datetime.datetime.now()
        sendPairs(pairs, b - a)  ### timedelta:  (days, seconds and microseconds)
except Exception as e:
    print("Exception in Kafka consumer: " + str(e))
finally:
    # print("Rows entered in table peptide:")
    # table_contents("*","xtandem.peptide")
    KafkaConsumer.close(consumer)