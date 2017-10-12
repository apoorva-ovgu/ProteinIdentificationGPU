import uuid
import re
import itertools

from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka import KafkaProducer

flag_compareAll = True
flag_dummyScore = True
mgfSpectrumIDs = []
fastaSpectrumIDs = []
pairs = []

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

    query = "SELECT "+toselect+" FROM "+table_name+";"
    select_results = session.execute(query)

    if "*" not in toselect:
        for row in select_results:
            stringRes = str(eval("row."+toselect))
            tempArray.append(stringRes)
    else:
        for row in select_results:
            tempArray.append(row)

    return tempArray

def createPairs():
    if flag_compareAll is True:
        for element in itertools.product(mgfSpectrumIDs, fastaSpectrumIDs):
            pairs.append(element)

def sendPairs():
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
        loadBalancer = 2
        for couple in pairs:
            if loadBalancer == 2:
                loadBalancer = 1
            else:
                loadBalancer = 2
            couple = couple + (loadBalancer,)
            producer.send("pairs", str(couple).encode('utf-8'))
            print("Sent ",couple)
    except Exception as e:
        print("Exception in Kafka producer in pairbuilder: " + e.message)

def readFromMgfProducer():
    try:
        session.execute("""TRUNCATE table xtandem.peptide  """)
        consumer = KafkaConsumer('UIDSandMGF',bootstrap_servers=['localhost:9092'], consumer_timeout_ms=5000)
        for msg in consumer:
            receivedLine = msg.value.decode("utf-8")
            m = p.match(receivedLine)

            if "BEGIN IONS" in receivedLine:
                mgf_id = receivedLine.split("MGFID=")[1]
                mgf_id = mgf_id.lstrip()
                mgfSpectrumIDs.append(mgf_id)


            elif "END IONS" in receivedLine:
                mgf_id = "Not Set"

            elif "=all_end" in receivedLine:
                print("Last spectrum line received!")

            elif m is not None :
                filldb(uuid.uuid1(),uuid.UUID('{'+mgf_id+'}'),receivedLine)

    except Exception as e:
        print("Exception in Kafka consumer: "+ str(e))
    finally:
        KafkaConsumer.close(consumer, True)
        #print("Rows entered in table peptide:")
        #table_contents("*","xtandem.peptide")

def readFromFastaDB():
    resultsFromCass = table_contents("id","xtandem.protein")
    for eachID in resultsFromCass:
        fastaSpectrumIDs.append(eachID)

readFromMgfProducer()
readFromFastaDB()
createPairs()
sendPairs()