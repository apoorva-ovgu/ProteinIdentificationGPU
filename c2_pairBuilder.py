import uuid
import re
import itertools
from datetime import timedelta, datetime as dt

from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka import KafkaProducer

flag_compareAll = True
fastaSpectrumIDs = []
mgf_id = "Not Set"


def connectToDB():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('xtandem')
    return session

def filldb(mgf_id, protein_uid, sequence):
    session = connectToDB()
    session.execute(
        """INSERT INTO xtandem.peptide (id, protein_uid, sequence) 
        VALUES (%s, %s, %s)""",
        (mgf_id, protein_uid, sequence)
    )
    session.shutdown()

def table_contents(toselect, table_name):
    session = connectToDB()
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
    session.shutdown()
    return tempArray

def createPairs(createForId):
    pairs_arr = []
    if flag_compareAll is True:

        for element in itertools.product([createForId], fastaSpectrumIDs):
            pairs_arr.append(element)
        producer_uidMatches = KafkaProducer(bootstrap_servers=['localhost: 9092'])
        producer_uidMatches.flush()
        try:
            producer_uidMatches.send("uidMatches"
                          , value=str(len(pairs_arr)).encode('utf-8')
                          , key= str(createForId).encode('utf-8'))
        except Exception as e:
            print("Leider exception in producer_uidMatches producer: " + str(e))

        producer_uidMatches.send("uidMatches"
                          , value=b'code by apoorva patrikar'
                          , key=b'__final__')
        producer_uidMatches.close()

    return pairs_arr

def sendPairs(pairsCreated, time):
    producer_c2 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_c2.flush()
    producer_c2.send("UIDSandMGF"
                  , value=b'code by apoorva patrikar'
                  , key=b'__init__')
    loadBalancer = 2

    for couple in pairsCreated:
        if loadBalancer == 2:
            loadBalancer = 1
        else:
            loadBalancer = 2
        couple = couple + (loadBalancer,) + (time,)
        packagedCouple = str(couple).encode('utf-8')
        try:
            producer_c2.flush()
            producer_c2.send("pairs"
                             ,key = "keyforpair".encode('utf-8')
                             ,value = packagedCouple)
        except Exception as e:
            print("Exception in Kafka producer in pairbuilder: " + e.message)
        finally:
            print "Paired as ", couple
    producer_c2.close()

def storeMGF(mgfid, mgfContent):
    #receivedLine = msgContent.decode("utf-8")
    #receivedSpectrum = receivedLine.split("endMGFID")
    #print("Received spectrum for ID ",receivedSpectrum[0])
    #filldb(uuid.uuid1(),uuid.UUID('{'+receivedSpectrum[0]+'}'),receivedSpectrum[1])
    #return receivedSpectrum[0]
    print("Received spectrum for ID ", mgfid)
    filldb(uuid.uuid1(), uuid.UUID('{' + mgfid + '}'), mgfContent)

def readFromFastaDB():
    resultsFromCass = table_contents("id", "xtandem.protein")
    for eachID in resultsFromCass:
        fastaSpectrumIDs.append(eachID)

def run_step2():
    consumer_c2 = KafkaConsumer('UIDSandMGF',
                                bootstrap_servers=['localhost:9092']
                                , group_id='apoorva-thesis')
                                #, auto_offset_reset='earliest')
    session = connectToDB()
    session.execute("""TRUNCATE table xtandem.peptide  """)
    session.shutdown()

    readFromFastaDB()

    print("Consumer is ready to listen!")#,consumer_c2.poll())
    for message in consumer_c2:
        if "__init__" not in message.key:
            preTime = dt.now()
            storeMGF(message.key, message.value)
            pairsCreated = createPairs(message.key)
            postTime = dt.now()
            sendPairs(pairsCreated, timedelta.total_seconds(postTime-preTime))  ### timedelta:  (days, seconds and microseconds)


run_step2()