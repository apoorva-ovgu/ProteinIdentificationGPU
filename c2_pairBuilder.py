import uuid
import re
import itertools
from datetime import timedelta, datetime as dt

from kafka import KafkaConsumer
from kafka import KafkaProducer
from dbOperations import connectToDB

flag_compareAll = True
fastaSpectrumIDs = []
mgf_id = "Not Set"


def filldb(mgf_id, mgf_metadata, mgf_sequence):
    session = connectToDB()
    session.execute("""INSERT INTO xtandem.exp_spectrum (id, metadata, data) 
        VALUES (%s, %s, %s)""",
        (mgf_id,  mgf_metadata, mgf_sequence)
    )
    session.shutdown()

def table_contents(toselect, table_name):
    cass_session = connectToDB()
    tempArray = []

    query = "SELECT " + toselect + " FROM " + table_name + ";"
    select_results = cass_session.execute(query)

    if "*" not in toselect:
        for row in select_results:
            stringRes = str(eval("row." + toselect))
            tempArray.append(stringRes)
    else:
        for row in select_results:
            tempArray.append(row)
    cass_session.shutdown()

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
    loadBalancer = 0


    for couple in pairsCreated:
        couple = couple + (loadBalancer,) + (time,)
        packagedCouple = str(couple).encode('utf-8')

        loadBalancer+=1
        if (loadBalancer > 7): #This what we change for scaling.
            loadBalancer=0
        try:
            producer_c2.flush()
            producer_c2.send("pairs"+str(loadBalancer)
                                ,key = "keyforpair".encode('utf-8')
                                 ,value = packagedCouple)
        except Exception as e:
            print("Exception in Kafka producer in pairbuilder: " + e.message)
        finally:
            print "Paired as ", couple
    producer_c2.close()

def storeMGF(mgfid, mgfContent):
    print("Received spectrum for ID ", mgfid)
    tmpArr = mgfContent.split("#")    #0:data 1:metadata
    filldb(uuid.UUID('{' + mgfid + '}'), tmpArr[1], tmpArr[0])

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
    session.execute("""TRUNCATE table xtandem.exp_spectrum  """)
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