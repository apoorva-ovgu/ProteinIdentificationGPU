import uuid
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka import KafkaProducer
from mgfClassFile import mgfClass
import threading
from threading import Thread
from datetime import timedelta, datetime as dt

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('xtandem')

mgfClassInstances = []
mgfDict = {}
receivedMatches = 0
timeDict = {}

def storeScores(esid, tsid,score):
    session.execute(
        """INSERT INTO xtandem.psm (id, exp_spectrum_uid, theo_spectrum_uid, score)
        VALUES (%s, %s, %s, %s)""",
        (uuid.uuid1(), uuid.UUID('{' + esid + '}'), uuid.UUID('{' + tsid + '}'), float(score))
    )

def collect(scoreLine):
    if "__init__" in scoreLine.key:
        return
    receivedScore = scoreLine.value
    receivedScoreFor = scoreLine.key.split("#")

    try:
        if mgfDict[receivedScoreFor[0]] is not None:
            mgfDict[receivedScoreFor[0]] = int(mgfDict[receivedScoreFor[0]])-1
            #mgfClass: name, match, score, remainingComparisons, timeReqd
            mgfClassObj = \
                mgfClass(receivedScoreFor[0] #name
                         ,receivedScoreFor[1] #matchedWith
                         , receivedScore  #score
                          ,mgfDict[receivedScoreFor[0]] #remainingcomparisons
                         ,receivedScoreFor[2] #timeRequired
                         )
            mgfClassInstances.append(mgfClassObj)
            storeScores(receivedScoreFor[0], receivedScoreFor[1], receivedScore)

            if int(mgfDict[receivedScoreFor[0]]) == 0:
                sortScores(receivedScoreFor[0])
                a = dt.now()
                b =timeDict[receivedScoreFor[0]]
                c = timedelta.total_seconds(a - b)
                print(receivedScoreFor[0]+" time from collector "+ str(c))
    except KeyError:
        print "Key does not exist....STALE  DATA"

def sortScores(mgfid):
    producer_d4 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_d4.send("results"
                     , value=b'code by apoorva patrikar'
                     , key=b'__init__')
    tmpArr = []
    print "All results collected for ", mgfid, "\nSorting order is "

    for comparison in mgfClassInstances:
        if mgfid in comparison.name:
            tmpArr.append(comparison)

    processingObjArr = sorted(tmpArr, reverse=True, key=lambda tmpArr: tmpArr.score)

    for eachItem in processingObjArr:
        mgfClassInstances.remove(eachItem)
        toPrint = " ...matched with="+eachItem.match\
                  +" ...with a score of="+str(eachItem.score)\
                  +" ...and computing time="+str(eachItem.timeReqd)

        print toPrint

        sendResults(producer_d4, mgfid, toPrint)

def getScores():
    consumer_scores = KafkaConsumer('scores'
                                     ,bootstrap_servers=['localhost:9092']
                                     , group_id='apoorva-thesis')
    print "Ready to collect scores!"
    for msg in consumer_scores:
        collect(msg)

def getuidMetadata():
    try:
        consumer_uidMatches = KafkaConsumer('uidMatches'
                                            ,bootstrap_servers=['localhost:9092']
                                            , group_id='apoorva-thesis')
        print "Ready to consume uidMatches"
        for msg in consumer_uidMatches:
            if "__final__" not in msg.key:
                print "%s has %s matches!" % (msg.key,msg.value)
                mgfDict[msg.key] = msg.value
                timeDict[msg.key] =  dt.now()
    except Exception as e:
        print("Exception in Kafka consumer in uidMatches Collector: "+ e.message)

def sendResults(producer_d4, keyToSend, valueToSend):
    producer_d4.send("results"
                  ,value = valueToSend.encode('utf-8')
                  , key = keyToSend.encode('utf-8'))


if __name__ == '__main__':
    Thread(target = getuidMetadata).start()
    Thread(target = getScores).start()
