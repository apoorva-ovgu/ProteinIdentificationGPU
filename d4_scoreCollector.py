import uuid
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka import KafkaProducer
from mgfClassFile import mgfClass
import threading
from threading import Thread


cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('xtandem')

mgfClassInstances = []
mgfDict = {}
receivedMatches = 0

def storeScores(esid, tsid,score):
    session.execute(
        """INSERT INTO xtandem.psm (exp_spectrum_uid, theo_spectrum_uid, score)
        VALUES (%s, %s, %s)""",
        (uuid.uuid1(), esid, tsid,score)
    )

def collect(scoreLine):
    receivedScore = scoreLine.value
    receivedScoreFor = scoreLine.key.split("#")

    try:
        if mgfDict[receivedScoreFor[0]] is not None:
            mgfDict[receivedScoreFor[0]] = int(mgfDict[receivedScoreFor[0]])-1
            mgfClassObj = \
                mgfClass(receivedScoreFor[0],receivedScoreFor[1],receivedScore,mgfDict[receivedScoreFor[0]])
            mgfClassInstances.append(mgfClassObj)

            if int(mgfDict[receivedScoreFor[0]]) == 0:
                sortScores(receivedScoreFor[0])
    except KeyError:
        print "Key does not exist....STALE  DATA"

def sortScores(mgfid):
    tmpArr = []
    print "All results collected for ", mgfid, "\nSorting order is "

    for comparison in mgfClassInstances:
        if mgfid in comparison.name:
            tmpArr.append(comparison)

    processingObjArr = sorted(tmpArr, key=lambda tmpArr: tmpArr.score)

    for eachItem in processingObjArr:
        mgfClassInstances.remove(eachItem)
        print " ...matched with=",eachItem.match," ...with a score of=",eachItem.score

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
    except Exception as e:
        print("Exception in Kafka consumer in uidMatches Collector: "+ e.message)


def sendResults(valueToSend):
    producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer.send("results", valueToSend)

#getuidMetadata()
#getScores()  ab01d38a-b1a5-11e7-82f8-b8ac6fa02cf9

if __name__ == '__main__':
    Thread(target = getuidMetadata).start()
    Thread(target = getScores).start()
