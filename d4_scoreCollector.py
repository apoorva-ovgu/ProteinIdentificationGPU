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
session.set_keyspace('scores')

mgfClassInstances = []
mgfDict = {}
receivedMatches = 0
timeDict = {}

def storeScores(esid, tsid,score):
    try:
        session.execute(
            """INSERT INTO scores.psm (id, exp_spectrum_uid, theo_spectrum_uid, score)
            VALUES (%s, %s, %s, %s)""",
            (uuid.uuid1(), uuid.UUID('{' + esid + '}'), uuid.UUID('{' + tsid + '}'), float(score))
        )
    except Exception as e:
        print ("error in saving score to cass: "+str(e))

def collect(scoreLine):
    if "__init__" in scoreLine.key:
        return
    receivedScore = scoreLine.value
    receivedScoreFor = scoreLine.key.split("#")

    #print "--ignore-- ",receivedScore," ",str(receivedScoreFor)
    try:
        if receivedScoreFor[0] in mgfDict:
            mgfDict[receivedScoreFor[0]] = mgfDict[receivedScoreFor[0]]-1
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
                max_time = sortScores(receivedScoreFor[0])
                a = dt.now()
                b =timeDict[receivedScoreFor[0]]
                c = timedelta.total_seconds(a - b)

            toprint ="\n"+str(receivedScoreFor[0])+"end-to-end time from collector (use last for total time plot 1) "+ str(c)+ ", wait time (for wt plot 2) "+str(float(c)-float(max_time))+", max_service_time (use all of them for service time plot 3) "+str(max_time)+"\n"
            print(toprint)
            f = open('output/big_scorer8_run1.txt', 'a')
            f.write(toprint)
            f.close()

        else:
            print ("Hey, it wasn't in the dictionary!")
    except KeyError:
        print (str(receivedScoreFor),"Key does not exist....STALE  DATA")

def sortScores(mgfid):
    producer_d4 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_d4.send("results"
                     , value=b'code by apoorva patrikar'
                     , key=b'__init__')
    tmpArr = []
    toPrint = "\nAll results collected for "+str( mgfid)+ " and the Sorting order is:"
    print (toPrint)
    f = open('output/big_scorer8_run1.txt', 'a')
    f.write(toPrint)
    f.close()

    for comparison in mgfClassInstances:
        if mgfid in comparison.name:
            tmpArr.append(comparison)

    processingObjArr = sorted(tmpArr, reverse=True, key=lambda tmpArr: tmpArr.score)
    max_time = 0
    f1 = 0
    for eachItem in processingObjArr:
        if f1==0:
            max_time = eachItem.timeReqd
            f1=1

        toPrint = " \n...matched with="+eachItem.match\
                  +" ...with a score of="+str(eachItem.score)\
                  +" ...and computing time (do not use these numbers)="+str(eachItem.timeReqd)

        print (toPrint)
        f = open('output/big_scorer8_run1.txt', 'a')
        f.write(toPrint)
        f.close()

        mgfClassInstances.remove(eachItem)
        
    sendResults(producer_d4, mgfid, toPrint)
    return max_time

def getScores():
    consumer_scores = KafkaConsumer('scores'
                                     ,bootstrap_servers=['localhost:9092']
                                     , group_id='apoorva-thesis')
    print ("Ready to collect scores!")
    for msg in consumer_scores:

        collect(msg)

def getuidMetadata():
    try:
        consumer_uidMatches = KafkaConsumer('uidMatches'
                                            ,bootstrap_servers=['localhost:9092']
                                            , group_id='apoorva-thesis')
        print ("Ready to consume uidMatches")
        for msg in consumer_uidMatches:
            if "__final__" not in msg.key.decode("'utf-8'"):
                toPrint = "\n%s has %s matches!" % (msg.key,msg.value)
                print (toPrint)
                mgfDict[msg.key] = int(msg.value)
                timeDict[msg.key] =  dt.now()

                f = open('output/big_scorer8_run1.txt', 'a')
                f.write(toPrint)
                f.close()
            #else:
            #    print "Closing metadata listener"
            #    consumer_uidMatches.close()
            #    return 0
    except Exception as e:
        print("Exception in Kafka consumer in uidMatches Collector: "+ e.message)

def sendResults(producer_d4, keyToSend, valueToSend):
    producer_d4.send("results"
                  ,value = valueToSend.encode('utf-8')
                  , key = keyToSend.encode('utf-8'))


if __name__ == '__main__':
    Thread(target = getuidMetadata).start()
    Thread(target = getScores).start()

