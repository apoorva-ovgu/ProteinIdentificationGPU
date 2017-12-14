import uuid
from kafka import KafkaConsumer
from kafka import KafkaProducer
from mgfClassFile import mgfClass
import multiprocessing as mp
from multiprocessing import Process, Manager
from dbOperations import connectToDB
from datetime import timedelta, datetime as dt

def storeScores(esid, tsid,score):
    try:
        session = connectToDB()
        session.set_keyspace('scores')
        session.execute(
            """INSERT INTO scores.psm (id, exp_spectrum_uid, theo_spectrum_uid, score)
            VALUES (%s, %s, %s, %s)""",
            (uuid.uuid1(), uuid.UUID('{' + esid + '}'), uuid.UUID('{' + tsid + '}'), float(score))
        )
        session.shutdown()

    except Exception as e:
        print ("error in saving score to cass: "+str(e))


def collect(scoreLine,mgfDict,timeDict):
    receivedScore = scoreLine.value.decode('utf-8')
    receivedScoreFor = scoreLine.key.decode('utf-8').split("#")
    lookFor = receivedScoreFor[0]

    if lookFor in mgfDict:

        mgfDict[lookFor] = mgfDict[receivedScoreFor[0]]-1
        mgfClassObj = \
            mgfClass(lookFor #name
                     ,receivedScoreFor[1] #matchedWith
                     , receivedScore  #score
                      ,mgfDict[lookFor] #remainingcomparisons
                     ,receivedScoreFor[2] #timeRequired
                     )
        mgfClassInstances.append(mgfClassObj)
        #print("----", str(mgfClassInstances))
        #commented for tests # storeScores(lookFor, receivedScoreFor[1], receivedScore)
        if int(mgfDict[receivedScoreFor[0]]) == 0:
                max_time = sortScores(receivedScoreFor[0])
                #print("***", str(max_time))
                a = dt.now()
                #print("b is ",timeDict[lookFor]," of type ",type(timeDict[lookFor]))
                b = timeDict[lookFor]
                c = timedelta.total_seconds(a - b)

                toprint ="\n"+str(lookFor)+"end-to-end time from collector (use last for total time plot 1) "+ str(c)+ ", wait time (for wt plot 2) "+str(float(c)-float(max_time))+", max_service_time (use all of them for service time plot 3) "+str(max_time)+"\n"
                print(toprint)
                f = open('output/big_scorer2_testrun1.txt', 'a')
                f.write(toprint)
                f.close()
        #else:
        #    print("kichkich")
    else:
        print ("Hey, it wasn't in the dictionary!")

def sortScores(mgfid):
    producer_d4 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_d4.send("results"
                     , value=b'code by apoorva patrikar'
                     , key=b'__init__')
    tmpArr = []
    toPrint = "\nAll results collected for "+str( mgfid)+ " and the Sorting order is:"
    print (toPrint)
    f = open('output/big_scorer2_testrun1.txt', 'a')
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
        f = open('output/big_scorer2_testrun1.txt', 'a')
        f.write(toPrint)
        f.close()

        mgfClassInstances.remove(eachItem)

    sendResults(producer_d4, mgfid, toPrint)
    return max_time

def getScores(mgfDict,timeDict):
    consumer_scores = KafkaConsumer("scores"
                                     ,bootstrap_servers=["localhost:9092"]
                                     )
    print ("Ready to collect scores!")
    for msg in consumer_scores:
        if "__init__" not in msg.key.decode('utf-8'):
            collect(msg, mgfDict,timeDict)

def getuidMetadata(mgfDict,timeDict):
    try:
        consumer_uidMatches = KafkaConsumer("numofmatches"
                                            ,bootstrap_servers=["localhost:9092"]

                                            )
        consumer_uidMatches.subscribe("numofmatches")
    except Exception as e:
        print("Exception in Kafka consumer in numofmatches Collector: " + e.message)
    print("Ready to consume numofmatches")
    for msg in consumer_uidMatches:
        #print(str(msg))
        key1 = msg.key.decode('utf-8')
        val1 = msg.value.decode('utf-8')
        if "__final__" not in key1 and "__init__" not in key1:
            key2 = key1.split("=")[1]
            val2 = val1.split("=")[1]
            toPrint = "%s has %s matches!\n" % (val2,key2)
            print (toPrint)

            mgfDict[val2] = int(key2)
            timeDict[val2] = dt.now()
            f = open('output/big_scorer2_testrun1.txt', 'a')
            f.write(toPrint)
            f.close()

def sendResults(producer_d4, keyToSend, valueToSend):
    producer_d4.send("results"
                  ,value = valueToSend.encode('utf-8')
                  , key = keyToSend.encode('utf-8'))



mgfClassInstances = []
mgfDict = {}
receivedMatches = 0
timeDict = {}

if __name__ == '__main__':
    manager = Manager()
    mgfDict = manager.dict()
    timeDict = manager.dict()

    t1 = mp.Process(target=getuidMetadata, args=(mgfDict,timeDict,))
    t1.start()
    t2 = mp.Process(target=getScores, args=(mgfDict,timeDict,))
    t2.start()
    t1.join()
    t2.join()
