import uuid
from kafka import KafkaConsumer
from kafka import KafkaProducer
from mgfClassFile import mgfClass
import multiprocessing as mp
from multiprocessing import Process, Manager
from dbOperations import connectToDB
from datetime import timedelta, datetime as dt
import time

giveup = 0
score_messages = set()
match_messages = set()
numPairBuilders= 4


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
    global giveup
    #scoreLine = pair, score, delta
    # pair = superstring = couple + "_separator_"
    # couple = mgf, fasta

    ##past receivedScore = scoreLine.value.decode('utf-8')
    ##past receivedScoreFor = scoreLine.key.decode('utf-8').split("#")
    ##past lookFor = receivedScoreFor[0]

    allrec = scoreLine.value.split("_separator2_")
    key_recvd = scoreLine.key.decode('utf-8').split("_the_real_separator")
    del allrec[-1]
    for new_array_temp  in allrec:
        new_array = new_array_temp.split("#")

        mgfid = new_array[0]
        fastaid = new_array[1]
        pairscore = new_array[2]
        pairtimedelta = new_array[3]

        lastKafkaTime = timedelta.total_seconds(dt.now()-dt.strptime(key_recvd[0], '%Y-%m-%d %H:%M:%S.%f'))
        secondKafkaTime = key_recvd[1]
        alternativekafkatime = key_recvd[2]
        firstKafkaTime = key_recvd[3]
        firstKafkaTime = key_recvd[3]
        scorer_casstime = key_recvd[4]
        casstime_pb = key_recvd[5]
        alternativeCassTimePb = key_recvd[6]

        while mgfid not in mgfDict:
            print ("Delaying and waiting for "+mgfid)
            time.sleep(15)
        if mgfid in mgfDict:
            giveup = 0
            mgfDict[mgfid] = mgfDict[mgfid]-1
            mgfClassObj = \
                mgfClass(mgfid #name
                         ,fastaid #matchedWith
                         , pairscore  #score
                          ,mgfDict[mgfid] #remainingcomparisons
                         ,pairtimedelta #timeRequired
                         )
            mgfClassInstances.append(mgfClassObj)
            if int(mgfDict[mgfid]) == 0:
                    max_time = sortScores(mgfid)
                    a = dt.now()
                    b = timeDict[mgfid]
                    c = timedelta.total_seconds(a - b)

                    toprint ="\n["+str(dt.now())+"]"+mgfid+\
                             " :: end-to-end time from collector (use last for total time plot 1) "+ str(c)+ \
                             ", wait time (for wt plot 2) "+str(float(c)-float(max_time))+\
                             ", max_service_time (use all of them for service time plot 3) "+str(max_time)+\
                             ", in that:: last kafka time is "+str(lastKafkaTime)+ \
                             ", in that:: secondKafkaTime is " + str(secondKafkaTime) + \
                             ", in that:: alternativekafkatime is " + str(alternativekafkatime) + \
                             ", in that:: firstKafkaTime is " + str(firstKafkaTime) + \
                             ", in that:: scorer_casstime is " + str(scorer_casstime) + \
                             ", in that:: casstime_pb is " + str(casstime_pb) + \
                             ", in that:: alternativeCassTimePb is "+str(alternativeCassTimePb)

                    print(toprint)
                    f = open('output/d4 graph.txt', 'a')
                    f.write(toprint)
                    f.close()
            #else:
                #print(".....Hey!! "+str(int(mgfDict[mgfid]))+" this needs to be zero")
        # else:
        #     if giveup > 5:
        #         giveup = 0
        #         return
        #     print ("Hey, it wasn't in the dictionary!:: "+mgfid+" delaying now....")
        #     time.sleep(5)
        #     giveup+=1
        #     alldone = getuidMetadata(mgfDict, timeDict, mgfid)
        #     if alldone is True:
        #         collect(scoreLine, mgfDict, timeDict)

def collect_reference(scoreLine,mgfDict,timeDict):
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
        #storeScores(lookFor, receivedScoreFor[1], receivedScore)
        if int(mgfDict[receivedScoreFor[0]]) == 0:
                max_time = sortScores(receivedScoreFor[0])
                #print("***", str(max_time))
                a = dt.now()
                #print("b is ",timeDict[lookFor]," of type ",type(timeDict[lookFor]))
                b = timeDict[lookFor]
                c = timedelta.total_seconds(a - b)

                toprint ="["+str(dt.now())+"]"+str(lookFor)+\
                         "end-to-end time from collector (use last for total time plot 1) "+ str(c)+ \
                         ", wait time (for wt plot 2) "+str(float(c)-float(max_time))+\
                         ", max_service_time (use all of them for service time plot 3) "+str(max_time)+"\n"
                print(toprint)
                f = open('output/d4 graph.txt', 'a')
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
    toPrint = "\n["+str(dt.now())+"]"+" All results collected for "+str( mgfid)+ " and the Sorting order is:"
    print (toPrint)
    f = open('output/d4 results.txt', 'a')
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

        toPrint = " \n["+str(dt.now())+"]...matched with="+eachItem.match\
                  +" ...with a score of="+str(eachItem.score)\
                  +" ...and computing time (do not use these numbers)="+str(eachItem.timeReqd)

        #print (toPrint)
        f = open('output/d4 results.txt', 'a')
        f.write(toPrint)
        f.close()

        mgfClassInstances.remove(eachItem)

    sendResults(producer_d4, mgfid, toPrint)
    return max_time

def getScores(mgfDict,timeDict):
    consumer_scores = KafkaConsumer("scores"  ,bootstrap_servers=["localhost:9092"])
    print ("Ready to collect scores!")
    for msg in consumer_scores:
        if "__final__" in msg.key.decode('utf-8'):
            print("All data received!")
        #print("Collecting::: ",msg)
        if msg not in score_messages:
            if "__init__" not in msg.key.decode('utf-8'):
                collect(msg, mgfDict,timeDict)
            score_messages.add(msg)

def getuidMetadata(mgfDict,timeDict,callFrom):
    ctr = 0
    try:
        consumer_uidMatches = KafkaConsumer("numofmatches"
                                            ,bootstrap_servers=["localhost:9092"]

                                            )
        consumer_uidMatches.subscribe("numofmatches")
    except Exception as e:
        print("Exception in Kafka consumer in numofmatches Collector: " + e.message)
    print("Ready to consume numofmatches")
    for msg in consumer_uidMatches:
        if msg not in match_messages:
            key1 = msg.key.decode('utf-8')
            val1 = msg.value.decode('utf-8')
            if "__final__" in key1:
                ctr+=1
                if ctr==numPairBuilders:
                    print("All metadata received")
                    break
            elif "__init__" not in key1:
                key2 = key1.split("=")[1]
                val2 = val1.split("=")[1]
                toPrint = "["+str(dt.now())+"] %s has %s matches!\n" % (val2,key2)
                print (toPrint)

                mgfDict[val2] = int(key2)
                timeDict[val2] = dt.now()
                f = open('output/d4 results.txt', 'a')
                f.write(toPrint)
                f.close()

                if callFrom != "initial":
                    print("breaking from loop")
                    break
            match_messages.add(msg)
    if callFrom != "initial":
        print("returning to score collector")
        if callFrom in mgfDict:
            return True
        else:
            return False
    getScores(mgfDict, timeDict)

def sendResults(producer_d4, keyToSend, valueToSend):
    try:
        producer_d4.send("results"
                  ,value = valueToSend.encode('utf-8')
                  , key = keyToSend.encode('utf-8'))
    except Exception as e:
        print("Exception: " + e.message)


mgfClassInstances = []
mgfDict = {}
receivedMatches = 0
timeDict = {}

if __name__ == '__main__':
     manager = Manager()
     mgfDict = manager.dict()
     timeDict = manager.dict()

     #multiprocessing
     t1 = mp.Process(target=getuidMetadata, args=(mgfDict,timeDict,"initial"))
     t1.start()

     t2 = mp.Process(target=getScores, args=(mgfDict,timeDict,))
     t2.start()
     t1.join()
     t2.join()

     #serial procecessing
     #getuidMetadata(mgfDict,timeDict)
