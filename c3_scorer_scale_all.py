from kafka import KafkaProducer
from kafka import KafkaConsumer
from datetime import timedelta, datetime as dt
import sys
from dbOperations import connectToDB

import os
import re
import itertools
import tensorflow as tf

vector_location = os.path.join(os.path.dirname(__file__), 'datafiles')
receivedPairs = []
flag_dummyScore = True

# internal variables
m_lM = []
m_fI = []
m_plSeq = []
m_pfSeq = []
vector1 = []
vector2 = []
allMatchesToCompute = []

def loadFakeData():
    global allMatchesToCompute
    global vector1
    global vector2

    allv1 = ""
    allv2 = ""


    vector1_location = os.path.join(os.path.dirname(__file__), 'datafiles/v1.txt')
    vector2_location = os.path.join(os.path.dirname(__file__), 'datafiles/v2.txt')

    with open(vector1_location, 'r') as content_file:
        allv1 = content_file.read().lstrip()
    with open(vector2_location, 'r') as content_file:
        allv2 = content_file.read().lstrip()

    vector1 = re.split("v1= \(\d+\)\n", allv1)
    vector2 = re.split("v2= \(\d+\)\n", allv2)

    for cpair in itertools.product(vector1, vector2):
        if cpair[0] == "" or cpair[1] == "":
            pass
        else:
            allMatchesToCompute.append(cpair)

def setVariables(pairdata, itercount):
    global m_lM
    global m_fI
    global m_plSeq
    global m_pfSeq


    del m_lM[:]
    del m_fI[:]
    del m_plSeq[:]
    del m_pfSeq[:]

    f = 0.0
    v1 = allMatchesToCompute[itercount][0]
    v2 = allMatchesToCompute[itercount][1]
    elements1 = v1.split("\n")
    elements2 = v2.split("\n")

    for elements in elements1:
        if elements.lstrip()!="":
            frag = elements.split("\t")
            m_lM.append(float(frag[0]))
            m_plSeq.append(float(frag[1]))

    for elements in elements2:
        if elements.lstrip()!="":
            frag = elements.split("\t")
            m_fI.append(float(frag[0]))
            m_pfSeq.append(float(frag[1]))

    try:
        f = calculateScore()
        print "Final score of pair ", itercount, " is: ", f
    except Exception as e:
         print "Error occured in pair ", itercount, "....Hence skipped\n",str(e)

    return f

def readFromPairBuilder(scorer_id):
    print(scorer_id)
    consumer_c3 = KafkaConsumer("pairs"+str(scorer_id)
                                , bootstrap_servers=['localhost:9092']
                                , group_id='apoorva-thesis')
    print("Consumer is ready to listen!")
    temp = 0
    for msg in consumer_c3:  #mgfID, fastaID, load, time in ms
        if "__init__" not in msg.key:
            pairdata = eval(str(msg.value))

            print "Received Pair: " ,pairdata
            preTime = dt.now()
            currScore = setVariables(pairdata,temp)
            temp+=1
            postTime = dt.now()
            sendScores(pairdata[0], pairdata[1], currScore, pairdata[3]+timedelta.total_seconds(postTime-preTime))

def calculateScore():
    global m_lM
    global m_fI
    global m_plSeq
    global m_pfSeq

    lcount = 0
    fscore = 0.0
    dot_v1 = []
    dot_v2 = []


    for x in m_lM:
        for y in m_fI:
            if x == y:
                dot_v1.append( m_plSeq[m_lM.index(x)])
                dot_v2.append(m_pfSeq[m_fI.index(x)])
    dotProducts = []
    tfsess = tf.Session()
    dotProducts = tfsess.run(tf.multiply(dot_v1, dot_v2))

    lcount = len(dot_v1)
    for dp in dotProducts:
        fscore += dp

    return fscore

def sendScores(mgfid, fastaid, currScore, timeReqd):
    producer_c3 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_c3.flush()
    formedKey = mgfid+"#"+fastaid+"#"+str(timeReqd)
    formedKey = str(formedKey.encode('utf-8'))

    try:
        producer_c3.send("scores"
                         ,value = str(currScore).encode('utf-8')
                         ,key = formedKey.encode('utf-8'))
    except Exception as e:
        print("Exception in Kafka producer in score sending: " + e.message)
    finally:
        producer_c3.close()

loadFakeData()
print(sys.argv[1])
readFromPairBuilder(sys.argv[1])

