from cassandra.cluster import Cluster
from kafka import KafkaProducer
from kafka import KafkaConsumer
import tensorflow as tf

import os
import re

vector_location = os.path.join(os.path.dirname(__file__), 'datafiles')
receivedPairs = []
flag_dummyScore = True

# internal variables
m_lM = []
m_fI = []
m_plSeq = []
m_pfSeq = []

def setVariables():
    del m_lM[:]
    del m_fI[:]
    del m_plSeq[:]
    del m_pfSeq[:]

    if flag_dummyScore is True:
        vector1 = []
        vector2 = []
        for file in os.listdir(vector_location):
         if file.endswith(".vector"):
            with open(vector_location + "/" + file, 'r') as content_file:
                bothv = content_file.read().lstrip()
            vectors = re.split(' *v\d+= *', bothv)
            for v in vectors:
                if v.lstrip() is not "":

                    if len(vector1) > 0:
                        vector2 = v.lstrip().split('\n')
                    else:
                        vector1 = v.lstrip().split('\n')
            for line in vector1:
                if line.lstrip() is not "":
                    vectorValues = line.split("\t")
                    m_lM.append(float(vectorValues[0]))
                    m_fI.append(float(vectorValues[1]))

            for line in vector2:
                if line.lstrip() is not "":
                    vectorValues = line.split("\t")
                    m_plSeq.append(float(vectorValues[0]))
                    m_pfSeq.append(float(vectorValues[1]))

            f = calculateScore()
            return f

def readFromPairBuilder():

    consumer_c3 = KafkaConsumer('pairs'
                                , bootstrap_servers=['localhost:9092']
                                , group_id='apoorva-thesis')
    print("Consumer is ready to listen!")
    for msg in consumer_c3:
        if "__init__" not in msg.key:
            pairdata = eval(str(msg.value))
            print "Received Pair: " ,pairdata
            currScore = setVariables()
            sendScores(pairdata[0], pairdata[1], currScore)

def calculateScore():
    lcount = 0
    fscore = 0.0
    ind_v1 = []
    ind_v2 = []
    dot_v1 = []
    dot_v2 = []

    ind_v1, ind_v2 = [i for i, item in enumerate(m_lM) if item in m_plSeq], [i for i, item in enumerate(m_plSeq) if
                                                                          item in m_lM]
    dot_v1, dot_v2 = [item for i, item in enumerate(m_fI) if i in ind_v1], [item for i, item in enumerate(m_pfSeq) if
                                                                            i in ind_v2]
    tfsess = tf.Session()
    dotProducts = tfsess.run(tf.multiply(dot_v1, dot_v2))

    lcount = len(ind_v1)
    for dp in dotProducts:
        fscore += dp

    return fscore

def sendScores(mgfid, fastaid, currScore):
    producer_c3 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_c3.flush()
    formedKey = mgfid+"#"+fastaid
    formedKey = str(formedKey.encode('utf-8'))

    try:
        producer_c3.send("scores"
                         ,value = str(currScore).encode('utf-8')
                         ,key = formedKey.encode('utf-8'))
    except Exception as e:
        print("Exception in Kafka producer in score sending: " + e.message)
    finally:
        producer_c3.close()

readFromPairBuilder()

