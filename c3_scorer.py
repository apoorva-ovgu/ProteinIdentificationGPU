from cassandra.cluster import Cluster
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import SimpleClient
import tensorflow as tf

import os
import re

vector_location = os.path.join(os.path.dirname(__file__), 'datafiles')
receivedPairs = []
currScore = 0.0
flag_dummyScore = True

#internal variables
m_lM = []
m_fI = []
m_plSeq = []
m_pfSeq = []



def setVariables():
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

            print "Fscore for file",file," is ",calculateScore()

        # for file in os.listdir(vector_location):
        #     if file.endswith(".v1") :
        #         for line in open(vector_location + "/" + file, 'U'):
        #             if line.lstrip() is not "":
        #
        #                 line = line.rstrip('\n')
        #                 vectorValues = line.split("\t")
        #                 m_lM.append(float(vectorValues[0]))
        #                 m_fI.append(float(vectorValues[1]))
        #     if file.endswith(".v2"):
        #         for line in open(vector_location + "/" + file, 'U'):
        #             if line.lstrip() is not "":
        #                 line = line.rstrip('\n')
        #                 vectorValues = line.split("\t")
        #                 m_plSeq.append(float(vectorValues[0]))
        #                 m_pfSeq.append(float(vectorValues[1]))


def readFromPairBuilder():

    try:
        consumer = KafkaConsumer('pairs', bootstrap_servers=['localhost:9092'], group_id='apoorva-thesis')

        for msg in consumer:
            print "Reading",msg
            receivedLine = msg.value.decode("utf-8")
            print("Received Pair: ",receivedLine)
            setVariables()
    except Exception as e:
        print("Exception in Kafka consumer in scorer: "+ e.message)
    finally:
        KafkaConsumer.close(consumer)

def calculateScore():
    lcount = 0
    fscore = 0.0

    ind_v1, ind_v2 = [i for i, item in enumerate(m_lM) if item in m_plSeq], [i for i, item in enumerate(m_plSeq) if
                                                                          item in m_lM]
    dot_v1, dot_v2 = [item for i, item in enumerate(m_fI) if i in ind_v1], [item for i, item in enumerate(m_pfSeq) if
                                                                            i in ind_v2]
    sess = tf.Session()
    dotProducts = sess.run(tf.multiply(dot_v1, dot_v2))

    lcount = len(ind_v1)
    for dp in dotProducts:
        fscore += dp
    # print "Lcount is ", lcount, " and Fscore is ", finalscore
    return fscore

def sendScores():
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
        producer.send("scores", currScore)
    except Exception as e:
        print("Exception in Kafka producer in scorer: " + e.message)

readFromPairBuilder()

