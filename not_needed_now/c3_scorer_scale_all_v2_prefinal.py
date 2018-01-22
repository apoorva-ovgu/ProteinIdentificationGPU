from kafka import KafkaProducer
from kafka import KafkaConsumer
from datetime import timedelta, datetime as dt
from dbOperations import connectToDB

import sys
import uuid
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

cass_session = connectToDB()

def loadRealData(mgfid, fastaid):
    global allMatchesToCompute
    # xtandem: m_lM = the M+H + error for an mspectrum
    global m_lM  # a vector of MZ values of MGF
    # xtandem: m_fI = the M+H - error for an mspectrum
    global m_fI  #a vector of intensities of MGF
    # xtandem: plSeq = residue masses corresponding to the current sequence, converted into integers
    global m_plSeq  #
    # xtandem: m_pfSeq = residue masses corresponding to the current sequence in daltons
    global m_pfSeq  #

    del m_lM[:]
    del m_fI[:]
    del m_plSeq[:]
    del m_pfSeq[:]


    query = "SELECT data FROM mgf.exp_spectrum where id = "+mgfid+";" #\t separated
    select_results = cass_session.execute(query)
    for row in select_results:
        data_mgf = str(eval("row.data")).split("\n")
        for el in data_mgf:
            if el.lstrip()!="":
                line1 = []
                if "\t" in el:
                    line1 = el.split("\t")
                else:
                    line1 = el.split(" ")
                m_lM.append(float(line1[0])) #mgf-mz
                m_fI.append(float(line1[1])) #mgf-intensity


    query = "SELECT mz_values FROM fasta.pep_spec " \
            "where spectrum_id = "+ fastaid +" " \
            " limit 50 ALLOW FILTERING ;";  # comma separated
    select_results = cass_session.execute(query)
    for row in select_results:
        m_plSeq_str = str(eval("row.mz_values")).split(",")
        for stof in m_plSeq_str:
            toInsert = float(stof)
            m_plSeq.append(round(toInsert * 2.5))
            m_pfSeq.append(1)


def calculateScore():
    global m_lM
    global m_fI
    global m_plSeq
    global m_pfSeq

    lcount = 0
    fscore = 0.0
    dot_v1 = []
    dot_v2 = []

    # print("m_plSeq= ", m_plSeq)
    # print("m_pfSeq= ", m_pfSeq)
    # print("m_lM= " ,str(m_lM))
    # print("m_fI= ",str(m_fI))


    for x in m_lM:
        for y in m_plSeq:
            if x == int(y):
                #print("Condition matched!! "+str(x))
                dot_v1.append( m_fI[m_lM.index(x)])
                dot_v2.append(m_pfSeq[m_plSeq.index(x)])
    dotProducts = []

    os.environ["CUDA_VISIBLE_DEVICES"] = ""
    tfsess = tf.Session()
    dotProducts = tfsess.run(tf.multiply(dot_v1, dot_v2))

    lcount = len(dot_v1)
    for dp in dotProducts:
        fscore += dp


    return fscore

def sendScores(mgfid, fastaid, currScore, timeReqd):
    producer_c3 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    try:
        producer_c3.send("scores"
                     , value="".encode('utf-8')
                     , key="__init__".encode('utf-8'))

    except Exception as e:
        print("Caught an exception in Kafka producer: " + str(e))
    if mgfid is not None and fastaid is not None:
        formedKey = mgfid+"#"+fastaid+"#"+str(timeReqd)
        try:
            producer_c3.send("scores"
                             ,value = str(currScore).encode('utf-8')
                             ,key = formedKey.encode('utf-8'))
            producer_c3.flush()
        except Exception as e:
            print("Exception in Kafka producer in score sending: " + e.message)
    else:
        print("Sent all scores")
        producer_c3.close()

def readFromPairBuilder(scorer_id):
    print(scorer_id)
    try:
        consumer_c3 = KafkaConsumer("pairs"+str(scorer_id)
                                , bootstrap_servers=['localhost:9092']
                                , group_id='apoorva-thesis')
        print("Consumer is ready to listen!")
    except Exception as e:
        print("Caught an exception in Kafka consumer: " + str(e))
    temp = 1
    currScore = -1
    prof_ctr = 10

    for msg in consumer_c3:  #(mgfID, fastaID), load, time in ms
        if "__final__" in msg.key.decode('utf-8'):
            print ("All pairs created... Shutting down cassandra connection")
            cass_session.shutdown()
            sendScores(None, None, None, None)
            sys.exit(0)

        if "__init__" not in msg.key.decode('utf-8'):
            pairdata = eval(str(msg.value.decode('utf-8')))
            print ("Received Pair: " ,pairdata)
            preTime = dt.now()
            loadRealData(pairdata[0],pairdata[1])
            try:
                currScore = calculateScore()
                st = "Final score of pair "+ str(temp)+ " is: "+ str(currScore)+ " at time "+str(dt.now())
                print (st)
            except Exception as e:
                print ("Error occured in pair ", temp, "....Hence skipped\n", str(e))

            temp+=1
            postTime = dt.now()
            sendScores(pairdata[0], pairdata[1], currScore, pairdata[3]+timedelta.total_seconds(postTime-preTime))

readFromPairBuilder(sys.argv[1])

