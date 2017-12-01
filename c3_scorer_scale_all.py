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

def loadRealData(mgfid, fastaid):
    global allMatchesToCompute
    # xtandem: m_lM = the M+H + error for an mspectrum
    global m_lM  # a vector of integer masses of peptides (theo spectra)
    # xtandem: m_fI = the M+H - error for an mspectrum
    global m_fI  # a vector of weight factors for m_lM:::always 1 (theo spectrum)
    # xtandem: plSeq = residue masses corresponding to the current sequence, converted into integers
    global m_plSeq  # a vector of integer masses of exp_spectrum
    # xtandem: m_pfSeq = residue masses corresponding to the current sequence in daltons
    global m_pfSeq  # a vector of weight factors for m_plSeq

    del m_lM[:]
    del m_fI[:]
    del m_plSeq[:]
    del m_pfSeq[:]

    cass_session = connectToDB()
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
                m_plSeq.append(line1[0])
                m_pfSeq.append(line1[1])


    query = "SELECT mz_values FROM fasta.pep_spec where peptide_id = "+ fastaid +";";  # comma separated
    select_results = cass_session.execute(query)
    for row in select_results:
        m_lM = str(eval("row.mz_values")).split(",")
        for i in range(0,len(m_lM)):
            m_fI.append("1")

    print "m_plSeq= ", m_plSeq
    print "m_plSeq= ", m_pfSeq
    print "m_lM= ", m_lM
    print "m_fI= ", m_fI

    cass_session.shutdown()

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

def readFromPairBuilder(scorer_id):
    print(scorer_id)
    consumer_c3 = KafkaConsumer("pairs"+str(scorer_id)
                                , bootstrap_servers=['localhost:9092']
                                , group_id='apoorva-thesis')
    print("Consumer is ready to listen!")
    temp = 0
    currScore = -1
    for msg in consumer_c3:  #(mgfID, fastaID), load, time in ms
        if "__init__" not in msg.key:
            pairdata = eval(str(msg.value))
            print "Received Pair: " ,pairdata
            preTime = dt.now()
            loadRealData(pairdata[0],pairdata[1])
            try:
                currScore = calculateScore()
                print "Final score of pair ", temp, " is: ", currScore
            except Exception as e:
                print "Error occured in pair ", temp, "....Hence skipped\n", str(e)

            #currScore = setVariables(pairdata,temp)
            temp+=1
            postTime = dt.now()
            sendScores(pairdata[0], pairdata[1], currScore, pairdata[3]+timedelta.total_seconds(postTime-preTime))

readFromPairBuilder(sys.argv[1])

