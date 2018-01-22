from kafka import KafkaProducer, TopicPartition
from kafka import KafkaConsumer
from datetime import timedelta, datetime as dt
from dbOperations import connectToDB
from ast import literal_eval as make_tuple

import sys
import os
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
messages = set()
batch = set()
cass_session = connectToDB()
tv = 0


def loadRealData(mgfid, fastaid):
    print("Loading "+mgfid+" ...fasta "+fastaid)
    global allMatchesToCompute
    # xtandem: m_lM = the M+H + error for an mspectrum
    global m_lM  # a vector of MZ values of MGF
    # xtandem: m_fI = the M+H - error for an mspectrum
    global m_fI  # a vector of intensities of MGF
    # xtandem: plSeq = residue masses corresponding to the current sequence, converted into integers
    global m_plSeq  #
    # xtandem: m_pfSeq = residue masses corresponding to the current sequence in daltons
    global m_pfSeq  #

    del m_lM[:]
    del m_fI[:]
    del m_plSeq[:]
    del m_pfSeq[:]

    query = "SELECT data FROM mgf.exp_spectrum where id = " + mgfid + ";"  # \t separated
    select_results = cass_session.execute(query)
    for row in select_results:
        data_mgf = row.data.split("\n")
        for el in data_mgf:
            if el.lstrip() != "":
                line1 = []
                if "\t" in el:
                    line1 = el.split("\t")
                else:
                    line1 = el.split(" ")
                m_lM.append(float(line1[0]))  # mgf-mz
                m_fI.append(float(line1[1]))  # mgf-intensity

    query = "SELECT mz_values FROM fasta.pep_spec " \
            "where spectrum_id = " + fastaid + " " \
                                               "ALLOW FILTERING ;";  # comma separated
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
                dot_v1.append(m_fI[m_lM.index(x)])
                dot_v2.append(m_pfSeq[m_plSeq.index(x)])
    dotProducts = []

    os.environ["CUDA_VISIBLE_DEVICES"] = ""
    tfsess = tf.Session()
    dotProducts = tfsess.run(tf.multiply(dot_v1, dot_v2))

    lcount = len(dot_v1)
    for dp in dotProducts:
        fscore += dp

    return fscore

# def sendSingleScore_reference(pair_accumulator, score_accumulator, delta_accumulator):
#     # print("Sending to scores ",mgfid, fastaid, currScore, timeReqd)
#     producer_c3 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
#     producer_c3.send("scores"
#                      , value="".encode('utf-8')
#                      , key="__init__".encode('utf-8'))
#
#     pairdata = pair_accumulator[0]
#     mgfid = pairdata[0]
#     fastaid = pairdata[1]
#     currScore = score_accumulator[0]
#     timeReqd = delta_accumulator[0]
#
#     if mgfid is not None and fastaid is not None:
#         formedKey = mgfid + "#" + fastaid + "#" + str(timeReqd)
#         try:
#             producer_c3.send("scores"
#                      , value=str(currScore).encode('utf-8')
#                      , key=formedKey.encode('utf-8'))
#             producer_c3.flush()
#         except Exception as e:
#             print("Exception in Kafka producer in score sending: " + e.message)
#     else:
#         print("Sent all scores")
#         producer_c3.close()

def sendSingleScore(pair_accumulator, score_accumulator, delta_accumulator):
    producer_c3 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_c3.send("scores"
                     , value="".encode('utf-8')
                     , key="__init__".encode('utf-8'))

    if pair_accumulator is not None:
        #formedKey = mgfid + "#" + fastaid + "#" + str(timeReqd)
        batch_of_strings = ""
        for i in range(len(pair_accumulator)):
            toform = pair_accumulator[i]
            horizontal_string = toform[0] + "#" + toform[1] + "#" + str(score_accumulator[i])+ "#" + str(delta_accumulator[i])
            batch_of_strings+=horizontal_string+"_separator2_"

        try:
            producer_c3.send("scores"
                    , value=batch_of_strings.encode('utf-8')
                    , key="".encode('utf-8'))
            producer_c3.flush()
        except Exception as e:
            print("Exception in Kafka producer in score sending: " + e.message)
    else:
       print("Sent all scores")
       producer_c3.close()

def sendScores(mgfid, fastaid, currScore, timeReqd):
    # print("Sending to scores ",mgfid, fastaid, currScore, timeReqd)
    producer_c3 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_c3.send("scores"
                     , value="".encode('utf-8')
                     , key="__init__".encode('utf-8'))

    if mgfid is not None and fastaid is not None:
        formedKey = mgfid + "#" + fastaid + "#" + str(timeReqd)
        try:
            producer_c3.send("scores"
                             , value=str(currScore).encode('utf-8')
                             , key=formedKey.encode('utf-8'))
            producer_c3.flush()
        except Exception as e:
            print("Exception in Kafka producer in score sending: " + e.message)
    else:
        print("Sent all scores")
        producer_c3.close()

def processbatch(batch, temp):
    currScore = -1
    prof_ctr = 10
    pairno = len(batch)-1

    for msg in batch:
        pd1 = msg.value.split("_separator_")
        for pd2 in pd1:
            pairdata = pd2.split("#")
            preTime = dt.now()
            try:
                loadRealData(pairdata[0], pairdata[1])
                currScore = calculateScore()
                st = "Final score of pair " + str(temp-pairno) + " is: " + str(currScore) + " at time " + str(dt.now())
                print (st)
            except Exception as e:
                print ("Error occured in pair ", str(temp-pairno), "....Hence skipped\n", str(e))


            postTime = dt.now()
            try:
                sendScores(pairdata[0], pairdata[1], currScore, pairdata[3] + timedelta.total_seconds(postTime - preTime))
            except Exception as e:
                print("Exception: " + e.message)
            pairno-=1

# def processbatch_reference(batch, temp):
#     currScore = -1
#     prof_ctr = 10
#     pairno = len(batch) - 1
#     superstring= batch[0]
#     msgs = superstring.split("_separator_")
#     pair_accumulator = []
#     score_accumulator = []
#     delta_accumulator = []
#     for msg in msgs:
#         pairdata = make_tuple(msg.value)
#         preTime = dt.now()
#         try:
#             loadRealData(pairdata[0], pairdata[1])
#             currScore = calculateScore()
#             postTime = dt.now()
#             st = "Final score of pair " + str(temp - pairno) + " is: " + str(currScore) + " at time " + str(dt.now())
#             print (st)
#             pair_accumulator.append(pairdata)
#             score_accumulator.append(currScore)
#             delta_accumulator.append(timedelta.total_seconds(postTime - preTime))
#         except Exception as e:
#             print ("Error occured in pair ", str(temp - pairno), "....Hence skipped\n", str(e))
#     pairno -= 1
#     try:
#         sendSingleScore(pair_accumulator, score_accumulator, delta_accumulator)
#     except Exception as e:
#         print("Exception: " + e.message)

def processspectra(spectra_superstring):
    pair_accumulator = []
    score_accumulator = []
    delta_accumulator = []
    ctr = 0

    msgs = spectra_superstring.split("_separator_")
    print("msgs is ",str(msgs))
    displaymgf = msgs[0].split("#")
    print("displaymgf is ",str(displaymgf))

    print("Received superstring for " + displaymgf[0])

    for eachpair in msgs:
        ctr+=1
        if ctr == len(msgs):
            break

        preTime = dt.now()
        pairdata = eachpair.split("#")
        loadRealData(pairdata[0], pairdata[1])
        currScore = calculateScore()
        postTime = dt.now()

        st = "Final score of pair " + str(ctr) + " is: " + str(currScore) + " at time " + str(dt.now())
        print(st)
        pair_accumulator.append((pairdata[0], pairdata[1]))
        score_accumulator.append(currScore)
        delta_accumulator.append(timedelta.total_seconds(postTime - preTime))

    try:
        sendSingleScore(pair_accumulator, score_accumulator, delta_accumulator)
    except Exception as e:
        print("Exception: " + e.message)

# def readFromPairBuilder_reference(scorer_id, batchsize):
#     print(scorer_id)
#     temp = 0
#     start = 0
#     consumer_c3 = KafkaConsumer("pairs" + str(scorer_id)
#                                 , bootstrap_servers=['localhost:9092']
#                                 , group_id='apoorva-thesis')
#     print("Consumer is ready to listen!")
#     last_read = dt.now()
#     timeout_period = timedelta(seconds=5)
#     timeout = last_read + timeout_period
#
#     for msg in consumer_c3:  # (mgfID, fastaID), load, time in ms
#         if msg not in messages:
#             if "__init__" not in msg.key.decode('utf-8'):
#                 temp += 1
#                 if "__final__" not in msg.key.decode("utf-8"):
#                     batch.add(msg)
#
#                 if len(batch) == batchsize or dt.now() > timeout:
#                     print("Batch of " + str(len(batch)) + " sent for processing!")
#                     processbatch(batch, temp)
#                     batch.clear()
#                     last_read = dt.now()
#                     timeout_period = timedelta(seconds=5)
#                     timeout = last_read + timeout_period
#                 else:
#                     if "__final__" in msg.key.decode("utf-8"):
#                         print("Final Batch of " + str(len(batch)) + " sent for processing!")
#                         processbatch(batch, temp)
#                         batch.clear()
#                         sendScores(None, None, None, None)
#                         cass_session.shutdown()
#                         print ("All pairs created... Shut down cassandra connection")
#                         # sys.exit(0)
#             messages.add(msg)

def readFromPairBuilder(scorer_id, batchtype, batchsize):
    print(scorer_id)
    temp = 0
    consumer_c3 = KafkaConsumer("pairs" + str(scorer_id)
                                , bootstrap_servers=['localhost:9092']
                                , group_id='apoorva-thesis')
    print("Consumer for pairs"+str(scorer_id)+" is ready to listen!")
    last_read = dt.now()
    timeout_period = timedelta(seconds=5)
    timeout = last_read + timeout_period

    for msg in consumer_c3:  # superstring: (mgfID, fastaID, load, time) _separator_
        if "_init_" in msg.key.decode('utf-8'):
            pass
        elif "_final_" in msg.key.decode('utf-8'):
            pass
        elif msg not in messages:
            messages.add(msg)

            if batchtype == "predefined":
                batch.add(msg)
                if len(batch) == batchsize or dt.now() > timeout:
                    print("Batch of " + str(len(batch)) + " sent for processing!")
                    processbatch(batch, temp)
                    batch.clear()
                    last_read = dt.now()
                    timeout_period = timedelta(seconds=5)
                    timeout = last_read + timeout_period

            elif batchtype == "perspectra":
                messageinquestion = msg.value.decode('utf-8')
                processspectra(messageinquestion)

#readFromPairBuilder(1, "predefined", 5)  # args= scorernumber,batch type (predefined, perspectra), batch size
#readFromPairBuilder(sys.argv[1], "predefined", 500)
readFromPairBuilder(sys.argv[1], "perspectra", 0)
