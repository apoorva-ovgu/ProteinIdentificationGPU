from kafka import KafkaProducer, TopicPartition
from kafka import KafkaConsumer
from datetime import timedelta, datetime as dt
from dbOperations import connectToDB
from ast import literal_eval as make_tuple

import sys
import os
import tensorflow as tf
import pyopencl as cl
import numpy as np

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
casstime = 0
kafkatime = 0
tv = 0

os.environ['PYOPENCL_COMPILER_OUTPUT'] = '0'
os.environ['PYOPENCL_CTX'] = '0'

ctx = cl.create_some_context()
queue = cl.CommandQueue(ctx)
prg = cl.Program(ctx, """
    __kernel void multiply(
    ushort exp_size,
    ushort theo_size, 
    __global float *exp_compare,
    __global float *exp_score,
    __global float *theo_compare,  
    __global float *theo_score,  
    __global float *c)
    {
      int gid = get_global_id(0);
      if (gid < theo_size){
       for (int i=0; i<exp_size; i++){
         if (exp_compare[i] == theo_compare[gid]){
           c[gid]= theo_score[gid]*exp_score[i];
           return;
         }
       }
       c[gid] = 0; 
      }
    }
    """).build()

def loadRealMgf(mgfid):
    global casstime
    global kafkatime
    m_lM = []
    m_fI= []
    time_x = dt.now()
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
    time_y = dt.now()
    casstime = timedelta.total_seconds(time_y - time_x)
    return (m_lM, m_fI, len(m_lM))

def loadRealFasta(fastaids):
    global casstime
    global kafkatime
    time_x = dt.now()
    query = "SELECT spectrum_id, mz_values FROM fasta.pep_spec " \
            "WHERE spectrum_id IN (" + fastaids + ") ALLOW FILTERING ;";  # comma separated
    select_results = cass_session.execute(query)
    limit_counter = 0
    theo_comp = []
    theo_score=[]
    theo_limit=[0]
    theo_ids = []
    for row in select_results:
        m_plSeq = [] #comp
        m_pfSeq = [] #score
        m_plSeq_str = str(eval("row.mz_values")).split(",")
        theo_ids.append(str(row.spectrum_id))

        for stof in m_plSeq_str:
            toInsert = float(stof)
            m_plSeq.append(round(toInsert * 2.5))
            m_pfSeq.append(1)
        theo_comp.extend(m_plSeq)
        theo_score.extend(m_pfSeq)
        limit_counter += len(m_pfSeq)
        theo_limit.append(limit_counter)
    time_y = dt.now()
    casstime += timedelta.total_seconds(time_y - time_x)
    return (theo_ids, theo_comp, theo_score, theo_limit)

def loadRealData(mgfid, fastaid):
    #print("Loading "+mgfid+" ...fasta "+fastaid)
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

def sendSingleScore(pair_accumulator, score_accumulator, delta_accumulator, scorer_casstime, alternativekafkatime, secondKafkaTime, firstKafkaTime, casstime_pb, alternativeCassTimePb):
    #print("PA ",pair_accumulator)
    #print("SA ", score_accumulator)
    #print("DA ", delta_accumulator)

    producer_c3 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_c3.send("scores"
                     , value="".encode('utf-8')
                     , key="__init__".encode('utf-8'))

    #print ("We made it till some part of the single score sending...")
    if pair_accumulator is not None:
        batch_of_strings = ""
        ##here here here delete this -1 jhol
        for i in range(len(pair_accumulator)):
            #print("Iterator on "+str(i)+" pair of ")
            #print("PA ",pair_accumulator[i])
            #print("SA ", score_accumulator[i])
            #print("DA ", delta_accumulator[i])
            toform = pair_accumulator[i]
            #print("Forming "+str(toform[0]) + "#" + str(toform[1]) + "#" + str(score_accumulator[i])+ "#" + str(delta_accumulator[i]))
            horizontal_string = str(toform[0]) + "#" + str(toform[1]) + "#" + str(score_accumulator[i])+ "#" + str(delta_accumulator[i])
            batch_of_strings+=horizontal_string+"_separator2_"
        try:
            lastKafkaTime= dt.now()
            producer_c3.send("scores"
                    , value=batch_of_strings.encode('utf-8')
                    , key=(str(lastKafkaTime)+"_the_real_separator_"+
                          str(secondKafkaTime) + "_the_real_separator_" +
                          str(alternativekafkatime) + "_the_real_separator_" +
                          str(firstKafkaTime) + "_the_real_separator_" +
                          str(scorer_casstime) + "_the_real_separator_" +
                          str(casstime_pb) + "_the_real_separator_" +
                          str(alternativeCassTimePb)).encode('utf-8'))
            producer_c3.flush()
        except Exception as e:
            print("Exception in Kafka producer in score sending: " + e.message)
    else:
       print("Sent all scores")
       #producer_c3.close()

def sendScores(mgfid, fastaid, currScore, timeReqd):
    # print("Sending to scores ",mgfid, fastaid, currScore, timeReqd)
    producer_c3 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_c3.send("scores"
                     , value="".encode('utf-8')
                     , key="__init__".encode('utf-8'))

    if mgfid is not None and fastaid is not None:
        formedKey = mgfid + "#" + fastaid + "#" + str(timeReqd)+ "#" + str(kafkatime)+ "#" + str(casstime)
        try:
            producer_c3.send("scores"
                             , value=str(currScore).encode('utf-8')
                             , key=formedKey.encode('utf-8'))
            producer_c3.flush()
        except Exception as e:
            print("Exception in Kafka producer in score sending: " + e.message)
    else:
        print("Sent all scores")
        producer_c3.send("scores"
                         , value="".encode('utf-8')
                         , key="__final__".encode('utf-8'))
        #producer_c3.close()

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
                print("Exception lala: " + e.message)
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

def processspectra(spectra_superstring, kafkatime, secondKafkaTime, firstKafkaTime, casstime2, alternativeCassTime):
    #pair_accumulator = []
    #score_accumulator = []
    #delta_accumulator = []
    #ctr = 0
    global casstime
    msgs = spectra_superstring.split("_separator_")
    #print("msgs is ",str(msgs))
    displaymgf = msgs[0].split("#")
    #print("displaymgf is ",str(displaymgf))

    #print("Received superstring for " + displaymgf[0])
    exp_tuple = loadRealMgf(str(displaymgf[0]))

    superstring2 = ""
    for eachpair in msgs:
        temp_list = eachpair.split("#")
        if len(temp_list)>1:
            superstring2+=temp_list[1]+","
    if len(superstring2[:-1])>1:
        superstring2= superstring2[:-1]
        theo_tuple = loadRealFasta(superstring2)
        #for abc in theo_tuple:
         #   print("Fasta ids (theo_tuple)", theo_tuple)

        exp_comp = np.array(exp_tuple[0]) #exp comp
        exp_comp.astype(np.float32)

        exp_score = np.array(exp_tuple[1]) #exp score
        exp_score.astype(np.float32)

        theo_comp = np.array(theo_tuple[1]) #theo comp
        theo_comp.astype(np.float32)

        theo_size= len(theo_tuple[2])
        c = np.zeros(theo_size, dtype=np.float32)

        theo_score = np.array(theo_tuple[2]) #theo score
        theo_score.astype(np.float32)

        mf = cl.mem_flags
        exp_comp_buf = cl.Buffer \
        (ctx, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=exp_comp)
        exp_score_buf = cl.Buffer \
        (ctx, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=exp_score)

        theo_compare_buf = cl.Buffer \
        (ctx, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=theo_comp)
        theo_score_buf = cl.Buffer \
            (ctx, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=theo_score)

        c_buf = cl.Buffer(ctx, mf.WRITE_ONLY, c.nbytes)

        preTime = dt.now()

        prg.multiply(queue, c.shape, None,
                     np.uint16(int(exp_tuple[2])), np.uint16(theo_size), exp_comp_buf, exp_score_buf,
                     theo_compare_buf, theo_score_buf, c_buf)

        result = np.empty_like(c)
        cl.enqueue_copy(queue, result, c_buf)

        collected_scores = []
        curr_tl_pos = 0
        theo_limits = theo_tuple[3]
        curr_c_pos = theo_limits[curr_tl_pos]

        aggregator = 0

        while (curr_c_pos < len(result)):
            curr_tl_pos += 1
            last_pos = theo_limits[curr_tl_pos]
            while (curr_c_pos < last_pos):
                aggregator += result[curr_c_pos]
                curr_c_pos += 1
            collected_scores.append(aggregator)
            aggregator = 0
            curr_c_pos = theo_limits[curr_tl_pos]

        postTime = dt.now()
        theo_ids = theo_tuple[0]

        #print("We finished calculating the score for "+ str(len(theo_ids))+"pairs")
        #print(theo_limits)
        #print(theo_size)
        #print(collected_scores)
        #st = "Final score of pair " + displaymgf[0] + " and "+str(theo_ids[0])+" is: " + str(collected_scores[0]) + " at time " + str(dt.now())
        #print(st)
        pair_accumulator = []
        delta_accumulator = []
        delta = timedelta.total_seconds(postTime - preTime)
        for tid in theo_ids:
            pair_accumulator.append((str(displaymgf[0]), tid))
            delta_accumulator.append(delta)
    #Printing time part
    #Sending scores part
        #print(st)
        #pair_accumulator.append((pairdata[0], pairdata[1]))
        #score_accumulator.append(currScore)
        #delta_accumulator.append(timedelta.total_seconds(postTime - preTime))
        try:
            sendSingleScore(pair_accumulator, collected_scores, delta_accumulator, casstime, kafkatime, secondKafkaTime, firstKafkaTime, casstime2, alternativeCassTime)
        except Exception as e:
            print("Exception::: " + e.message)
            print("\nLengths: ..PA"+str(len(pair_accumulator))+" ..SA"+str( len(collected_scores))+" ..DA"+str(len(delta_accumulator)))

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
    time_x = dt.now()
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
            timings = msg.key.decode('utf-8').split("_the_real_separator_")
            casstime2 = timings[3]
            secondKafkaTime= timedelta.total_seconds(dt.now()-dt.strptime(timings[0], '%Y-%m-%d %H:%M:%S.%f'))
            firstKafkaTime = timings[1]
            alternativeCassTime= timings[2]
            if batchtype == "predefined":
                batch.add(msg)
                if len (batch) == batchsize or dt.now() > timeout:
                    print("Batch of " + str(len(batch)) + " sent for processing!")
                    processbatch(batch, temp)
                    batch.clear()
                    last_read = dt.now()
                    timeout_period = timedelta(seconds=5)
                    timeout = last_read + timeout_period

            elif batchtype == "perspectra":
                messageinquestion = msg.value.decode('utf-8')
                time_y = dt.now()
                kafkatime = timedelta.total_seconds(time_y - time_x)
                processspectra(messageinquestion, kafkatime, secondKafkaTime, firstKafkaTime, casstime2, alternativeCassTime)

#readFromPairBuilder(1, "predefined", 5)  # args= scorernumber,batch type (predefined, perspectra), batch size
#readFromPairBuilder(sys.argv[1], "predefined", 500)

readFromPairBuilder(sys.argv[1], "perspectra", 0)
#readFromPairBuilder(1, "perspectra", 0)