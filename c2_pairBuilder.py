import uuid
import re
import itertools
from datetime import timedelta, datetime as dt
from operator import itemgetter
from kafka import KafkaConsumer
from kafka import KafkaProducer
from dbOperations import connectToDB

flag_compareAll = False
fastaSpectrumIDs = []
mgf_id = "Not Set"
curr_mgf_pepmass = 0.0

filter_pepmass = 100
filter_intensity = 1.0

def filldb(mgf_id, mgf_metadata, mgf_title, mgf_mass, mgf_charge,mgf_sequence):
    session = connectToDB()
    session.execute("""INSERT INTO mgf.exp_spectrum (id, allmeta, title, pepmass, charge, data) 
        VALUES (%s, %s, %s, %s ,%s, %s)""",
        (mgf_id,  mgf_metadata,  mgf_title, mgf_mass, mgf_charge, mgf_sequence)
    )
    session.shutdown()

def table_contents(toselect, table_name):
    cass_session = connectToDB()
    tempArray = []

    query = "SELECT " + toselect + " FROM " + table_name + ";"
    select_results = cass_session.execute(query)

    if "*" not in toselect:
        for row in select_results:
            stringRes = str(eval("row." + toselect))
            tempArray.append(stringRes)
    else:
        for row in select_results:
            tempArray.append(row)
    cass_session.shutdown()

    return tempArray

def createPairs(createForId):
    print "Creating pairs for ",createForId
    pairs_arr = []
    if flag_compareAll is True:
        for element in itertools.product([createForId], fastaSpectrumIDs):
            pairs_arr.append(element)

    else:
        selectedFastas = []
        min_threshold = curr_mgf_pepmass - filter_pepmass
        max_threshold = curr_mgf_pepmass + filter_pepmass
        for f in fastaSpectrumIDs:
            if float(f[1]) > min_threshold and float(f[1]) < max_threshold:
                selectedFastas.append(f[0])
        print selectedFastas
        for element in itertools.product([createForId], selectedFastas):
            pairs_arr.append(element)


    producer_uidMatches = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_uidMatches.flush()
    try:
        producer_uidMatches.send("uidMatches"
                      , value=str(len(pairs_arr)).encode('utf-8')
                      , key= str(createForId).encode('utf-8'))
    except Exception as e:
        print("Leider exception in producer_uidMatches producer: " + str(e))

        producer_uidMatches.send("uidMatches"
                          , value=b'code by apoorva patrikar'
                          , key=b'__final__')
        producer_uidMatches.close()

    return pairs_arr

def sendPairs(pairsCreated, time):
    producer_c2 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_c2.flush()
    #producer_c2.send("UIDSandMGF"
    #              , value=b'code by apoorva patrikar'
    #              , key=b'__init__')
    loadBalancer = 0
    for couple in pairsCreated:
        couple = couple + (loadBalancer,) + (time,)
        packagedCouple = str(couple).encode('utf-8')

        loadBalancer+=1
        if (loadBalancer > 7): #This what we change for scaling.
            loadBalancer=0
        try:
            producer_c2.flush()
            #producer_c2.send("pairs"+str(loadBalancer)
            producer_c2.send("pairs1"
                                ,key = "keyforpair".encode('utf-8')
                                 ,value = packagedCouple)
        except Exception as e:
            print("Exception in Kafka producer in pairbuilder: " + e.message)
        finally:
            print "Paired as ", couple
    producer_c2.close()

def storeMGF(mgfid, mgfContent):
    global curr_mgf_pepmass
    print("Received spectrum for ID ", mgfid)
    tmpArr = mgfContent.split("#")    #0:data 1:metadata

    metadata = tmpArr[1].split("\n")
    title = ""
    pepmass = 0
    charge = ""

    for md in metadata:
        if "TITLE" in md:
           title = md.split("=",1)[1]
        elif "PEPMASS" in md:
            pepmassAndIntensity = md.split("=", 1)[1]
            #pepmass = pepmassAndIntensity.split("\t")[0]
            #curr_mgf_pepmass = float(pepmass.lstrip())
            p = re.compile('(\w+)')
            m = p.match(pepmassAndIntensity)
            curr_mgf_pepmass = float(m.group(1))

        elif "CHARGE" in md:
            charge = md.split("=", 1)[1]
    try:
        filldb(uuid.UUID('{' + mgfid + '}'), tmpArr[1], title, curr_mgf_pepmass, charge,tmpArr[0])
    except Exception as e:
        print("Error filling exp_spectrum: " + str(e))

def readFromFastaDB():
    cass_session = connectToDB()
    query = "SELECT peptide_id,pep_mass FROM fasta.pep_spec";
    select_results = cass_session.execute(query)
    for row in select_results:
        stringId = str(eval("row.peptide_id"))
        stringMass = str(eval("row.pep_mass"))
        fastaSpectrumIDs.append((stringId,stringMass))
    cass_session.shutdown()

def filter1(mgf_received):
    filter1_mgf = ""
    start_flag = False
    fm = ()  # previous line's mz,intensity
    data_arr = mgf_received.split("\n")
    ctr = 0
    prev_inserted = False

    for eachMGFrow in data_arr:
        ctr +=1
        if "\t" in eachMGFrow:
            x = eachMGFrow.split("\t")
        else:
            x = eachMGFrow.split(" ")

        if start_flag is False:
            start_flag = True
            prev_fm = (float(x[0]),float(x[1]))
            pass
        else:
            if ctr == len(data_arr):
                break
            fm = (float(x[0]), float(x[1]))

            filter_diff = fm[0] - prev_fm[0]  # remove isotopes
            prev_inserted = False
            if (fm[0] < 200) or (filter_diff >= 0.95):
                filter1_mgf += str(prev_fm[0]) + "\t" + str(prev_fm[1]) + "\n"
                prev_fm = fm
                prev_inserted = True
            elif (fm[1]>prev_fm[1]):
                prev_fm = fm
            #else:
                #print "lt 200 crit= ",str(fm[0]),"  or  gt 0.95... ",str(filter_diff)
                #print "previous (what did not get added, for now)= ",str(prev_fm)

    filter1_mgf += eachMGFrow+ "\n"
    print "First filter spplied, size = "+str(len(filter1_mgf.split("\n")))
    return filter1_mgf

def filter2(mgf_received, highestIntensity):
    #print "after f1:::\n",str(mgf_received)
    filter_threshold = 1.0
    filter_dynamicrange = 100
    filter2_mgf = ""
    data_arr = mgf_received.split("\n")
    ctr = 0
    for eachMGFrow in data_arr:
        ctr += 1
        if eachMGFrow.lstrip()!="":
            if "\t" in eachMGFrow:
                x = eachMGFrow.split("\t")
            else:
                x = eachMGFrow.split(" ")
            #print str(x)
            oldIntensity =  float(x[1])
            newIntensity = oldIntensity / (float(highestIntensity) / filter_dynamicrange)
            if newIntensity >= filter_threshold:
                filter2_mgf+= x[0] + "\t" + str(newIntensity) + "\n"
            #else:
            #    tmp = x[0] + "\t" + str(newIntensity) + "\n"
            #    if 200<float(x[0])<300:
            #        print "failed ",tmp

    #print "after f2:::\n", str(filter2_mgf)
    print "Second filter spplied, size = "+str(len(filter2_mgf.split("\n")))+"\non highest "+str(highestIntensity)
    return filter2_mgf

def filter3(mgf_received):
    filter3_mgf = ""
    start_flag = False
    fm = ()  # previous line's mz,intensity
    data_arr = mgf_received.split("\n")
    ctr = 0
    prev_inserted = False
    for eachrow3 in data_arr:
        ctr += 1
        if "\t" in eachrow3:
            x = eachrow3.split("\t")
        else:
            x = eachrow3.split(" ")

        if start_flag is False:
            start_flag = True
            prev_fm = (float(x[0]), float(x[1]))
            pass
        else:
            if ctr == len(data_arr):
                break
            fm = (float(x[0]), float(x[1]))

            filter_diff = fm[0] - prev_fm[0]  # remove isotopes
            if (fm[0] < 200) or (filter_diff >= 1.5):
                filter3_mgf += str(prev_fm[0]) + "\t" + str(prev_fm[1]) + "\n"
                prev_fm = fm
            elif (fm[1] > prev_fm[1]):
                prev_fm = fm

    filter3_mgf += eachrow3 + "\n"
    #print str(filter3_mgf)
    print "Third filter spplied, size = " + str(len(filter3_mgf.split("\n")))
    return filter3_mgf

def filter4(mgf_received):
    filter4_mgf = ""
    data_arr = mgf_received.split("\n")
    ctr = 0
    tosort_array = []
    sorted_array = []

    for eachMGFrow in data_arr:
        ctr += 1
        if eachMGFrow.lstrip() != "":
            if "\t" in eachMGFrow:
                x = eachMGFrow.split("\t")
            else:
                x = eachMGFrow.split(" ")
            #sort by intensities
            tosort_array.append((float(x[0]),float(x[1])))

    sorted_array = sorted(tosort_array,key=itemgetter(1), reverse=True)

    ctr = 50
    for topn in sorted_array:
        ctr-=1
        if ctr>0:
            filter4_mgf+= str(topn[0]) + "\t" + str(topn[1]) + "\n"
        else:
            break

    #print filter4_mgf
    print "Fourth filter applied, size = " + str(len(filter4_mgf.split("\n")))
    return  filter4_mgf

def postProcessMgf(message):
    currMgfSpectra = ""
    fullMGFkey = message.key.split("#")
    highestIntensity = float(fullMGFkey[1])
    rawmgf = ""
    rawmetadata = ""

    mgf1 = message.value.split("\n")
    p = re.compile('(\d+\.*\d*)[ \t](\d+\.*\d*)[ \t]*(\d*\+*)')
    for eachMGFrow in mgf1:
        m = p.match(eachMGFrow)
        if m is not None:
            #print "data: apply filters"
            rawmgf+= eachMGFrow + "\n"
        else:
            #print "metadata"
            rawmetadata += eachMGFrow + "\n"

    f1mgf = filter1(rawmgf)  # 3: remove isotopes
    f2mgf = filter2(f1mgf, highestIntensity)  # 6: /100
    f3mgf = filter3(f2mgf)  #
    f4mgf = filter4(f3mgf)

def postProcessMgf2(message):
    currMgfSpectra = ""
    fullMGFkey = message.key.split("#")
    highestIntensity = int(fullMGFkey[1])

    mgf1 = message.value.split("\n")
    p = re.compile('(\d+\.*\d*)[ \t](\d+\.*\d*)[ \t]*(\d*\+*)')
    start_flag = False
    ok_flag = False
    fm = () #previous line's mz,intensity

    for eachMGFrow in mgf1:
        m = p.match(eachMGFrow)
        if start_flag is False and m is not None:
            start_flag = True
            prev_fm = (float(m.group(1)),float(m.group(2)))
            pass
        else:

            if m is not None:
                #**** 3 *****
                fm = (float(m.group(1)), float(m.group(2)))

                filter_diff = fm[0]-prev_fm[0]  #remove isotopes
                if (fm[0] < 200) or (filter_diff>=0.95):
                    ok_flag = True

                else:
                    print "old fm - new fm is " + str(fm[0] - prev_fm[0])
                    ok_flag = False
                # **** 3 *****
                # **** 6 *****
                # **** 6 *****

                if ok_flag is True:
                    #intensity processing
                    oldIntensity = prev_fm[1]
                    newIntensity = oldIntensity / highestIntensity * 100
                    oldmz = prev_fm[0]

                    if newIntensity > filter_intensity :
                        # MZprocessing
                        newMZ = round(oldmz) / 0.4
                        processedLine = str(newMZ) + "\t"
                        processedLine += str(round(newIntensity,3))+ "\n"

                        # skip charge even if exists
                        #blurring with m_dwidth, here width is 1
                        prevSpecLine = str(newMZ - 1) + "\t" + str(newIntensity) + "\n"
                        nextSpecLine = str(newMZ + 1) + "\t" + str(newIntensity) + "\n"

                        currMgfSpectra += prevSpecLine
                        currMgfSpectra += processedLine
                        currMgfSpectra += nextSpecLine
                    else:
                        pass
                    print "prev fm being processed ", str(prev_fm), " at mz ", str(fm[0])
                    prev_fm = fm
            else:
                #metadata
                currMgfSpectra += eachMGFrow + "\n"


    print "Postprocessed MGF= ", currMgfSpectra
    return currMgfSpectra

def run_step2():
    consumer_c2 = KafkaConsumer('topic_mgf'
                                ,bootstrap_servers=['localhost:9092']
                                , group_id='apoorva-thesis')
                                #, auto_offset_reset='earliest')
    session = connectToDB()
    session.execute("""TRUNCATE table mgf.exp_spectrum  """)
    session.shutdown()

    readFromFastaDB()

    print("Consumer is ready to listen!",consumer_c2.poll())
    for message in consumer_c2:
        if "__init__" not in message.key:
            filteredMGFdata = postProcessMgf(message)
            fullMGFkey = message.key.split("#")
            preTime = dt.now()
            storeMGF(fullMGFkey[0], filteredMGFdata)
            pairsCreated = createPairs(fullMGFkey[0])
            postTime = dt.now()
            sendPairs(pairsCreated, timedelta.total_seconds(postTime-preTime))  ### timedelta:  (days, seconds and microseconds)
        else:
            print "Initializing.... send again"

run_step2()