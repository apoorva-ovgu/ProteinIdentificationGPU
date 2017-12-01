import uuid
import re
import itertools
from datetime import timedelta, datetime as dt

from kafka import KafkaConsumer
from kafka import KafkaProducer
from dbOperations import connectToDB

flag_compareAll = False
fastaSpectrumIDs = []
mgf_id = "Not Set"

filter_pepmass = 50
curr_mgf_pepmass = 0.0

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
            producer_c2.send("pairs"+str(loadBalancer)
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

def postProcessMgf(message):
    currMgfSpectra = ""
    fullMGFkey = message.key.split("#")
    highestIntensity = int(fullMGFkey[1])

    mgf1 = message.value.split("\n")
    p = re.compile('(\d+.\d+)\t(\d+)\t(\d\+)')

    for eachMGFrow in mgf1:
        m = p.match(eachMGFrow)
        if m is not None:
            newMZ = round(float(m.group(1)) / 0.4)
            processedLine = str(newMZ) + "\t"
            oldIntensity = int(float(m.group(2)))
            newIntensity = oldIntensity / highestIntensity * 100
            processedLine += str(newIntensity) + "\t" + str(m.group(3)) + "\n"

            prevSpecLine = str(newMZ - 1) + "\t" + str(newIntensity) + "\t" + str(m.group(3)) + "\n"
            nextSpecLine = str(newMZ + 1) + "\t" + str(newIntensity) + "\t" + str(m.group(3)) + "\n"

            currMgfSpectra += prevSpecLine
            currMgfSpectra += processedLine
            currMgfSpectra += nextSpecLine
        else:
            currMgfSpectra += eachMGFrow + "\n"
    #print "Postprocessed MGF= ", currMgfSpectra
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