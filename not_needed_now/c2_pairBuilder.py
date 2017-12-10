import uuid
import re
import itertools
import math, sys
from datetime import timedelta, datetime as dt
from operator import itemgetter
from kafka import KafkaConsumer
from kafka import KafkaProducer
from dbOperations import connectToDB

import cProfile
import cStringIO
import pstats

#Profile block 1
profile = False

if profile:
    pr = cProfile.Profile()
    pr.enable()
#End of Profile block 1


flag_compareAll = False
fastaSpectrumIDs = []
mgf_id = "Not Set"
curr_mgf_pepmass = 0.0
loadBalancer = 0

filter_pepmass = 100
filter_intensity = 1.0
filter_maxPeaks = 50

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

def createPairs(createForId, finalFlag):
    producer_uidMatches = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_uidMatches.flush()
    if finalFlag is True:
        producer_uidMatches.send("uidMatches"
                                 , value="".encode('utf-8')
                                 , key="__final__".encode('utf-8'))
        return 0
    else:
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
                    #print "shortlisted: "+str(f)
            #print selectedFastas
            for element in itertools.product([createForId], selectedFastas):
                pairs_arr.append(element)



        try:
            producer_uidMatches.send("uidMatches"
                          , value=str(len(pairs_arr)).encode('utf-8')
                          , key= str(createForId).encode('utf-8'))
            #print "Uid message sent- Key ",str(createForId)," Val ",str(len(pairs_arr))
        except Exception as e:
            print("Leider exception in producer_uidMatches producer: " + str(e))

        producer_uidMatches.send("uidMatches"
                              , value=b'code by apoorva patrikar'
                              , key=b'__final__')
        producer_uidMatches.close()

        return pairs_arr

def sendPairs(pairsCreated, time, finalFlag):
    producer_c2 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    producer_c2.flush()
    global loadBalancer

    if finalFlag is True:
        try:
            producer_c2.send("pairs"+str(loadBalancer)
                             , value="".encode('utf-8')
                             , key="__final__".encode('utf-8'))

        except Exception as e:
            print("Exception in closing Kafka producer c2: " + e.message)
        finally:
            producer_c2.close()
            sys.exit(0)


    for couple in pairsCreated:
        loadBalancer += 1

        if (loadBalancer > 8): #This what we change for scaling.
            loadBalancer = 1
        try:
            couple = couple + (loadBalancer,) + (time,)
            packagedCouple = str(couple).encode('utf-8')

            producer_c2.flush()
            producer_c2.send("pairs"+str(loadBalancer)
            #producer_c2.send("pairs1"
                                ,key = "keyforpair".encode('utf-8')
                                 ,value = packagedCouple)
        except Exception as e:
            print("Exception in Kafka producer in pairbuilder: " + e.message)
        finally:
            print "Paired as ", couple


def storeMGF(mgfid, mgfContent):
    global curr_mgf_pepmass
    print("Received spectrum for ID ", mgfid)
    tmpArr = mgfContent.split("#")    #0:data 1:metadata
    #print tmpArr
    metadata = tmpArr[1].split("\n")
    title = ""
    pepmass = 0
    charge = ""

    for md in metadata:
        if "TITLE" in md:
           title = md.split("=",1)[1]
        elif "PEPMASS" in md:
            pepmassAndIntensity = md.split("=", 1)[1]
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
    filter1_mgf = []
    x = []
    start_flag = False
    fm = ()  # previous line's mz,intensity
    data_arr = mgf_received.split("\n")
    #print "\nNo filter applied, size = "+str(len(data_arr))
    f_fm = 0.0
    prev_fm = ()

    ctr = 0
    prev_inserted = False

    for eachMGFrow in data_arr:
        if "\t" in eachMGFrow:
            x = eachMGFrow.split("\t")
        else:
            x = eachMGFrow.split(" ")

        if start_flag is False:
            start_flag = True
            prev_fm = (float(x[0]),float(x[1]))
            f_fm=float(x[0])
            pass
        else:
            if ctr == len(data_arr):
                break
            fm = (float(x[0]), float(x[1]))

            filter_diff = fm[0] - f_fm  # remove isotopes
            if (fm[0] < 200) or (filter_diff >= 0.95):
                #filter1_mgf += str(prev_fm[0]) + "\t" + str(prev_fm[1]) + "\n"
                filter1_mgf.append(prev_fm)
                prev_fm = fm
                f_fm = fm[0]
            elif (fm[1]>prev_fm[1]):
                prev_fm = fm
            #else:
                #print "lt 200 crit= ",str(fm[0]),"  or  gt 0.95... ",str(filter_diff)
                #print "previous (what did not get added, for now)= ",str(prev_fm)
            ctr += 1

    #print "Adding prev_fm "+str(prev_fm)
    filter1_mgf.append(prev_fm)
    #print "\nFirst filter applied, size = "+str(len(filter1_mgf))
    #print "After f1= "+str(filter1_mgf)
    return filter1_mgf

def filter2(mgf_received, highestIntensity):
    filter_threshold = 1.0
    filter_dynamicrange = 100
    filter2_mgf = []
    #data_arr = mgf_received.split("\n")
    ctr = 0
    for eachMGFrow in mgf_received:
      #  ctr += 1
      #  if eachMGFrow.lstrip()!="":
            oldIntensity =  float(eachMGFrow[1])
            newIntensity = oldIntensity / (float(highestIntensity) / filter_dynamicrange)
            if newIntensity >= filter_threshold:
                #filter2_mgf+= eachMGFrow[0] + "\t" + str(newIntensity) + "\n"
                filter2_mgf.append((eachMGFrow[0],newIntensity))
            #else:
            #    tmp = eachMGFrow[0] + "\t" + str(newIntensity) + "\n"
            #    if 200<float(x[0])<300:
            #        print "failed ",tmp

    #print "\nSecond filter applied, size = " + str(len(filter2_mgf))
    #print "After f2=", str(filter2_mgf)

    return filter2_mgf

def filter3(mgf_received):
    filter3_mgf = []
    start_flag = False
    fm = ()  # previous line's mz,intensity
    x = []
    prev_fm = ()
    f_fm = 0.0

    for eachrow3 in mgf_received:
        if start_flag is False:
            start_flag = True
            prev_fm = eachrow3
            f_fm = float(eachrow3[0])
            pass
        else:
            fm = eachrow3
            filter_diff = fm[0] - f_fm
            if (fm[0] < 200) or (filter_diff >= 1.5):
                filter3_mgf.append(prev_fm)
                prev_fm = fm
                f_fm = fm[0]
            elif (fm[1] > prev_fm[1]):
                prev_fm = fm

    filter3_mgf.append(prev_fm)

    #print "\nThird filter applied, size = " + str(len(filter3_mgf))
    #print "after f3= " + str(filter3_mgf)
    return filter3_mgf

def filter4(mgf_received):
    #print "Input mgf ",str(mgf_received)
    filter4_mgf = []
    ctr = 0

    sorted_array = sorted(mgf_received,key=itemgetter(1), reverse=True)
    ctr = filter_maxPeaks
    for topn in sorted_array:
        if ctr>0:
            filter4_mgf.append(topn)
        else:
            break
        ctr -= 1

    #print "\nFourth filter applied, size = " ,len(filter4_mgf)
    #print "after f4= " + str(filter4_mgf)
    return  filter4_mgf

def filter5(mgf_received):
    filter5mgf = ""
    #new_arr = []

    for eachline in mgf_received:
        currmz = eachline[0]
        currint = str(eachline[1])
        newmz = math.floor(currmz / 0.4)

        prevSpecLine = str(newmz-1) + "\t" + currint + "\n"
        processedLine = str(newmz) + "\t" + currint + "\n"
        nextSpecLine = str(newmz + 1) + "\t" + currint + "\n"

        filter5mgf += prevSpecLine
        filter5mgf += processedLine
        filter5mgf += nextSpecLine

    #    new_arr.append((newmz-1, currint))
    #    new_arr.append((newmz , currint))
    #    new_arr.append((newmz + 1, currint))

    #sorted_array = sorted(new_arr, key=itemgetter(0))
    #print "sorted= "+str(sorted_array)
    return filter5mgf

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

    f2mgf = None
    f3mgf = None
    f4mgf = None
    f5mgf = None

    skip_flag = False
    f1mgf = filter1(rawmgf.rstrip())  # 3: remove isotopes, removes multiple entries within 0.95 Da of each other
    if len(f1mgf)<=0:
        skip_flag = True
    if skip_flag is False:
        f2mgf = filter2(f1mgf, highestIntensity)  # 6:remove all peaks with a normalized intensity < 1
    if f2mgf:
        f3mgf = filter3(f2mgf)  # 8.1 clean_isotopes removes peaks that are probably C13 isotopes
    if f3mgf:
        f4mgf = filter4(f3mgf) #limit the total number of peaks used
    if f4mgf:
        f5mgf = filter5(f4mgf) #blurring
    if f5mgf:
        fullspectrum = f5mgf+rawmetadata
    else:
        fullspectrum = rawmetadata
    return fullspectrum

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

    print("Consumer is ready to listen!")
    for message in consumer_c2:
        if "__final__" in message.key:
            print "All pairs for input MGF sent!"
            createPairs(None, True)
            sendPairs(None, None, True)
            sys.exit(0)

        if "__init__" not in message.key:
            filteredMGFdata = postProcessMgf(message)
            fullMGFkey = message.key.split("#")
            preTime = dt.now()
            storeMGF(fullMGFkey[0], filteredMGFdata)
            pairsCreated = createPairs(fullMGFkey[0], False)
            postTime = dt.now()
            # Profile block 2
            if profile:
                pr.disable()
                s = cStringIO.StringIO()
                sortby = 'cumulative'
                ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
                ps.print_stats()
                print s.getvalue()
            # End of profile block 2
            sendPairs(pairsCreated, timedelta.total_seconds(postTime-preTime), False)  ### timedelta:  (days, seconds and microseconds)
        else:
            print "Initializing.... send again"

run_step2()

