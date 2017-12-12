import uuid
import re
import itertools
import math, sys, time
from datetime import timedelta, datetime as dt
from operator import itemgetter
from kafka import KafkaConsumer
from kafka import KafkaProducer
from dbOperations import connectToDB


flag_compareAll = False
fastaSpectrumIDs = []
mgf_id = "Not Set"
curr_mgf_pepmass = 0.0
curr_mgf_charge = 0
loadBalancer = 0

filter_pepmass = 0.0
filter_intensity = 1.0
filter_maxPeaks = 50

protonwt = 1.00728
neutronwt= 1.00335

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
    filter_pepmass = curr_mgf_pepmass/1000
    global protonwt
    global neutronwt

    producer_uidMatches = KafkaProducer(bootstrap_servers=["localhost: 9092"])
    producer_uidMatches.send("numofmatches"
                  , value=b'code by apoorva patrikar'
                  , key=b'__init__')

    if finalFlag is True:
        producer_uidMatches.send("numofmatches"
                                 , value="coded by apoorva".encode('utf-8')
                                 , key="__final__woo".encode('utf-8'))
        return 0
    else:
        print ("Creating pairs for ",createForId)
        pairs_arr = []
        if flag_compareAll is True:
            for element in itertools.product([createForId], fastaSpectrumIDs):
                pairs_arr.append(element)
        else:
            selectedFastas = []
            m_dmh = round(((curr_mgf_pepmass - protonwt) * curr_mgf_charge) + protonwt,3)
            if curr_mgf_pepmass > 1000:
                m_dmh -= neutronwt
            min_threshold = m_dmh - (m_dmh / 10000) + 1.00769
            max_threshold = m_dmh + (m_dmh / 10000) + 1.00769
            readFromFastaDB(min_threshold, max_threshold )

            lenVal = "len=" + str(len(fastaSpectrumIDs))
            keyVal = "val=" + str(createForId)
            producer_uidMatches.send("numofmatches"
                                     , value=bytes(keyVal, encoding='utf-8')
                                     , key=bytes(lenVal, encoding='utf-8'))
            producer_uidMatches.flush()

            if len(fastaSpectrumIDs)==0:
                print ("No pairs were found...")
                return None
            else:
                for f in fastaSpectrumIDs:
                    if float(f[1]) > min_threshold and float(f[1]) < max_threshold:
                        selectedFastas.append(f[0])
                for element in itertools.product([createForId], selectedFastas):
                    pairs_arr.append(element)
                print("No. of fastas selected = ", len(selectedFastas))
        return pairs_arr

def sendPairs(pairsCreated, time, finalFlag):
    global curr_mgf_pepmass
    global loadBalancer

    producer_c2 = KafkaProducer(bootstrap_servers=['localhost: 9092'])
    if finalFlag is True:
        try:
            producer_c2.send("pairs"+str(loadBalancer)
                             , value="".encode('utf-8')
                             , key="__final__".encode('utf-8'))
        except Exception as e:
            print("Exception in closing Kafka producer c2: " + e.message)
        finally:
            producer_c2.close()
            #sys.exit(0)
    else:
        for couple in pairsCreated:
            #loadBalancer += 1
            if (loadBalancer > 8): #This what we change for scaling.
                loadBalancer = 1
            loadBalancer = 1
            try:
                couple = couple + (loadBalancer,) + (time,)
                packagedCouple = str(couple).encode('utf-8')


                producer_c2.send("pairs"+str(loadBalancer)
                                    ,key = "keyforpair".encode('utf-8')
                                     ,value = packagedCouple)
                producer_c2.flush()

            except Exception as e:
                print("Exception in Kafka producer in pairbuilder: " + e.message)
            finally:
                print ("Paired as ", couple)

def storeMGF(mgfid, mgfContent):
    global curr_mgf_pepmass
    global curr_mgf_charge

    #toPrint = "Received spectrum for ID ", mgfid
    #print(toPrint)

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
            p = re.compile('(\d+.*\d*)[ \t](\d+.*\d*)')
            m = p.match(pepmassAndIntensity)
            curr_mgf_pepmass = float(m.group(1))
        elif "CHARGE" in md:
            charge = md.split("=", 1)[1]
            p = re.compile('(\d+)(\+)')
            m = p.match(charge)
            curr_mgf_charge = float(m.group(1))

    try:
        filldb(uuid.UUID('{' + mgfid + '}'), tmpArr[1], title, curr_mgf_pepmass, charge,tmpArr[0])
    except Exception as e:
        print("Error filling exp_spectrum: " + str(e))

def readFromFastaDB(minval, maxval):
    cass_session = connectToDB()
    select_results = None
    global fastaSpectrumIDs

    #delete this and limit
    query = "SELECT spectrum_id,pep_mass,peptide_sequence FROM fasta.pep_spec" \
            " WHERE pep_mass > "+str(minval)+" and pep_mass < "+str(maxval) +\
            " ALLOW FILTERING ;";
    print("fetching = "+query)
    try:
        select_results = cass_session.execute(query)
    except Exception as e:
        print("Couldn't get queries becase:: "+str(e))
    if select_results is not None:
        for row in select_results:
            stringId = str(eval("row.spectrum_id"))
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

    return filter3_mgf

def filter4(mgf_received):
    #print ("Input mgf ",str(mgf_received))
    filter4_mgf = []
    #ctr = 0
    global filter_maxPeaks
    filter4_mgf_sorted = sorted(mgf_received,key=itemgetter(1), reverse=True)

    ctr = filter_maxPeaks

    for topn in filter4_mgf_sorted:
        if ctr>0:
            filter4_mgf.append(topn)
            ctr -= 1
        else:
            break

        #print ("\nFourth filter applied, size = " ,len(filter4_mgf))
    #print "after f4= " + str(filter4_mgf)

    return  filter4_mgf

def filter5(mgf_received):
    filter5_mgf = []
    f5string = ""

    for peaks in mgf_received:
        currmz = peaks[0]
        currint = peaks[1]
        newmz = math.floor(currmz / 0.4)

        filter5_mgf.append((newmz-1, currint))
        filter5_mgf.append((newmz, currint))
        filter5_mgf.append((newmz + 1, currint))


    sorted_filter5_mgf = sorted(filter5_mgf, key=itemgetter(0))
    for x in sorted_filter5_mgf:
        f5string+=str(x[0])+"\t"+str(x[1])+"\n"

    #print("After f5:: ")
    #for x in sorted_filter5_mgf:
    #    print(x)
    return f5string

def postProcessMgf(message):
    currMgfSpectra = ""
    fullMGFkey = message.key.decode('utf-8').split("#")
    highestIntensity = float(fullMGFkey[1])
    rawmgf = ""
    rawmetadata = ""

    mgf1 = message.value.decode('utf-8').split("\n")
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
        #print("return sorted: "+str(f4mgf))
    if f4mgf:
        f5mgf = filter5(f4mgf) #blurring
    if f5mgf:
        fullspectrum = f5mgf+rawmetadata
    else:
        fullspectrum = rawmetadata
    return fullspectrum

def run_step2():
    consumer_c2 = KafkaConsumer('topic_mgf'
                                ,bootstrap_servers=['localhost:9092']
                                , group_id='apoorva-thesis')
    session = connectToDB()
    session.execute("""TRUNCATE table mgf.exp_spectrum;  """)
    session.execute("""TRUNCATE table scores.psm;  """)
    session.shutdown()

    #print("Going to read from fastadb")
    #readFromFastaDB()

    print("Consumer is ready to listen!")
    for message in consumer_c2:
        if '__final__' in message.key.decode('utf-8'):
            print ("All pairs for input MGF sent!")

            createPairs(None, True)
            sendPairs(None, None, True)
            #sys.exit(0)

        elif '__init__' not in message.key.decode('utf-8'):
            filteredMGFdata = postProcessMgf(message)
            fullMGFkey = message.key.decode('utf-8').split("#")
            preTime = dt.now()
            storeMGF(fullMGFkey[0], filteredMGFdata)
            pairsCreated = createPairs(fullMGFkey[0], False)

            postTime = dt.now()

            if pairsCreated is not None:
                sendPairs(pairsCreated, timedelta.total_seconds(postTime-preTime), False)  ### timedelta:  (days, seconds and microseconds)
        else:
            print ("Initializing.... send again")

run_step2()

