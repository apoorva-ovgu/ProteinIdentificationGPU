from kafka import KafkaProducer
import uuid
import re
import os
import cProfile
import time
#import cStringIO
import pstats
from datetime import timedelta, datetime as dt

#Profile block 1
profile = False
pairbuilders = 4

if profile:
    pr = cProfile.Profile()
    pr.enable()
#End of Profile block 1

mgf_location = os.path.join(os.path.dirname(__file__), 'datafiles')
mgfSpectrumIDs = []
fullSpectra_s = ""
p = re.compile('(\d+\.*\d*)[ \t](\d+\.*\d*)')

generatedID = ""
metaData = ""
mz_threshold = 150
producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
try:
    for i in range(1,pairbuilders+1):
        producer.send("topic_mgf"+str(i)
                    , value = b'code by apoorva patrikar'
                    , key = b'__init__')
except Exception as e:
    print("Exception: " + e.message)

loadBalancer = 0
f = open('output/d4 graph.txt', 'a')
for file in os.listdir(mgf_location):
        counter = 0
        if file.endswith(".mgf"):
            highest_intensity = 0
            for line in open(mgf_location + "/" + file, 'U'):
                line = line.rstrip('\n')
                m = p.match(line)

                if line.lstrip() is not "":
                    if "BEGIN IONS" in line:
                        generatedID = str(uuid.uuid1())

                    elif "END IONS" in line:
                        try:
                            generatedID+="#"+str(highest_intensity)
                            fullSpectra_s+="#"+metaData
                            loadBalancer+=1
                            if (loadBalancer> pairbuilders):
                                loadBalancer = 1
                            producer.send("topic_mgf"+str(loadBalancer)
                                          , value = fullSpectra_s.encode('utf-8')
                                          , key = str(dt.now())+"_the_real_separator_"+generatedID.encode('utf-8'))
                            print("Sent to topic: topic_mgf"+str(loadBalancer))
                            #counter+=1
                            #if counter>4:
                            #    time.sleep(40)
                            #    counter=0
                        except Exception as e:
                            print("Leider exception in Kafka producer: " + str(e))
                        toprint = "["+str(dt.now())+"] Produced spectra: " + generatedID+"\n"
                        print(toprint)
                        f = open('output/d4 results.txt', 'a')
                        f.write(toprint)
                        f.close()

                        fullSpectra_s = ""
                        metaData = ""


                    elif m is not None and "=" not in line:
                        currmz = float(m.group(1))
                        currint = float(m.group(2))

                        if int(currmz) > mz_threshold:
                            fullSpectra_s+=line+"\n"
                            if int(float(m.group(2)))>highest_intensity:
                                highest_intensity = float(m.group(2))

                    elif "=" in line:
                            metaData+=line+"\n"

for i in range(1,pairbuilders+1):
    try:
        producer.send("topic_mgf"+str(i)
                , value = "coded by apoorva".encode('utf-8')
                , key = "__final__".encode('utf-8'))
        print("Sent final message to topic_mgf"+str(i))
    except Exception as e:
        print("Exception: " + e.message)
f.close()
producer.flush()
producer.close()

