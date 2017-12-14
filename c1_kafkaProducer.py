from kafka import KafkaProducer
import uuid
import re
import os
from datetime import timedelta, datetime as dt
import time

mgf_location = os.path.join(os.path.dirname(__file__), 'datafiles')
mgfSpectrumIDs = []
fullSpectra_s = ""
p = re.compile('(\d+\.*\d*)[ \t](\d+\.*\d*)')

generatedID = ""
metaData = ""
mz_threshold = 150

producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
producer.send("topic_mgf"
                , value = b'code by apoorva patrikar'
                , key = b'__init__')

for file in os.listdir(mgf_location):
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
                            time.sleep(22)
                            producer.send("topic_mgf"
                                          , value = fullSpectra_s.encode('utf-8')
                                          , key = generatedID.encode('utf-8'))
                            print("Sent spectra: " + generatedID + "at " + str(dt.now()))
                            time.sleep(22)
                        except Exception as e:
                            print("Leider exception in Kafka producer: " + str(e))
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

producer.send("topic_mgf"
            , value = "coded by apoorva".encode('utf-8')
            , key = "__final__".encode('utf-8'))
print("Sent all MGF spectra!")
producer.flush()
#producer.close()

