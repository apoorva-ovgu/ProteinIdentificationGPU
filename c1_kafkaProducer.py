import uuid
import re
import os
from kafka import KafkaProducer

mgf_location = os.path.join(os.path.dirname(__file__), 'datafiles')
mgfSpectrumIDs = []
fullSpectra_s = ""
p = re.compile('(\d+.\d+)\t(\d+)\t(\d\+)')
generatedID = ""
metaData = ""
mz_threshold = 150

producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
producer.send("UIDSandMGF"
                , value = b'code by apoorva patrikar'
                , key = b'__init__')

for file in os.listdir(mgf_location):
        if file.endswith(".mgx"):
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
                            producer.send("UIDSandMGF"
                                          , value = fullSpectra_s.encode('utf-8')
                                          , key = generatedID)
                        except Exception as e:
                            print("Leider exception in Kafka producer: " + str(e))
                        print("Sent spectra: " + generatedID)
                        fullSpectra_s = ""
                        metaData = ""

                    elif m is not None:
                        if int(float(m.group(1)) > mz_threshold):
                            fullSpectra_s+=line+"\n"
                            if int(float(m.group(2)))>highest_intensity:
                                highest_intensity = int(float(m.group(2)))
                    else:
                        metaData+=line+"\n"
producer.flush()
producer.close()

