import uuid
import json
import os
from kafka import KafkaProducer

mgf_location = os.path.join(os.path.dirname(__file__), 'datafiles')
mgfSpectrumIDs = []

try:
    producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])

    for file in os.listdir(mgf_location):
        if file.endswith(".mgx"):
            ### opening with 'U' translates all conventions of newline to a "\n"
            for line in open(mgf_location + "/" + file, 'U'):
                line = line.rstrip('\n').encode('utf-8')
                if line.lstrip() is not "":

                    ### CREATE UUID for every new spectra
                    if "BEGIN IONS" in line:
                        generatedID = uuid.uuid1()
                        mgfSpectrumIDs.append(generatedID)
                        line += "\tMGFID="+ str(generatedID)
                    producer.send("UIDSandMGF",line)
                    print("sent "+line)

    producer.send("UIDSandMGF", "=all_end")
except Exception as e:
    print("Leider exception in Kafka producer: "+ e.message)