from cassandra.cluster import Cluster
import os
import uuid

protein_id = None
protein_desc = None
protein_seq = None
fullprotein = None
fasta_location = None
separated_proteins = []

header = None
sequence = None


def filldb(protein_desc, protein_seq):
    ### uuid1: Generate a UUID from a host ID, sequence number, and the current time. ###
    session.execute(
        """INSERT INTO xtandem.protein (id, description, sequence)
        VALUES (%s, %s, %s)""",
        (uuid.uuid1(), protein_desc, protein_seq)
    )

def table_contents(table_name):
    query = "SELECT * FROM "+table_name
    select_results = session.execute_async(query)
    try:
        rows = select_results.result()
    except Exception as e:
        print("Leider exception in querying: " + e.message)
    for row in rows:
        print(row)

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('xtandem')
### initial cleanup ###
#session.execute("""TRUNCATE table xtandem.protein; """ )

fasta_location = os.path.join(os.path.dirname(__file__), 'datafiles')
for file in os.listdir(fasta_location):
    if file.endswith(".fasta"):
        for line in open(fasta_location + "/" + file, 'U'):
            line = line.rstrip('\n')
            if len(line) > 0 and line[0] == '>':
                if sequence is not None:
                    separated_proteins.append([header, sequence])
                    filldb(header, sequence)
                    curr_protein = []
                sequence = ""
                header = line
            else:
                sequence += line

table_contents("xtandem.protein")

print("successful cassandra connection.\n")
cluster.shutdown()