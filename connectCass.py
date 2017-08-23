from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('dna')

#session.execute("CREATE TABLE dna.sample2 ( id UUID PRIMARY KEY, dna_width INT, grooves text, gc_content int );")
select_results = session.execute_async("SELECT * FROM sample1")
print("ID\t\t\t\t\t\t\t\t\t\t\t\t\tExtraction\t\t\t\t\t\t\t\t\t\t\t\t\tType")
try:
    rows = select_results.result()
except Exception:
    print("Leider exception in querying: "+ Exception)

for row in rows:
    print(row)


print("success")
cluster.shutdown()