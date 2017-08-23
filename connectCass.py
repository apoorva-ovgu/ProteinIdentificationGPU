from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('dna')

#session.execute("CREATE TABLE dna.sample2 ( id UUID PRIMARY KEY, dna_width INT, grooves text, gc_content int );")
future = session.execute_async("SELECT * FROM sample1")
print("key\tcol1\tcol2")
print("---\t----\t----")
try:
    rows = future.result()
except Exception:
    print("Leider exception in querying: "+ Exception)

for row in rows:
    print(row)


print("success")
cluster.shutdown()