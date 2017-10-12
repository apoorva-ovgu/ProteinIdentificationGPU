from cassandra.cluster import Cluster

KEYSPACE = "xtandem"

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

session.execute("""CREATE KEYSPACE %s WITH replication = 
    { 'class': 'SimpleStrategy', 'replication_factor': '2' }""" % KEYSPACE)
session.set_keyspace(KEYSPACE)
print("Keyspace successfully created and set to default.")

session.execute("""
        CREATE TABLE xtandem.protein
        (id UUID PRIMARY KEY, 
         description text, 
         sequence text );
        """)
print("Table xtandem.protein successfully created.")

session.execute("""
        CREATE TABLE xtandem.peptide
        (id UUID PRIMARY KEY,
         protein_uid UUID, 
         sequence text );
        """)
print("Table xtandem.peptide successfully created.")

session.execute("""
    CREATE TABLE xtandem.theo_spectrum
    (id UUID PRIMARY KEY,
     protein_uid UUID, 
     peptide_uid UUID,
     data text );
        """)
print("Table xtandem.theo_spectrum successfully created.")

session.execute("""
        CREATE TABLE xtandem.exp_spectrum
        (id UUID PRIMARY KEY,
         data text );
        """)
print("Table xtandem.exp_spectrum successfully created.")

session.execute("""
        CREATE TABLE xtandem.psm
        (id UUID PRIMARY KEY,
        exp_spectrum_uid UUID,
        theo_spectrum_uid UUID,
        score double);
        """)
print("Table xtandem.psm successfully created.")

cluster.shutdown()