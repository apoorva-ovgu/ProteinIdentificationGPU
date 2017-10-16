from cassandra.cluster import Cluster

KEYSPACE = "xtandem"

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

try:
    session.execute("""CREATE KEYSPACE %s WITH replication = 
        { 'class': 'SimpleStrategy', 'replication_factor': '2' }""" % KEYSPACE)
    session.set_keyspace(KEYSPACE)
except Exception as e:
    print("Error creating keyspace: "+str(e))
finally:
    print("Keyspace successfully created and set to default.")

try:
    session.execute("""
            CREATE TABLE xtandem.protein
            (id UUID PRIMARY KEY, 
             description text, 
             sequence text );
            """)

except Exception as e:
    print("Error creating protein: " + str(e))
finally:
    print("Table xtandem.protein successfully created.")

try:
    session.execute("""
            CREATE TABLE xtandem.peptide
            (id UUID PRIMARY KEY,
             protein_uid UUID, 
             sequence text );
            """)

except Exception as e:
    print("Error creating peptide: " + str(e))
finally:
    print("Table xtandem.peptide successfully created.")

try:
    session.execute("""
        CREATE TABLE xtandem.theo_spectrum
        (id UUID PRIMARY KEY,
         protein_uid UUID, 
         peptide_uid UUID,
         data text );
            """)

except Exception as e:
    print("Error creating theo_spectrum: " + str(e))
finally:
    print("Table xtandem.theo_spectrum successfully created.")

try:
    session.execute("""
            CREATE TABLE xtandem.exp_spectrum
            (id UUID PRIMARY KEY,
            metadata text,
             data text );
            """)

except Exception as e:
    print("Error creating exp_spectrum: " + str(e))
finally:
    print("Table xtandem.exp_spectrum successfully created.")

try:
    session.execute("""
            CREATE TABLE xtandem.psm
            (id UUID PRIMARY KEY,
            exp_spectrum_uid UUID,
            theo_spectrum_uid UUID,
            score double);
            """)
except Exception as e:
    print("Error creating psm: " + str(e))
finally:
    print("Table xtandem.psm successfully created.")
cluster.shutdown()