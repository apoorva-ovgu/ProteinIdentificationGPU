from cassandra.cluster import Cluster



cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

KEYSPACE = "fasta"
try:
    session.execute("""CREATE KEYSPACE %s WITH replication = 
        { 'class': 'SimpleStrategy', 'replication_factor': 1 }""" % KEYSPACE)
    session.set_keyspace(KEYSPACE)
except Exception as e:
    print("Error creating keyspace: "+str(e))
finally:
    print("Keyspace successfully created and set to default.")


# try:
#     #protein
#     session.execute("""
#            CREATE TABLE fasta.prot_table(
#            id uuid PRIMARY KEY,
#            protein_description text,
#            protein_sequence text,
#            protein_accession text,
#            );
#             """)
# except Exception as e:
#     print("Error creating protein: " + str(e))
# finally:
#     print("Table fasta.prot_table successfully created.")

# try:
#     #peptide
#     session.execute("""
#             CREATE TABLE fasta.prot_pep(
#            id uuid PRIMARY KEY,
#            protein_id uuid,
#            peptide_id uuid
#            );
#             """)
#
# except Exception as e:
#     print("Error creating peptide: " + str(e))
# finally:
#     print("Table fasta.prot_pep (peptide) successfully created.")

try:
    #theo_spectrum... Comment on mzvalues: --comma seperated double values, the intensities are 1.0 because theoretical spectrum
     session.execute("""
          CREATE TABLE fasta.pep_spec(
              peptide_id uuid,
              spectrum_id uuid,
              peptide_sequence text,
              spectrum_charge int,
              pep_mass double,
              pep_mz double,
              mz_values text, 
              PRIMARY KEY (spectrum_id, pep_mass)
           )
           WITH CLUSTERING ORDER BY (pep_mass DESC) 
            """)

except Exception as e:
    print("Error creating theo_spectrum: " + str(e))
finally:
    print("Table fasta.pep_spec (theo_spectrum) successfully created.")

KEYSPACE = "mgf"
try:
    session.execute("""CREATE KEYSPACE %s WITH replication = 
        { 'class': 'SimpleStrategy', 'replication_factor': '1' }""" % KEYSPACE)
    session.set_keyspace(KEYSPACE)
except Exception as e:
    print("Error creating keyspace: "+str(e))
finally:
    print("Keyspace successfully created and set to default.")

try:
    session.execute("""
            CREATE TABLE mgf.exp_spectrum
            (id UUID PRIMARY KEY,
            allmeta text, 
            title text, 
            pepmass float,
            charge text, 
            data text );
            """)

except Exception as e:
    print("Error creating exp_spectrum: " + str(e))
finally:
    print("Table mgf.exp_spectrum successfully created.")

KEYSPACE = "scores"
try:
    session.execute("""CREATE KEYSPACE %s WITH replication = 
        { 'class': 'SimpleStrategy', 'replication_factor': '1' }""" % KEYSPACE)
    session.set_keyspace(KEYSPACE)
except Exception as e:
    print("Error creating keyspace: "+str(e))
finally:
    print("Keyspace successfully created and set to default.")
try:
    session.execute("""
            CREATE TABLE scores.psm
            (id UUID PRIMARY KEY,
            exp_spectrum_uid UUID,
            theo_spectrum_uid UUID,
            score double);
            """)
except Exception as e:
    print("Error creating psm: " + str(e))
finally:
    print("Table scores.psm successfully created.")
cluster.shutdown()
