from cassandra.cluster import Cluster

def connectToDB():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    #session.set_keyspace('xtandem')
    return session

def viewTable(toselect, table_name):
    cass_session = connectToDB()
    tempArray = []

    query = "SELECT " + toselect + " FROM " + table_name + ";"
    select_results = cass_session.execute(query)

    if "*" not in toselect:
        for row in select_results:
            stringRes = str(eval("row." + toselect))
            tempArray.append(stringRes)
    else:
        for row in select_results:
            tempArray.append(row)
    cass_session.shutdown()

    for entry in tempArray:
        print str(entry)

def truncTable(table_name):
    cass_session = connectToDB()
    query = "TRUNCATE TABLE " + table_name
    try:
        cass_session.execute(query)
    except Exception as e:
        print "Table "+table_name+" not truncated.....!\n"+str(e)
    finally:
        print "Table " + table_name + " truncated successfully."

def dropTable(table_name):
    cass_session = connectToDB()
    query = "DROP TABLE " + table_name
    try:
        cass_session.execute(query)
    except Exception as e:
        print "Table " + table_name + " not dropped.....!\n" + str(e)
    finally:
        print "Table " + table_name + " dropped successfully."

#truncTable('xtandem.exp_spectrum')
#dropTable('xtandem.exp_spectrum')

#print "\nTable protein:::\n"
#viewTable('*','xtandem.protein')

#print "\nTable peptide:::\n"
#viewTable('*','xtandem.peptide')

#print "\nTable theo_spectrum:::\n"
#viewTable('*','xtandem.theo_spectrum')

#print "\nTable exp_spectrum:::\n"
#viewTable('*','xtandem.exp_spectrum')

#print "\nTable scores:::\n"
#viewTable('*','xtandem.psm')

#dropTable("mgf.exp_spectrum");