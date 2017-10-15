import tensorflow as tf
import os
import re, itertools



lcount = 0
fscore = 0.0
f1= 1.0
f2 = 1.0

v1_1 = []
v1_2 = []

v2_1 = []
v2_2 = []
bothv = ""
vector_location = os.path.join(os.path.dirname(__file__), '../datafiles')


def calc():
    sess = tf.Session()
    finalscore = 0.0

    dot_v1 = []
    dot_v2 = []

    #ind_v1, ind_v2 = [i for i, item in enumerate(v1_1) if item in v2_1], [i for i, item in enumerate(v2_1) if
    #                                                                      item in v1_1]
    #dot_v1, dot_v2 = [item for i, item in enumerate(v1_2) if i in ind_v1], [item for i, item in enumerate(v2_2) if
    #                                                                        i in ind_v2]

    for x in v1_1:
        for y in v2_1:
            if x == y:
                dot_v1.append( v1_2[v1_1.index(x)])
                dot_v2.append(v2_2[v2_1.index(x)])
    print v1_1
    print v2_1
    sess = tf.Session()
    dotProducts = sess.run(tf.multiply(dot_v1, dot_v2))

    lcount = len(dot_v1)
    for dp in dotProducts:
        finalscore += dp
    sess.close()
    return finalscore

def logic1():
    for file in os.listdir(vector_location):
        vector1 = []
        vector2 = []

        if file.endswith(".vector"):
            with open(vector_location + "/" + file, 'r') as content_file:
                bothv = content_file.read().lstrip()
            vectors = re.split(' *v\d+= *', bothv)
            for v in vectors:
                if v.lstrip() is not "":
                    if len(vector1)>0:
                        vector2 = v.lstrip().split('\n')
                    else:
                        vector1 = v.lstrip().split('\n')

            for line in vector1:
                if line.lstrip() is not "":
                    vectorValues = line.split("\t")
                    v1_1.append(float(vectorValues[0]))
                    v1_2.append(float(vectorValues[1]))

            for line in vector2:
                if line.lstrip() is not "":
                    vectorValues = line.split("\t")
                    v2_1.append(float(vectorValues[0]))
                    v2_2.append(float(vectorValues[1]))

            fscore = calc()
            print "Final score of ",str(file)," is",fscore

def logic2():

    allv1 = ""
    allv2 = ""

    vector1_location = os.path.join(os.path.dirname(__file__), '../datafiles/v1.txt')
    vector2_location = os.path.join(os.path.dirname(__file__), '../datafiles/v2.txt')
    with open(vector1_location, 'r') as content_file:
        allv1 = content_file.read().lstrip()
    with open(vector2_location, 'r') as content_file:
        allv2 = content_file.read().lstrip()

    vector1 = re.split("v1= \(\d+\)\n", allv1)
    vector2 = re.split("v2= \(\d+\)\n", allv2)
    vector1_arr = []
    vector2_arr = []

    for v1spectra in vector1:
        if v1spectra.lstrip() is not "":
            vector1_arr.append(re.split("\n", v1spectra))

    for v2spectra in vector2:
        if v2spectra.lstrip() is not "":
            vector2_arr.append(re.split("\n", v2spectra))



    for i in range(1,10):
        del v1_1[:]
        del v1_2[:]
        del v2_1[:]
        del v2_2[:]
        for spectraLine in vector1_arr[i%len(vector1_arr)]:
            if spectraLine.lstrip() is not "":
                splitSpectraLine = re.split("\t", spectraLine)
                v1_1.append( float(splitSpectraLine[0]))
                v1_2 .append( float(splitSpectraLine[1]))

        for spectraLine in vector2_arr[i%len(vector2_arr)]:
            if spectraLine.lstrip() is not "":
                splitSpectraLine = re.split("\t", spectraLine)
                v2_1.append( float(splitSpectraLine[0]))
                v2_2 .append(float( splitSpectraLine[1]))
        try:
            fscore = calc()
        except:
            print "Error occured in iteration ",i,"....Hence skipped"
        print "Final score of iteration ",i," is: ", fscore

def logic3():
    allv1 = ""
    allv2 = ""

    vector1_location = os.path.join(os.path.dirname(__file__), '../datafiles/v1.txt')
    vector2_location = os.path.join(os.path.dirname(__file__), '../datafiles/v2.txt')
    with open(vector1_location, 'r') as content_file:
        allv1 = content_file.read().lstrip()
    with open(vector2_location, 'r') as content_file:
        allv2 = content_file.read().lstrip()

    vector1 = re.split("v1= \(\d+\)\n", allv1)
    vector2 = re.split("v2= \(\d+\)\n", allv2)

    #################processing
    temp = 0
    for pair in itertools.product(vector1,vector2):
        if temp > 10:
            break
        if pair[0] == "" or pair[1] == "":
            pass
        else:
            del v1_1[:]
            del v1_2[:]
            del v2_1[:]
            del v2_2[:]

            temp += 1
            allV1s = pair[0].split("\n")
            allV2s = pair[1].split("\n")

            for x in allV1s:
                y = x.split("\t")
                if y[0] !="":
                    v1_1.append(float(y[0]))
                    v1_2.append(float(y[1]))

            for x in allV2s:
                y = x.split("\t")
                if y[0] != "":
                    v2_1.append(float(y[0]))
                    v2_2.append(float(y[1]))

            try:
                fscore = calc()
                print "Final score of iteration ", temp, " is: ", fscore
            except Exception as e:
                print "Error occured in iteration ", temp, "....Hence skipped\n",e



    #################processing


logic3()