import tensorflow as tf
import time

setA = [(1,0),(2,3),(50,2)]
setB = [(10,0),(20,2),(50,7)]

setC = [(1,2,50,70),(1,13,12,15)]
setD = [(10,20,50,63,70),(0,2,7,1,4)]

sess = tf.Session()
operation = tf.sets.set_intersection(setA,setB)
operation_run = sess.run(operation)
print ("--------------------------\nIntersection of "+str(setA)+" and "+ str(setB)+" is at \n")
print(operation_run)

operation = tf.sets.set_intersection(setC,setD)
operation_run = sess.run(operation)
print ("--------------------------\nIntersection of "+str(setC)+" and "+ str(setD)+" is at \n")
print((operation_run))

dense = tf.sparse_tensor_to_dense(operation_run)
print("\n")
print(sess.run(dense))
