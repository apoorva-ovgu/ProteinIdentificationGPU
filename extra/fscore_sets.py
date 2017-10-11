import tensorflow as tf
sess=tf.Session()
setC = [(1,2,50,70),(11,13,12,15)]
setD = [(10,20,50,63,70),(0,2,7,1,4)]


ooperation = tf.sets.set_intersection(setC,setD)
operation_run = sess.run(operation)
print ("Intersection of "+str(setC)+" and "+ str(setD)+" is at ",operation_run.values)

for m in operation_run.values:
    lcount+=1
    #cannot calculate fscore without index

print("Lcount is ",lcount," and Fscore is ",fscore)

#dense = tf.sparse_tensor_to_dense(operation_run)
#print("\n")
#print(sess.run(dense))

### OUTPUT:
### Intersection of [(1, 2, 50, 70), (11, 13, 12, 15)] and [(10, 20, 50, 63, 70), (0, 2, 7, 1, 4)] is at 

### SparseTensorValue(indices=array([[0, 0],[0, 1]], dtype=int64), 
###                     values=array([50, 70]), 
###                     dense_shape=array([2, 2], dtype=int64))


### Lcount is  2  and Fscore is  0.0
