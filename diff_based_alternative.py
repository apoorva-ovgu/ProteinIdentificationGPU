import tensorflow as tf
sess=tf.Session()
v1_compVal = [1, 2, 3, 4, 5, 6]
v1_dotVal=[1, 1, 1, 1, 1, 1]

v2_compVal = [1, 3, 5, 7]
v2_dotVal=[10, 30, 50, 70]

v1_matchingPos =sess.run( #The matching indexes in v1 are
    tf.setdiff1d( #the difference between 
        tf.range(tf.size(v1_compVal)), #a list of values from 0 to v1.size
        #and...
        tf.setdiff1d( 
            v1_compVal, 
            v2_compVal, 
            index_dtype=tf.int32, name=None).idx #the indexes of values in v1 not in v2
            , 
        index_dtype=tf.int32, name=None))

#Same logic for v2        
v2_matchingPos = sess.run(tf.setdiff1d(tf.range(tf.size(v2_compVal)), tf.setdiff1d(v2_compVal, v1_compVal, index_dtype=tf.int32, name=None).idx, index_dtype=tf.int32, name=None))

lcount= sess.run(tf.size(v2_matchingPos.out))
fscore = sess.run(tf.reduce_sum(tf.multiply(tf.gather(v1_dotVal, v1_matchingPos.idx), tf.gather(v2_dotVal, v2_matchingPos.idx))))

print("Lcount is ", lcount," and Fscore is ", fscore)


### OUTPUT:
### Lcount is  3  and Fscore is  90

