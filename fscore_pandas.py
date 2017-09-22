import tensorflow as tf
import pandas as pd

lcount = 0
fscore = 0.0

#comparing-value is index, dotprod value is value
v1 = {1 : 0, 2:3, 5:2, 20:1, 22:7, 30:5}
v2 = {10:0, 20:2, 30:3}


matched = pd.Index(v1).intersection(pd.Index(v2))

for m in matched:
    print(m," ===> ",v1[m], " and ",v2[m], " matched!")
    lcount+=1
    fscore += (v1[m]*v2[m])
print("Lcount is ",lcount," and Fscore is ",fscore)

#s1 = {'a1' : 1.5, 'a' : 1.5, 'b' : 12.5}
#s2 = {'a' : 0.5, 'b' : 1.56, 'w' : 2.5}
#tf1 = tf.feed_dict = s1
#tf2 = tf.feed_dict = s2
#print(tf1['b'])

### OUTPUT:
### 20  ===>  1  and  2  matched!
### 30  ===>  5  and  3  matched!
### Lcount is  2  and Fscore is  17.0
