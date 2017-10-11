import tensorflow as tf

sess = tf.Session()
lcount = 0
fscore = 0.0
f1= 1.0
f2 = 1.0

v1_1 = [1,2,5,20,27,30]
v1_2 = [0,3,2,1,7,5]

v2_1 = [10,20,30]
v2_2 = [0,2,3]

ind_v1, ind_v2 = [i for i, item in enumerate(v1_1) if item in v2_1],[i for i, item in enumerate(v2_1) if item in v1_1]

for m in ind_v1:
    f1*=m
    lcount+=1
for m in ind_v2:
    f2 *= m
print("Lcount is ",lcount," and Fscore is ",(f1+f2))

### OUTPUT:
### Lcount is  2  and Fscore is  17.0
