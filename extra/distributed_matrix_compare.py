import tensorflow as tf
import grpc
import datetime


arr = [[11, 22, 33],
       [44, 55, 66],
       [77, 88, 99]]
trace = 0
arrtranspose = None
arr2 = None

server = tf.train.Server.create_local_server()
node1 = str(server.target,'utf-8')


print("- - - - - - - - - - - - - - - - - - - - - - - - SINGLE- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")
t1 = datetime.datetime.now()
xs = tf.Session()
arrtranspose = xs.run(tf.matmul(arr, tf.transpose(arr)))
trace = xs.run(tf.trace(arr))
arrts = xs.run(tf.add(arrtranspose, trace))
arr2 = xs.run(tf.matmul(arrtranspose, arrts))


print(xs.run(tf.add(arr,arr2)))

t2 = datetime.datetime.now()
print ("single node computation time: " + str(t2-t1))
print("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")


print("- - - - - - - - - - - - - - - - - - - - - - - - - - - MULTI- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")


t1 = datetime.datetime.now()

with tf.device('/job:local/task:0/cpu:0'):
  arrtranspose = tf.matmul(arr, tf.transpose(arr))

with tf.device("/job:local/task:0/cpu:0"):
    trace = tf.trace(arr)
    arrts = tf.add(arrtranspose, trace)
    arr2 = tf.matmul(arrtranspose, arrts)

with tf.Session(node1) as sess:
    print(sess.run(arr+arr2))
    sess.close()

t2 = datetime.datetime.now()
print ("Multi node computation time: " + str(t2-t1))


