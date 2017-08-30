import tensorflow as tf
from pandas import *


sess = tf.Session()

arr = [[11, 22, 33],
       [44, 55, 66]]
arr2 = sess.run(tf.constant([7, 8, 9, 10, 11, 12], shape=[3, 2]))

print("Matrix= ")
print(DataFrame(arr))

#trace: sum of diagonals
print("\nTrace= ")
resTrace= sess.run(tf.trace(arr))
print(resTrace)

#transpose
resTranspose = sess.run(tf.transpose(arr))
print("\nTranspose= ")
print(DataFrame(resTranspose))

#diagonal
print("\nDiagonal = ")
resDiag = sess.run(tf.diag([11,22,33]))
print(DataFrame(resDiag))

#identity matrix
print("\nIdentity 2x3= ")
resI = sess.run(tf.eye(2, num_columns=3))
print(DataFrame(resI))

#multiplication matrix
print("\nMatrix1= ")
print(DataFrame(arr))
print("Matrix2= ")
print(DataFrame(arr2))
print("arr1 x arr2= ")
resMul = sess.run(tf.matmul(arr, arr2))
print(DataFrame(resMul))

