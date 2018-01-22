import pyopencl as cl
import numpy as np
from datetime import timedelta, datetime as dt
import os
os.environ['PYOPENCL_COMPILER_OUTPUT'] = '0'
os.environ['PYOPENCL_CTX'] = '0'

time_x = dt.now()

exp_compare = np.array([1,2,3])
exp_compare = exp_compare.astype(np.float32)

exp_score = np.array([3,3,3])
exp_score = exp_score.astype(np.float32)
exp_size = 3

theo_compare = np.array([1,2,3,4, 1,2,3,4, 1,2,3,4, 1,2,3,4, 1,2,3,4, 1,2,3,4, 1,2,3,4, 1,2,3,4, 1,2,3,4, 1,2,3,4,5])
theo_compare = theo_compare.astype(np.float32)

theo_limits = np.array([0,4,8,12,16,20,24,28,32,36,41])

theo_score = np.array([2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2,2])
theo_score = theo_score.astype(np.float32)

theo_size = 41
limits_size= 11

# a = np.random.randn(n, m).astype(np.float32)
# b = np.random.randn(m, p).astype(np.float32)
c = np.zeros(41, dtype=np.float32)

ctx = cl.create_some_context()
queue = cl.CommandQueue(ctx)

mf = cl.mem_flags
exp_comp_buf = cl.Buffer\
   (ctx, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=exp_compare)
exp_score_buf = cl.Buffer\
   (ctx, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=exp_score)

theo_compare_buf = cl.Buffer\
   (ctx, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=theo_compare)
theo_score_buf = cl.Buffer\
   (ctx, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=theo_score)


c_buf = cl.Buffer(ctx, mf.WRITE_ONLY, c.nbytes)

prg = cl.Program(ctx, """
    __kernel void multiply(
    ushort exp_size,
    ushort theo_size, 
    ushort limits_size, 
    __global float *exp_compare,
    __global float *exp_score,
    __global float *theo_compare,  
    __global float *theo_score,  
    __global float *c)
    {
      int gid = get_global_id(0);
      if (gid < theo_size){
       for (int i=0; i<exp_size; i++){
         if (exp_compare[i] == theo_compare[gid]){
           c[gid]= theo_score[gid]*exp_score[i];
           return;
         }
       }
       c[gid] = 0; 
      }
    }
    """).build()

prg.multiply(queue, c.shape, None,
             np.uint16(exp_size), np.uint16(theo_size), np.uint16(limits_size), exp_comp_buf, exp_score_buf, theo_compare_buf, theo_score_buf, c_buf)

result = np.empty_like(c)
cl.enqueue_copy(queue, result, c_buf)

print "result"
print result

print "theo_limits"
print theo_limits

collected_scores = []
curr_tl_pos = 0
curr_c_pos = theo_limits[curr_tl_pos]


aggregator = 0

while (curr_c_pos < len(result)):
    curr_tl_pos+=1
    last_pos = theo_limits[curr_tl_pos]
    while (curr_c_pos<last_pos):
        aggregator += result[curr_c_pos]
        curr_c_pos += 1
    collected_scores.append(aggregator)
    aggregator=0
    curr_c_pos = theo_limits[curr_tl_pos]

print("collected ",collected_scores)
time_y = dt.now()

print ("timewise ",timedelta.total_seconds(time_y - time_x))

a = str(dt.now())
print("...... now string ",a)

b = dt.strptime(a, '%Y-%m-%d %H:%M:%S.%f')
print("...... now datetime ",b)