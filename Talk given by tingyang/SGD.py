from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext()
spark = SparkSession(sc)
import numpy as np
from pyspark.mllib.linalg import SparseVector
from pyspark.sql import Row
from scipy.sparse import csr_matrix
from scipy.sparse import vstack
import time, pickle, random

# Convert SparseVector to csr_matrix
def as_matrix(vec):
    if  isinstance(vec, SparseVector):
        data, indices = vec.values, vec.indices
        shape = 1, vec.size
        return csr_matrix((data, indices, np.array([0, vec.values.size])), shape)
    else:
        return csr_matrix(vec)

# Compute gradient for map
def grad(x,y,w):
    return (x.dot(w).toarray()[0][0]-y)*x.T

# fast way to sample from iterable
def iter_sample_fast(iterable, samplesize):
    results = []
    iterator = iter(iterable)
    # Fill in the first samplesize elements:
    for _ in range(samplesize):
        ele = next(iterator,None)
        if ele:
            results.append(ele)
        else:
            return results
    random.shuffle(results)  # Randomize their positions
    for i, v in enumerate(iterator, samplesize):
        r = random.randint(0, i)
        if r < samplesize:
            results[r] = v  # at a decreasing rate, replace random items
    return results

# Compute gradient for mapPartitions
def part_grad(data,w,batch=10):
    sample_data = iter_sample_fast(data,batch)
    n = len(sample_data)
    if n > 0:
        X, y = vstack([fea.features for fea in sample_data]), vstack([fea.label for fea in sample_data])
        yield X.T.dot(X.dot(w)-y)
    else:
        yield csr_matrix(w.T.shape).T

# Load data
data = spark.read.format("libsvm").load("file:///scratch/tix11001/spark/E2006.train").rdd.map(lambda row: Row(features = as_matrix(row.features), label = row.label)).cache()

n = data.count()
m = data.getNumPartitions()
step = 0.01
batch = 1000
tor = 1e-5

# Use map to compute the results
w = csr_matrix(data.first().features.shape).T
time_start = time.time()
for Iter in range(100):
    time_mid = time.time()
    w_old = w.copy()
    w = w - step*data.sample(False,batch*m/n).map(lambda row: grad(row.features,row.label,w)).reduce(lambda a,b: a+b)/batch/m
    err = (w-w_old).power(2).sum()
    print("Iter {0}: {1}, elapse: {2} sec, total elapse: {3} sec.".format(Iter,err,time.time()-time_mid,time.time()-time_start))
    if err < tor:
        break
# Use mapPartitions to compute the results
w = csr_matrix(data.first().features.shape).T
time_start = time.time()
for Iter in range(100):
    time_mid = time.time()
    w_old = w.copy()
    w = w - step*data.mapPartitions(lambda row: part_grad(row,w,batch)).reduce(lambda a,b: a+b)/batch/m
    err = (w-w_old).power(2).sum()
    print("Iter {0}: {1}, elapse: {2} sec, total elapse: {3} sec.".format(Iter,err,time.time()-time_mid,time.time()-time_start))
    if err < tor:
        break
# Compute by Spark LinearRegression
from pyspark.ml.regression import LinearRegression
regression_data = spark.read.format("libsvm").load("file:///scratch/tix11001/spark/E2006.train").cache()
lr = LinearRegression(maxIter=1000, regParam=0, elasticNetParam=0)
# Fit the model
lrModel = lr.fit(regression_data)

		