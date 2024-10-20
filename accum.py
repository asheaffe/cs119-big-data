from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('appName').setMaster('local')
sc = SparkContext(conf=conf)

data = [1,2,3,4,5]
counter = 0
rdd = sc.parallelize(data)

def print_ (x):
  print (x)
  return x

rdd.foreach(print_)
# 4
# 5
# 3
# 2
# 1

def accum(x):                                                               
    counter += x

rdd.foreach(accum)

# Error messages ensue
# 24/10/18 12:47:09 ERROR Executor: Exception in task 4.0 in stage 3.0 (TID 28)
# org.apache.spark.api.python.PythonException: Traceback (most recent call last):
#  File "/usr/sup/Python-3.9.2/lib/python3.9/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 619, in main
#    process()
#  File "/usr/sup/Python-3.9.2/lib/python3.9/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 609, in process