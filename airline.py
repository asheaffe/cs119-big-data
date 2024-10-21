from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.streaming import StreamingContext

import pandas as pd
import re
import sys, os
dir_path = os.path.realpath(__file__)

spark = SparkSession.builder.getOrCreate()

# get data
file1 = pd.read_csv(os.path.dirname(dir_path) + '/data/ontime.td.202406.asc', delimiter="|")
file2 = pd.read_csv(os.path.dirname(dir_path) + '/data/ontime.td.202407.asc', delimiter="|")

# data from both June and July 2024
all_data = pd.concat([file1, file2], ignore_index=True)
print(all_data.shape)
# Define the schema
schema = StructType([
    StructField("carrier", StringType(), True),
    StructField("flightnum", StringType(), True),
    StructField("depart_airport", StringType(), True),
    StructField("arrival_airport", StringType(), True),
    StructField("date_flight", StringType(), True),
    StructField("DOW_flight", StringType(), True),
    StructField("OAG_depart_time", StringType(), True),
    StructField("CRS_depart_time", StringType(), True),
    StructField("gate_depart_time", StringType(), True),
    StructField("OAG_arrival_time", StringType(), True),
    StructField("CRS_arrival_time", StringType(), True),
    StructField("gate_arrival", StringType(), True),
    StructField("OAG_diff_gh", StringType(), True),
    StructField("OAG_diff_jk", StringType(), True),
    StructField("elapsed_time", StringType(), True),
    StructField("gate_to_gate", StringType(), True),
    StructField("depart_delay", StringType(), True),
    StructField("arrival_delay", StringType(), True),
    StructField("elapse_time_diff", StringType(), True),
    StructField("wheels_off", StringType(), True),
    StructField("wheels_on", StringType(), True),
    StructField("tailnum", StringType(), True),
    StructField("cancel_code", StringType(), True),
    StructField("min_late_E", StringType(), True),
    StructField("min_late_F", StringType(), True),
    StructField("min_late_G", StringType(), True),
    StructField("min_late_H", StringType(), True),
    StructField("min_late_I", StringType(), True)
])

df = spark.createDataFrame(all_data, schema=schema)

df = df.withColumn("depart_delay", df["depart_delay"].cast("int"))

# airline_delays = df.groupBy("carrier").agg(F.sum("depart_delay").alias("total_delay"))
# airline_delays.show()

df.select("carrier", "depart_delay").show()
# def get_delays(df):
#     # returns the airline with the least delays (full names)
#     delays = df.groupBy()