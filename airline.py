from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.streaming import StreamingContext

import pandas as pd
import re
import sys, os
dir_path = os.path.realpath(__file__)

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")

# get data
file1 = pd.read_csv(os.path.dirname(dir_path) + '/data/ontime.td.202406.asc', delimiter="|")
file2 = pd.read_csv(os.path.dirname(dir_path) + '/data/ontime.td.202407.asc', delimiter="|")

# data from both June and July 2024
all_data = pd.concat([file1, file2], ignore_index=True)
# remove 4 columns between B and C
all_data = all_data.drop(all_data.columns[[2, 3, 4, 5]], axis=1)

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

airline_dict = {
    'AA': 'American Airlines',
    'DL': 'Delta',
    'UA': 'United Airlines',
    'B6': 'JetBlue Airways',
    'AS': 'Alaska Airlines',
    'F9': 'Frontier Airlines',
    'NK': 'Spirit Airlines',
    'HA': 'Hawaiian Airlines',
    'G4': 'Allegiant Air',
    'WN': 'Southwest Airlines'
}

airport_dict = {
    'ATL': 'Atlanta - Hartsfield Jackson',
    'BWI': "Baltimore/Wash. Int'l Thurgood.Marshall",
    'BOS': 'Boston - Logan International',
    'CLT': 'Charlotte - Douglas',
    'MDW': 'Chicago - Midway',
    'ORD': "Chicago - O'Hare",
    'CVG': 'Cincinnati Greater Cincinnati',
    'DFW': 'Dallas-Fort Worth International',
    'DEN': 'Denver - International',
    'DTW': 'Detroit - Metro Wayne County',
    'FLL': 'Fort Lauderdale Hollywood International',
    'IAH': 'Houston - George Bush International',
    'LAS': 'Las Vegas - McCarran International',
    'LAX': 'Los Angeles International',
    'MIA': 'Miami International',
    'MSP': 'Minneapolis-St. Paul International',
    'EWR': 'Newark Liberty International',
    'JFK': 'New York - JFK International',
    'LGA': 'New York - LaGuardia',
    'MCO': 'Oakland International',
    'OAK': 'Orlando International',
    'PHL': 'Philadelphia International',
    'PHX': 'Phoenix - Sky Harbor International',
    'PDX': 'Portland International',
    'SLC': 'Salt Lake City International',
    'STL': 'St. Louis Lambert International',
    'SAN': 'San Diego Intl. Lindbergh Field',
    'SFO': 'San Francisco International',
    'SEA': 'Seattle-Tacoma International',
    'TPA': 'Tampa International',
    'DCA': 'Washington - Reagan National',
    'IAD': 'Washington - Dulles International'
}

df = spark.createDataFrame(all_data, schema=schema)
df = df.replace(airline_dict, subset=["carrier"])

# convert depart delay to integer
df = df.withColumn("depart_delay", df["depart_delay"].cast("int"))

# airline_delays = df.groupBy("carrier").agg(F.mean("depart_delay").alias("depart delay"))
# airline_delays = airline_delays.orderBy(col("depart delay").asc())
# airline_delays.show()

# # add zeros at the beginning to ensure 4 digits
# df = df.withColumn(
#     "CRS_depart_time_fixed", 
#     F.lpad(F.col("CRS_depart_time"), 4, '0')
# )

# # Convert to a timestamp datatype
# df = df.withColumn(
#     "CRS_depart_timestamp", 
#     F.to_timestamp(F.col("CRS_depart_time_fixed"), "HHmm")
# )

# # convert CRS departure time to int
# df = df.withColumn("CRS_depart_time", df["CRS_depart_time"].cast("int"))
# df = df.withColumn("hour", F.hour(col("CRS_depart_timestamp")))

# # Create time_block column to organize mean delay by time of day
# df = df.withColumn("time_block",
#                    F.when((F.col("hour") >= 22) | (F.col("hour") < 6), "10pm-6am")
#                     .when((F.col("hour") >= 6) & (F.col("hour") < 10), "6am-10am")
#                     .when((F.col("hour") >= 10) & (F.col("hour") < 14), "10am-2pm")
#                     .when((F.col("hour") >= 14) & (F.col("hour") < 18), "2pm-7pm")
#                     .otherwise("7pm-10pm"))
# depart_delays = df.groupBy("time_block").agg(F.mean("depart_delay").alias("mean_delay"))
# depart_delays.show()

airport_codes = list(airport_dict.keys())

df = df.filter(col("depart_airport").isin(airport_codes))

df = df.replace(airport_dict, subset=["depart_airport"])
df = df.replace(airport_dict, subset=["arrival_airport"])

# df = df.withColumn("total_delay", df["depart_delay"] + df["arrival_delay"])
# airport_delays = df.groupBy("depart_airport").agg(F.mean("total_delay").alias("delay"))
# airport_delays = airport_delays.orderBy(col("delay").asc())
# airport_delays.show()

arrival_df = df.alias("arrival_df")
depart_df = df.alias("depart_df")

# Perform the join
df_joined = arrival_df.join(depart_df, arrival_df["arrival_airport"] == depart_df["depart_airport"], "inner").alias("airport")

# Show the result
df_joined.show()

airport_count = df.groupBy("depart_airport").agg(F.count("depart_airport").alias("count"))
airport_count = airport_count.orderBy(col("count").desc())
airport_count.show(5)