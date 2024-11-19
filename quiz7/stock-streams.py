from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# create the SparkSession
spark = SparkSession.builder.appName("PriceAnalysis").getOrCreate()

# write the schema
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

# create dataframe 
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# parsing incoming data
data = lines.select(
    split(col("value"), "\s+").getItem(0).alias(("symbol"),
            to_timestamp(split(col("value"), "\s+").getItem(1)).alias("datetime"),
            split(col("value"), "\s+").getItem(2).cast("double").alias("open"),
            split(col("value"), "\s+").getItem(2).cast("double").alias("high"),
            split(col("value"), "\s+").getItem(2).cast("double").alias("low"),
            split(col("value"), "\s+").getItem(2).cast("double").alias("close"),
            split(col("value"), "\s+").getItem(2).cast("double").alias("volume")
            )
)  

# separate streams for AAPL and MSFT
aaplPrice = data.filter(col("symbol") == "AAPL")
msftPrice = data.filter(col("symbol") == "MSFT")

# write data to the console
write_aapl = aaplPrice \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

write_msft = msftPrice \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()