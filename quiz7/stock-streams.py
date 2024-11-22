from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# create the SparkSession
spark = SparkSession.builder.appName("PriceAnalysis").getOrCreate()

# https://www.waitingforcode.com/apache-spark-structured-streaming/what-new-apache-spark-3.4.0-structured-streaming-correctness-issue/read
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

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
    .option("port", 10000) \
    .load() \
    .select(split(col("value"), "\t").alias("cols"))

parsedDF = lines.select(
    col("cols").getItem(0).alias("symbol"),
    to_timestamp(col("cols").getItem(1), "yyyy-MM-dd HH:mm:ss").alias("datetime"),
    col("cols").getItem(2).cast(DoubleType()).alias("open"),
    col("cols").getItem(3).cast(DoubleType()).alias("high"),
    col("cols").getItem(4).cast(DoubleType()).alias("low"),
    col("cols").getItem(5).cast(DoubleType()).alias("close"),
    col("cols").getItem(6).cast(DoubleType()).alias("volume")
)
    
# separate streams for AAPL and MSFT
aaplPrice = parsedDF.filter(col("symbol") == "AAPL")
msftPrice = parsedDF.filter(col("symbol") == "MSFT")

# add watermark to aaplPrice and msftPrice for 10 Day and 40 Day
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withWatermark.html
aaplPrice = aaplPrice.withWatermark("datetime", "10 minutes")
msftPrice = msftPrice.withWatermark("datetime", "10 minutes")

# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.streaming.DStream.window.html
window_global = window("datetime", "10 minutes")

# 10 day and 40 day cumulative close numbers
aapl10Day = aaplPrice.groupBy(window_global,
                               "symbol"
                               ).agg(sum("close").alias("aapl10Day"))

msft10Day = msftPrice.groupBy(window_global,
                              "symbol"
                              ).agg(sum("close").alias("msft10Day"))

aapl40Day = aaplPrice.groupBy(window_global,
                               "symbol"
                               ).agg(sum("close").alias("aapl40Day"))

msft40Day = msftPrice.groupBy(window_global,
                              "symbol"
                              ).agg(sum("close").alias("msft40Day"))

aapl10Day_ma = aapl10Day.groupBy("window", "symbol").agg(
                                avg(1000 / col("aapl10Day")).alias("aapl10Day_MA"))

msft10Day_ma = msft10Day.groupBy("window", "symbol").agg(
                                avg(1000 / col("msft10Day")).alias("msft10Day_MA"))

aapl40Day_ma = aapl40Day.groupBy("window", "symbol").agg(
                                avg(1000 / col("aapl40Day")).alias("aapl40Day_MA"))

msft40Day_ma = msft40Day.groupBy("window", "symbol").agg(
                                avg(1000 / col("msft40Day")).alias("msft40Day_MA"))

# # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html
# result_10Day = aapl10Day_ma.join(msft10Day_ma, on=["window", "symbol"]) \
#     .select(
#         expr("window.start").alias("datetime"),
#         "symbol",
#         "aapl10Day_MA",
#         "msft10Day_MA"
#     )

# result_40Day = aapl40Day_ma.join(msft40Day_ma, on=["window", "symbol"]) \
#     .select(
#         expr("window.start").alias("datetime"),
#         "symbol",
#         "aapl40Day_MA",
#         "msft40Day_MA"
#     )

# full_result = result_10Day.join(result_40Day, on=["symbol", "datetime"])

# # comparison columns
# full_result = full_result.withColumn(
#     "symbol",
#     when(col("aapl10Day_MA") > col("aapl40Day_MA"), "Sell") \
#     .when(col("msft10Day_MA") > col("msft40Day_MA"), "Sell") \
#     .when(col("aapl10Day_MA") < col("aapl40Day_MA"), "Buy") \
#     .when(col("msft10Day_MA") < col("msft40Day_MA"), "Buy")
# )


# compare 10 day and 40 day avg's for aapl
aapl_comparison = aapl10Day_ma.join(aapl40Day_ma, ["symbol", "window"])

# comparison column for aapl
aapl_filtered = aapl_comparison.withColumn(
    "comparison",
    when(col("aapl10Day_MA") > col("aapl40Day_MA"), "10MA_Above")
    .when(col("aapl10Day_MA") < col("aapl40Day_MA"), "10MA_Below")
    .otherwise("Equal")
)

# compare 10 day and 40 day avg's for msft
msft_comparison = msft40Day_ma.join(msft10Day_ma, ["symbol", "window"])

# comparison column for msft
msft_filtered = msft_comparison.withColumn(
    "comparison",
    when(col("msft10Day_MA") > col("msft40Day_MA"), "10MA_Above")
    .when(col("msft10Day_MA") < col("msft40Day_MA"), "10MA_Below")
    .otherwise("Equal")
)

# combine the results
full_result = aapl_filtered.union(msft_filtered)

formatted_result = full_result.select(
    concat(
        lit("("),
        col("window.start").cast("string"),
        lit(" "),
        when(col("comparison") == "10MA_Above", "sell")
        .when(col("comparison") == "10MA_below", "buy")
        .otherwise("hold"),
        lit(" "),
        col("symbol"),
        lit(")")
    ).alias("formatted_output")
)

query = formatted_result \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()