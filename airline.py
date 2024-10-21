from pyspark import SparkSession

spark = SparkSession.builder.appName("Airline Data Processing").getOrCreate()

file_path_june = "data\ontime.td.202406.asc"
file_path_july = "data\ontime.td.202407.asc"

# get data
df = spark.read.csv(file_path_june, header=True, inferSchema=True)

# sanity check
df.show(5)