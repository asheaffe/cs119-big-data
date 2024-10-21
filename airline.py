from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="Airline Data")

file_path_june = "data\ontime.td.202406.asc"
file_path_july = "data\ontime.td.202407.asc"

# get data
df = sc.textFile(file_path_june)
print(type(df))     # can I just append the second datafile to the first?

df2 = sc.textFile(file_path_july)

# sanity check
five = df.take(5)
for line in five:
    print(line)