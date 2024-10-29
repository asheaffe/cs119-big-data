from pyspark import SparkContext
from pyspark.sql import SparkSession
from bloom_filter import BloomFilter
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType

# read in the bad words from file
f = open('afinn.txt', "r")
f = f.readlines()

# init bad words list
bad_words = []
# find words with a -4 or -5 score
for item in f:
    sep = item.strip().split("\t")
    word = sep[0]
    score = int(sep[1])

    if score == -4 or score == -5:
        bad_words.append(word)

# initialize spark
spark = SparkSession.builder.appName("BloomFilterExample").getOrCreate()
sc = spark.sparkContext

# bloom filter params
m = 1000
k = 10

# init bloom filter I made 
bloom_filter = BloomFilter(m, k)

for word in bad_words:
    bloom_filter.add(word)

# broadcast the filter
broadcast = sc.broadcast(bloom_filter)

# method for piping sentences and checking if it is clean
def clean_sentence(sentence):
    blm = broadcast.value

    # returns clean version of sentence
    words = sentence.split()
    return all(word not in blm for word in words)

clean_udf = udf(clean_sentence, BooleanType())

# dataframe that reads from the socket stream
lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

clean_sentences = lines \
	.withColumn("is_clean", clean_udf(col("value"))) \
	.filter(col("is_clean") == True) \
	.select("value")

# write clean sentence to console
query = clean_sentences.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
