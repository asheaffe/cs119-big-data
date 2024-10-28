from pyspark import SparkContext
from pyspark.sql import SparkSession
from bloom_filter import BloomFilter

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