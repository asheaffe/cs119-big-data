import requests
import re
import string
import os
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.data import load

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from pyspark.streaming import StreamingContext
nltk.download('punkt_tab')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('averaged_perceptron_tagger_eng')
nltk.download('tagsets')
stopwords_list = requests.get("https://gist.githubusercontent.com/rg089/35e00abf8941d72d419224cfd5b5925d/raw/12d899b70156fd0041fa9778d657330b024b959c/stopwords.txt").content
stopwords = list(set(stopwords_list.decode().splitlines()))

# poe's story data
poe_src = "data/poe-stories"

content = {}
for filename in os.listdir(poe_src):
    file_path = os.path.join(poe_src, filename)

    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            content[filename] = file.read()

def remove_stopwords(words):
    list_ = re.sub(r"[^a-zA-Z0-9]", " ", words.lower()).split()
    return [itm for itm in list_ if itm not in stopwords]


def clean_text(text):
    text = text.lower()
    text = re.sub('\[.*?\]', '', text)
    text = re.sub('[%s]' % re.escape(string.punctuation), ' ', text)
    text = re.sub('[\d\n]', ' ', text)
    return ' '.join(remove_stopwords(text))


no_stopwords = remove_stopwords(content['A_DESCENT_INTO_THE_MAELSTROM'])

content['A_DESCENT_INTO_THE_MAELSTROM'] = clean_text(content['A_DESCENT_INTO_THE_MAELSTROM'])


## Q2: NLTK


# getting the text from the first story (A_DESCENT_INTO...)
#story_path = "data/poe-stories/A_DESCENT_INTO_THE_MAELSTROM"

# with open(story_path, 'r') as file:
#             paragraph = file.read()

# method for extracting parts of speech from a particular story
def parts_of_speech(story, content):
    paragraph = clean_text(content[story])

    sent_text = nltk.sent_tokenize(paragraph) # this gives us a list of sentences
    # now loop over each sentence and tokenize it separately
    all_tagged = [nltk.pos_tag(nltk.word_tokenize(sent)) for sent in sent_text]

    tagdict = load('help/tagsets/upenn_tagset.pickle')

    taglist = list(tagdict.keys())

    parts_o_speech = {}
    # go through all tagged elements in story
    for tagged in all_tagged[0]:
        key = tagged[1]
        if key in taglist:
            if key in parts_o_speech:
                parts_o_speech[key].append(tagged[0])
            else:
                parts_o_speech[key] = [tagged[0]]

    return parts_o_speech

# create a list of dictionaries for each story
pos_dict = []

temp = {}
# add each story and data to dictionary
for story in content:
    pos = parts_of_speech(story, content)

    temp['text'] = content[story]
    temp['Noun'] = []
    temp['Verb'] = []
    temp['Adjective'] = []
    temp['Adverb'] = []

    for key, value in pos.items():  
        if key.startswith('N'): 
            for item in value: 
                temp['Noun'].append(item)
        elif key.startswith('VB'):  
            for item in value: 
                temp['Verb'].append(item)
        elif key.startswith('JJ'):  
            for item in value: 
                temp['Adjective'].append(item)
        elif key.startswith('RB'):  
            for item in value: 
                temp['Adverb'].append(item)

    pos_dict.append(temp)
    temp = {}

# create a dataframe for parts of speech
spark = SparkSession.builder.getOrCreate()

# set schema
schema = StructType([
    StructField("text", StringType(), True),
    StructField("Noun", ArrayType(StringType()), True),
    StructField("Verb", ArrayType(StringType()), True),
    StructField("Adjective", ArrayType(StringType()), True),
    StructField("Adverb", ArrayType(StringType()), True)
])

df = spark.createDataFrame(pos_dict, schema=schema)


df = df.withColumn("Noun_Count", F.size(df['Noun']))
df = df.withColumn("Verb_Count", F.size(df['Verb']))
df = df.withColumn("Adjective_Count", F.size(df['Adjective']))
df = df.withColumn("Adverb_Count", F.size(df['Adverb']))
df = df.select("Noun_Count", 'Verb_Count', "Adjective_Count", "Adverb_Count")
df.show()