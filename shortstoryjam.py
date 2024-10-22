import requests
import re
import string
import os
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
nltk.download('punkt_tab')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('averaged_perceptron_tagger_eng')
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

paragraph = clean_text(content['A_DESCENT_INTO_THE_MAELSTROM'])

sent_text = nltk.sent_tokenize(paragraph) # this gives us a list of sentences
# now loop over each sentence and tokenize it separately
all_tagged = [nltk.pos_tag(nltk.word_tokenize(sent)) for sent in sent_text]

print(all_tagged)