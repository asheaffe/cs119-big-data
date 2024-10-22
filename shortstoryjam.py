import requests
import re
import string
import os
stopwords_list = requests.get("https://gist.githubusercontent.com/rg089/35e00abf8941d72d419224cfd5b5925d/raw/12d899b70156fd0041fa9778d657330b024b959c/stopwords.txt").content
stopwords = list(set(stopwords_list.decode().splitlines()))

# poe's story data
poe_src = "data/poe-stories"

content = ''
for filename in os.listdir(poe_src):
    file_path = os.path.join(poe_src, filename)

    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            content += " " + file.read()

def remove_stopwords(words):
    list_ = re.sub(r"[^a-zA-Z0-9]", " ", words.lower()).split()
    return [itm for itm in list_ if itm not in stopwords]


def clean_text(text):
    text = text.lower()
    text = re.sub('\[.*?\]', '', text)
    text = re.sub('[%s]' % re.escape(string.punctuation), ' ', text)
    text = re.sub('[\d\n]', ' ', text)
    return ' '.join(remove_stopwords(text))

no_stopwords = remove_stopwords(content)


content = clean_text(content)

print(content)
