from functools import reduce
import xml.etree.ElementTree as ET
from collections import Counter
from operator import add
import re

tree = ET.parse(r'D:\Alkemy Aceleracion\Stack Overflow\112010 Meta Stack Overflow\posts.xml')
root = tree.getroot()

def chunkify(iterable, len_of_chunk):
    for i in range(0, len(iterable), len_of_chunk):
        yield iterable[i:i + len_of_chunk]

def extract_words(data):
    body = data.attrib['Body']    
    body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))', body)
    counter_words = Counter(body)
    return counter_words
def mapeado(data):
    palabras_mapeadas= list(map(extract_words, data))
    reducido = reduce(add, palabras_mapeadas)
    return reducido

if __name__ == '__main__':

    data_chunks = chunkify(root, 50)
    mapped = list(map(mapeado, data_chunks))
    reduced = reduce(add, mapped)
    top_ten = reduced.most_common(10)

print(top_ten)