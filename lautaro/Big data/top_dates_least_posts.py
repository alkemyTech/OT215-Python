from functools import reduce
import xml.etree.ElementTree as ET
from collections import Counter
from operator import add

tree = ET.parse(r'D:\Alkemy Aceleracion\Stack Overflow\112010 Meta Stack Overflow\posts.xml')
posts = tree.getroot()

def chunkify(iterable, len_of_chunk):
    for i in range(0, len(iterable), len_of_chunk):
        yield iterable[i:i + len_of_chunk]

def mapper(posts):
    cDates = [post.attrib['CreationDate'][0: 10] for post in posts]
    return Counter(cDates)

def reducer(counter_Cdates):
    return reduce(add, counter_Cdates)

if __name__ == '__main__':

    data_chunks = chunkify(posts, 50)
    mapped_dates = list(map(mapper, data_chunks))
    reduced = reducer(mapped_dates)
    top_ten = reduced.most_common()[:-10-1:-1]

print(top_ten)
