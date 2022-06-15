import xml.etree.ElementTree as ET
from collections import Counter
from functools import reduce
from operator import add

tree = ET.parse('posts.xml')
posts = tree.getroot()


# n = cantidad de chunks
def mk_chunks(posts, n=1):
    if n < 0:
        return posts

    return [posts[i: i + len(posts) // n]
            for i in range(0, len(posts), len(posts) // n)]


def mapper(posts):
    dates = [post.attrib['CreationDate'] for post in posts]
    return Counter(dates)


def reducer(count_lst):
    return reduce(add, count_lst)


if __name__ == '__main__':

    chunks = mk_chunks(posts, 100)
    mapped_chunks = list(map(mapper, chunks))
    reduced = reducer(mapped_chunks)
    top_ten = reduced.most_common(10)
