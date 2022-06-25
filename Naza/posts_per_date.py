import xml.etree.ElementTree as ET
from collections import Counter
from functools import reduce
from operator import add
from map_reduce_utils import mk_chunks, get_posts


def mapper(posts):
    dates = [post.attrib['CreationDate'] for post in posts]
    return Counter(dates)


def reducer(count_lst):
    if not count_lst:
        return Counter()

    return reduce(add, count_lst)


def get_top_ten(xml_path):
    posts = get_posts(xml_path)
    chunks = mk_chunks(posts, 25)
    mapped_chunks = list(map(mapper, chunks))  # Mapper toma lista de posts
    reduced = reducer(mapped_chunks)
    return reduced.most_common(10)


if __name__ == '__main__':

    print(get_top_ten('posts.xml'))
