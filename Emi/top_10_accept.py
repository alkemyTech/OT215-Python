"""
Top 10 tipo de post 
con mayor respuestas aceptadas
"""
import xml.etree.ElementTree as ET
from functools import reduce
import re
from collections import Counter

#configure logs
logging.config.fileConfig(f'log_bigdata.cfg')
logger = logging.getLogger('log big data')

def get_data():
  '''
  Read and parse XML 
  '''
  xml_data = open('posts.xml', 'r').read()  
  paser_root = ET.XML(xml_data)  
  return paser_root

def chunkify(seq, size):
  '''
  Split seq list into N chunks
  '''
  return (seq[i::size] for i in range(size))

def get_post_accept(post):
  '''
  Extract Accepted Answer Id and type of post
  clean characters type of post
  '''
  post_accept = post.get('AcceptedAnswerId')
  post_tags = post.get('Tags')
  if post_accept != None:
      return re.findall(r'<([^>]+)>',post_tags)

def mapper(chunker_list):
  '''
  Obteined pairs key
  and value [type_post,type_post_count]
  convert to list unique value pair
  return counter pair of list mapped
  '''
  mapper_data = list(map(get_post_accept, chunker_list))
  mapper_data = list([x for x in mapper_data if x is not None])
  mapper_data = [item for subl in mapper_data for item in subl]
  return Counter(mapper_data)

def reducer_counter(type_post, type_count):
  '''
  Reducer values to sum 
  of fav_count and user_id
  return [type_post,count(type_pos)]
  '''
  type_post.update(type_count)
  return type_post

if '__main__' == __name__:
   '''
  Get data of xml file
  divide list in 100 chunks
  Get Top 10 most 
  accepted post type
  '''
  xml_data = get_data()
  chunker_list = chunkify(xml_data, 32)
  mapped = list(map(mapper, chunker_list))
  mapped = reduce(reducer_counter, mapped)

  # Get Top 10 most post type
  top_10_aceept =mapped.most_common(10)
  logging.info(f"Top 10 most post type is {top_10_aceept}")
