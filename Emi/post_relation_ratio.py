"""
Relación entre cantidad de palabras 
en un post y su cantidad de respuestas
"""
import xml.etree.ElementTree as ET
from functools import reduce
import re

#configure logs
logging.config.fileConfig(f'log_bigdata.cfg')
logger = logging.getLogger('log big data')

#list especial chars html body in post
CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')

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

def post_body(post):
  '''
  Extract post_id and post_body 
  and Answer_Count 
  Clean post_body and 
  return post_id with post_relation
  '''
  post_id=post.get('Id')
  body=post.get('Body')
  AnswerCount = post.get("AnswerCount")
  # Clean html body post 
  clean_body = re.sub(CLEANR, '', body)

  if AnswerCount != None:
    AnswerCount= int(AnswerCount)
    # Obteined relation ratio words body post
    post_relation= (len(clean_body) / AnswerCount) if AnswerCount else 0
    return [post_id,post_relation]

def mapped(chunker_list):
  '''
  Obteined pairs key and value [post_id,post_relation]
  return pair of list mapped
  '''
  return list(map(post_body, chunker_list))

def reducer(pos_id,post_relation):
  '''
  Reducer values to a unique list of pair
  clean values None type
  '''
  reduce_list =  pos_id + post_relation
  return [x for x in reduce_list if x is not None]

if '__main__' == __name__:
  '''
  Get data of xml file
  divide list in 100 chunks
  Get Relationship between 
  the number of words in a post
  '''  
  xml_data = get_data()
  chunker_list = chunkify(xml_data, 32)
  mapped = map(mapped, chunker_list)
  
  #reduce pair of relationship post words 
  mapped = reduce(reducer, mapped)
  logging.info(f"ratio relationship post words is {mapped}")