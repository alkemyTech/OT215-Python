"""
Top 10 de usuarios con mayor porcentaje
de respuestas favoritas
"""
import xml.etree.ElementTree as ET
from functools import reduce
import re
from collections import Counter
import logging

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

def chunkify(seq, N):
  '''
  Split seq list into N chunks
  '''
  return (seq[i::N] for i in range(N))


def get_user_fav(post):
  '''
  Extract user_id and favorite count post
  '''
  user_id = post.get('OwnerUserId')
  fav_count = post.get('FavoriteCount')
  if fav_count!=None and user_id!=None:
     return [user_id, int(fav_count)] 

def mapper_fav(chunker_list):
  '''
  Obteined pairs key and value [user_id,fav_count]
  convert to list unique value pair
  return counter pair of list mapped
  '''
  mapper_data = list(map(get_user_fav, chunker_list))
  mapper_data = list(filter(None, mapper_data))
  return  dict(mapper_data)

def reducer_fav(data1, data2):
  '''
  Reducer values to sum of fav_count and user_id
  get sum list of total fav_count
  return [data1,count(data2)]
  '''
  fav_total.append(sum(data1.values()))
  for key, value in data2.items():
    if key in data1.keys():
        data1.update({key: data1[key] + value})
    else:
        data1.update({key: value})
  return data1

if '__main__' == __name__:
  '''
  Get data of xml file
  divide list in 100 chunks
  Get top 10 users highest
  percentage of fav answers
  '''
  fav_total=[]
  # Get data of xml file
  xml_data = get_data()
  chunker_list = chunkify(xml_data, 100)
  # Mapped and reduced of data
  mapped = map(mapper_fav, chunker_list)
  #mapped = reduce(reducer_fav, mapped)
  mapped = reduce(reducer_fav, mapped)
  # Get top 10 users
  top_10 =dict(Counter(mapped).most_common(10))
  
  # Get % percentage of favorite answers
  top_10_percent = dict(map(lambda x : (x[0], format(x[1]*100/int(fav_total.pop()), '.2f') ), top_10.items()))
  print(top_10_percent)
  logging.info(f"Top 10 % favorite answers is {top_10_percent}")