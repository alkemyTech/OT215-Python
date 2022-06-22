from post_relation_ratio import get_data, mapped, chunkify, reducer
from functools import reduce

def get_ratio():
  # Get results of first 10 elements
  xml_data = get_data()
  chunker_list = chunkify(xml_data, 32)
  mapper = map(mapped, chunker_list)
  relation_ratio = reduce(reducer, mapper)
  return  relation_ratio[:10]