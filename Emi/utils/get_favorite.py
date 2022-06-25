from top_10_favorite import get_data, chunkify,mapper_fav
from functools import reduce
from collections import Counter
# total favorite all post
fav_total=[]

def reducer_fav(data1, data2):
    fav_total.append(sum(data1.values()))
    for key, value in data2.items():
        if key in data1.keys():
            data1.update({key: data1[key] + value})
        else:
            data1.update({key: value})
    return data1

def get_favorite():
  # Get results of top 10 elements
  xml_data = get_data()
  chunker_list = chunkify(xml_data, 100)
  mapped = map(mapper_fav, chunker_list)
  mapped = reduce(reducer_fav, mapped)
  top_10 =dict(Counter(mapped).most_common(10))
  top_10_percent = dict(map(lambda x : (x[0], format(x[1]*100/int(fav_total.pop()), '.2f') ), top_10.items()))
  return top_10_percent