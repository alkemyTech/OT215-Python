from collections import Counter
from functools import reduce
import re
import xml.etree.ElementTree as ET


def data_chunks(root, n):
	for i in range(0, len(root), n):
		yield root[i:i + n]

def get_tags(data):
	if data.get('AcceptedAnswerId') != None:
		return re.findall(r'<(.+?)>', data.attrib['Tags'])

def mapper(data_chunk):
	# obtener tags de cada post
	data_mapped = list(map(get_tags, data_chunk))
	# eliminar Nones
	data_mapped = list(filter(None, data_mapped))
	# aplanar lista
	data_mapped = [item for sublist in data_mapped for item in sublist]
	return Counter(data_mapped)

def reducer_counter(current_count, count_update):
	current_count.update(count_update)
	return current_count


if '__main__' == __name__:
	tree = ET.parse("posts.xml")
	# Division de datos.
	data_chunk = data_chunks(tree.getroot(), 100)
	# llamada a la funcion mapper principal
	mapped = list(map(mapper, data_chunk))
	# llamada a la funcion reduce principal
	mapped = reduce(reducer_counter, mapped)
	# ordenar los valores, obviamente no en diccionarios
	top_accepted = mapped.most_common(10)
