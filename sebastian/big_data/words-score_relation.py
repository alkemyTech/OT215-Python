from functools import reduce
import re
import xml.etree.ElementTree as ET


def data_chunks(root, n):
	for i in range(0, len(root), n):
		yield root[i:i + n]

def data_body(data):
	words = re.findall(
		r"(?<!<\S)[a-zA-Z0-9']+(?!\S)|(?<!\S)[a-zA-Z0-9']+(?=\S)",
		data.attrib['Body'])
	score = int(data.attrib['Score'])
	return score, abs(len(words))

def mapper(data_chunk):
	return list(map(data_body, data_chunk))

def reducer_counter(mapped):
	return [[a / b for a, b in data if b] for data in mapped]
	

if '__main__' == __name__:
	tree = ET.parse("posts.xml")
	# Division de datos.
	data_chunk = data_chunks(tree.getroot(), 100)
	# llamada a la funcion mapper principal
	mapped = list(map(mapper, data_chunk))
	# aplanar lista
	reduced = reducer_counter(mapped)
	reduced = reduce(reducer_counter, reduced[0:1])
	print(reduced[0:1])