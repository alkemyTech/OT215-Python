from functools import reduce
import re
import xml.etree.ElementTree as ET


def data_chunks(root, n):
	# Dividing data into groups of n values.
	for i in range(0, len(root), n):
		yield root[i:i + n]

def data_body(data):
	# Obtaining post content and score.
	words = re.findall(
		r"(?<!<\S)[a-zA-Z0-9']+(?!\S)|(?<!\S)[a-zA-Z0-9']+(?=\S)",
		data.attrib['Body'])
	score = int(data.attrib["Score"])
	return score, abs(len(words))

def mapper(data_chunk):
	return list(map(data_body, data_chunk))

def reducer_counter(mapped):
	# Relating word count to score.
	return [[a / b for a, b in data if b] for data in mapped]
	

if '__main__' == __name__:
	tree = ET.parse("posts.xml")
	# Data division.
	data_chunk = data_chunks(tree.getroot(), 100)
	# Main mapping function.
	mapped = list(map(mapper, data_chunk))
	# Main reduction function.
	reduced = reducer_counter(mapped)
	relation_w_s = reduce(reducer_counter, reduced[0:1])
	