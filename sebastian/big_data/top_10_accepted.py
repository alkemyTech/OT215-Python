from collections import Counter
from functools import reduce
import re
import xml.etree.ElementTree as ET


def data_chunks(root, n):
	# Dividing data into groups of n values.
	for i in range(0, len(root), n):
		yield root[i:i + n]

def get_tags(data):
	# Getting tags from accepted answers.
	if data.get("AcceptedAnswerId") != None:
		return re.findall(r"<(.+?)>", data.attrib["Tags"])

def mapper(data_chunk):
	# Obtaining tags of each post,
	data_mapped = list(map(get_tags, data_chunk))
	# Elimination of Nones values.
	data_mapped = list(filter(None, data_mapped))
	# List flattening.
	data_mapped = [item for sublist in data_mapped for item in sublist]
	return Counter(data_mapped)

def reducer_counter(current_count, count_update):
	# Data reduction.
	current_count.update(count_update)
	return current_count


if '__main__' == __name__:
	tree = ET.parse("posts.xml")
	# Data division.
	data_chunk = data_chunks(tree.getroot(), 100)
	# Main mapping function.
	mapped = list(map(mapper, data_chunk))
	# Main reduction function.
	reduced = reduce(reducer_counter, mapped)
	# Getting top 10.
	top_accepted = reduced.most_common(10)
