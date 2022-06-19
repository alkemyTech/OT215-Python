from datetime import datetime
from functools import reduce
import time
import xml.etree.ElementTree as ET


def data_chunks(root, n):
	# Dividing data into groups of n values.
	for i in range(0, len(root), n):
		yield root[i:i + n]

def get_dates(data):
	# Obtaining the creation date of the publications and their responses,
	# as well as their relationship.
	post_id=""
	if data.attrib["PostTypeId"] == "1":
		post_id = data.attrib["Id"]
	elif data.attrib["PostTypeId"] == "2":
		post_id = data.attrib["ParentId"]
	else: return
	creation_date = datetime.strptime(data.attrib["CreationDate"],
		"%Y-%m-%dT%H:%M:%S.%f")
	return {post_id: [creation_date]}

def reducer(dicc1, dicc2):
	# Joining dictionaries with the same key.
	for key, value in dicc2.items():
		if key in dicc1.keys():
			dicc1[key] += value
		else:
			dicc1[key] = value
	return dicc1

def mapper(data_chunk):
	# Getting creation dates.
	data_mapped = list(map(get_dates, data_chunk))
	# Elimination of Nones values.
	data_mapped = list(filter(None, data_mapped))
	# Relating comments to their posts.
	return reduce(reducer, data_mapped)

def average_calculator(dicc):
	# Calculating the average time of each dictionary and the total average of the list.
	for key, value in dicc.items():
		for i in range(len(dicc[key]), 0, -1):
			dicc[key][i-1] = abs((dicc[key][0]-dicc[key][i-1]).total_seconds())
		average = sum(dicc[key])/len(dicc[key])
		dicc[key].pop(0)
		dicc[key] = average
	return time.strftime("Days: %d - Time: %H:%M:%S", 
		time.gmtime(sum(dicc.values())/len(dicc)))


if '__main__' == __name__:
	tree = ET.parse("posts.xml")
	# Data division.
	data_chunk = data_chunks(tree.getroot(), 100)
	# Main mapping function.
	mapped = list(map(mapper, data_chunk))
	# Main reduction function.
	reduced = reduce(reducer, mapped)
	# Obtaining the average time.
	reduced = list(map(average_calculator, [reduced]))
