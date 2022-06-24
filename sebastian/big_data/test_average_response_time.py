import unittest

from average_response_time import *


tree = ET.parse("posts.xml")
data = tree.getroot()

class Testing(unittest.TestCase):
	# Normal operation of the function.
	def test_reducer(self):
		data_1 = {"a": "b"}
		data_2 = {"c": "d"}
		self.assertEqual(reducer(data_1,data_2), 
			{"a": "b", "c": "d"})
	# Returns a dict still with an empty input.
	def test_wrong_reducer(self):
		self.assertEqual(reducer({},{}), {})

	# The function return does not contain None values.
	def test_mapper(self):
		self.assertNotIn(None, mapper(data))

	# The mapreduce must not contain None values and 
	# will return the first 10 values [(str, int)].
	def test_mapreduce_average_response_time(self):
		file="posts.xml"
		self.assertIsNotNone(mapreduce_average_response_time(file))
		self.assertEqual(len(mapreduce_average_response_time(file)),1)
		self.assertIsInstance(mapreduce_average_response_time(file)[0],str)
	# Can accept wrong files.
	def test_wrong_mapreduce_average_response_time(self):
		file="test.xml"
		self.assertEqual(mapreduce_average_response_time(file),None)


if __name__ == "__main__":
	unittest.main()
