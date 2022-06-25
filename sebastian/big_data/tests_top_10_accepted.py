import unittest

from top_10_accepted import *


tree = ET.parse("posts.xml")
data = tree.getroot()

class Testing(unittest.TestCase):
	# The function filters the data according to the desired requirements.
	def test_get_tags(self):
		self.assertIsNotNone(get_tags(data[0]))
		self.assertIsNone(get_tags(data[1]))
	# Can accept an empty input.
	def test_wrong_get_tags(self):
		self.assertEqual(get_tags({}), None)

	# The function return does not contain None values.
	def test_mapper(self):
		self.assertNotIn(None, mapper(data))
	# Returns a counter still with an empty input.
	def test_wrong_mapper(self):
		self.assertEqual(mapper({}), Counter())

	# Normal operation of the function.
	def test_reducer_counter(self):
		data_1 = Counter([0, 1])
		data_2 = Counter([1, 2])
		self.assertEqual(reducer_counter(data_1,data_2), 
			Counter({1:2, 0:1, 2:1}))
	# Returns a counter still with an empty input.
	def test_wrong_reducer_counter(self):
		self.assertEqual(reducer_counter({},{}), Counter())

	# The mapreduce must not contain None values 
	# and will return the first 10 values [(str, int)].
	def test_mapreduce_top_10_accepted(self):
		file="posts.xml"
		self.assertIsNotNone(mapreduce_top_10_accepted(file))
		self.assertEqual(len(mapreduce_top_10_accepted(file)),10)
		self.assertIsInstance(mapreduce_top_10_accepted(file)[0][0],str)
		self.assertIsInstance(mapreduce_top_10_accepted(file)[0][1],int)
	# Can accept wrong files.
	def test_wrong_mapreduce_top_10_accepted(self):
		file="test.xml"
		self.assertEqual(mapreduce_top_10_accepted(file),None)


if __name__ == "__main__":
	unittest.main()
