import unittest

from words_score_relation import *


tree = ET.parse("posts.xml")
data = tree.getroot()

class Testing(unittest.TestCase):
	# The mapreduce contains no None values
	# and returns a single value.
	def test_mapreduce_word_score_relation(self):
		file="posts.xml"
		self.assertIsNotNone(mapreduce_word_score_relation(file))
		self.assertEqual(len(mapreduce_word_score_relation(file)),1)
		self.assertIsInstance(mapreduce_word_score_relation(file)[0],float)
	# Can accept wrong files.
	def test_wrong_mapreduce_word_score_relation(self):
		file="test.xml"
		self.assertEqual(mapreduce_word_score_relation(file),None)


if __name__ == "__main__":
	unittest.main()
