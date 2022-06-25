from collections import Counter
from posts_per_date import mapper, get_posts, reducer, get_top_ten
import unittest
import xml.etree.ElementTree as ET
from datetime import datetime


ptree = ET.parse('posts.xml')
posts = ptree.getroot()

vtree = ET.parse('votes.xml')
votes = vtree.getroot()


class TestPostsPerDate(unittest.TestCase):

    # No se rompe con path no valido
    def test_invalid_xml_path(self):
        self.assertEqual(get_posts('invalid path'), [])

    # Si el xml no contiene posts retorna vacio
    def test_wrong_xml(self):
        self.assertEqual(get_posts('votes.xml'), [])

    # Checkea si Mapper devuelve un Counter
    def test_returns_counter(self):
        self.assertIsInstance(mapper(posts), Counter)

    # Checkea si Mapper se rompe al no recibir posts
    def test_no_posts(self):
        self.assertEqual(mapper([]), Counter())

    # Logica correcta de reduce
    def test_working_reducer(self):
        ctest = [Counter([1, 1]), Counter([1, 1])]
        self.assertEqual(reducer(ctest), Counter({1: 4}))

    # Reducer no se rompe con lista vacia
    # Se espera como resultado un contador en cero
    def test_reducer_empty_list(self):
        self.assertEqual(reducer([]), Counter())

    # Checkea que el resultado tenga por lo menos 10 elementos
    def test_top_ten_len(self):
        self.assertLessEqual(len(get_top_ten('posts.xml')), 10)

    # Compara el resultado con el intento de reordenar de mayor a menor,
    # si es igual, es porque el resultado esta bien formado
    def test_valid_top_ten(self):
        res = get_top_ten('posts.xml')
        self.assertEqual(res, sorted(res, key=lambda d: d[1], reverse=True))


if __name__ == '__main__':
    unittest.main()
