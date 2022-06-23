from map_reduce_utils import mk_chunks, get_posts, answer_filter, question_filter
from avg_res_time import *
import unittest
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta


ptree = ET.parse('posts.xml')
posts = ptree.getroot()

vtree = ET.parse('votes.xml')
votes = vtree.getroot()


class TestAvgResTime(unittest.TestCase):

    # No se rompe con path no valido
    def test_invalid_xml_path(self):
        self.assertEqual(get_posts('invalid path'), [])

    # Si el xml no contiene tags retorna vacio
    def test_wrong_xml(self):
        self.assertEqual(get_posts('votes.xml'), [])

    # Resultado de la funcion tenga la forma correcta (int, int, datetime)
    def test_res_type_get_isc(self):
        post = posts[0]
        with self.subTest():
            self.assertIsInstance(get_isc(post)[0], int)
        with self.subTest():
            self.assertIsInstance(get_isc(post)[1], int)
        with self.subTest():
            self.assertIsInstance(get_isc(post)[2], datetime)

    # get_isc no se rompe con post invalido
    def test_get_isc_invalid_post(self):
        with self.subTest():
            self.assertEqual(get_isc(None), None)

        with self.subTest():
            wrong_post = votes[0]
            self.assertEqual(get_isc(wrong_post), None)

    # lista de posts y lista de isc's deben tener el mismo largo
    def test_len_isc_from_posts(self):
        self.assertEqual(len(isc_from_posts(posts)), len(posts))

    # El tiempo promedio de respuesta no puede ser negativo
    def test_avg_ans_time_res(self):
        res = avg_res_time('posts.xml')
        self.assertGreaterEqual(res, timedelta(0))


if __name__ == '__main__':
    unittest.main()
