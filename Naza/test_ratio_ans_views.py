from map_reduce_utils import mk_chunks, get_posts, answer_filter, question_filter
from ratio_ans_views import *
import unittest
import xml.etree.ElementTree as ET

ptree = ET.parse('posts.xml')
posts = ptree.getroot()

vtree = ET.parse('votes.xml')
votes = vtree.getroot()


class TestRatioAnsViews(unittest.TestCase):

    # No se rompe con path no valido
    def test_invalid_xml_path(self):
        self.assertEqual(get_posts('invalid path'), [])

    # Si el xml no contiene tags retorna vacio
    def test_wrong_xml(self):
        self.assertEqual(get_posts('votes.xml'), [])

    # Checkea si Mapper_get_questions devuelve solo preguntas
    def test_only_questions(self):
        chunks = mk_chunks(posts)
        qchunks = mapper_get_questions(chunks)
        ok = all(all(q.attrib['PostTypeId'] == '1' for q in qlist)
                 for qlist in qchunks)
        self.assertEqual(ok, True)

    # get_ans_views no se rompe con input invalido
    def test_get_ans_invalid_post(self):
        with self.subTest():
            self.assertEqual(get_ans_views(None), (0, 0))

        with self.subTest():
            ans = answer_filter(posts)[0]
            self.assertEqual(get_ans_views(ans), (0, 0))

    # lista de posts, y la lista de atributos
    # de posts deberian tener mismo largo
    def test_res_get_ans_list(self):
        qposts = question_filter(posts)
        self.assertEqual(len(qposts), len(get_ans_views_list(qposts)))

    # get_ans_views_list no se rompe con input invalido
    def test_get_ans_invalid_list(self):
        with self.subTest():
            self.assertEqual(get_ans_views_list(None), [(0, 0)])

        with self.subTest():
            ans = answer_filter(posts)
            self.assertEqual(
                get_ans_views_list(ans), [(0, 0) for _ in range(0, len(ans))])

    # mapper_get_ans_views_list no se rompe con input invalido
    def test_mapper_get_ans_invalid(self):
        with self.subTest():
            self.assertEqual(mapper_get_ans_views_chunks(None), [[(0, 0)]])

        with self.subTest():
            chunks = mk_chunks(posts, 25)
            ans_chunk = list(map(answer_filter, chunks))
            self.assertEqual(
                mapper_get_ans_views_chunks(ans_chunk),
                [[(0, 0) for an in ans] for ans in ans_chunk])

    # lista de posts, y la lista de atributos
    # de posts deberian tener mismo largo
    def test_len_mapper_get_ans_list(self):
        chunks = mk_chunks(posts, 25)
        qchunks = mapper_get_questions(chunks)
        self.assertEqual(len(qchunks), len(mapper_get_ans_views_chunks(qchunks)))

    # Checkea que no se rompe con una lista no valida
    def test_flat_va_invalid_list(self):
        with self.subTest():
            self.assertEqual(flat_va_list(None), [(0, 0)])

        with self.subTest():
            self.assertEqual(flat_va_list([]), [(0, 0)])

    # Logica correcta de reduce
    # dados [(Xi, Yi),...,(Xn, Yn)] se espera
    # (Xi + ... + Xn, Yi + ... + Yn)
    def test_res_reducer(self):
        self.assertEqual(reducer([(1, 2),(1, 2)]), (2, 4))

    # Checkea que no se rompa con una lista no valida
    def test_reducer_invalid_list(self):
        with self.subTest():
            self.assertEqual(reducer(None), (0, 0))

        with self.subTest():
            self.assertEqual(reducer([]), (0, 0))

    # Checkea que la funcion devuelva un solo numero
    def test_valid_return_type_ratio(self):
        res = ratio_ans_views('posts.xml')
        self.assertIsInstance(res, float)

    # El ratio no puede ser negativo
    def test_valid_ratio(self):
        res = ratio_ans_views('posts.xml')
        self.assertGreaterEqual(res, 0)


if __name__ == '__main__':
    unittest.main()
