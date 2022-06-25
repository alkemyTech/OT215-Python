import xml.etree.ElementTree as ET
from functools import reduce
from operator import concat
from map_reduce_utils import mk_chunks, get_posts, question_filter

QUESTION = '1'
VIEWS = 0
ANSWERS = 1


# List[List[POSTS]] -> List[List[QUESTIONS]]
def mapper_get_questions(chunks):
    questions = list(map(question_filter, chunks))
    return questions


# QUESTION -> (QUESTION.Views, QUESTION.Answers)
def get_ans_views(qpost):
    try:
        views = int(qpost.attrib['ViewCount'])
        answers = int(qpost.attrib['AnswerCount'])
    except (KeyError, AttributeError):
        return 0, 0
    return views, answers


# List[QUESTION]-> List[(QUESTION.Views, QUESTION.Answers)]
def get_ans_views_list(qposts):
    if qposts:
        va_list = list(map(get_ans_views, qposts))
        return va_list
    else:
        return [(0, 0)]


# List[List[QUESTION]]-> List[List[(QUESTION.Views, QUESTION.Answers)]]
def mapper_get_ans_views_chunks(chunks):
    if chunks:
        va_chunks = list(map(get_ans_views_list, chunks))
        return va_chunks
    else:
        return [[(0, 0)]]


# List[List[(QUESTION.Views, QUESTION.Answers)]] -> List[(QUESTION.Views, QUESTION.Answers)]
def flat_va_list(chunks):
    if chunks:
        return reduce(concat, chunks)
    else:
        return [(0, 0)]


# List[(QUESTION.Views, QUESTION.Answers)] -> (QUESTION.Views, QUESTION.Answers)
# Retorna suma de los primeros elemntos, y suma de los segundos elementos
def reducer(va_list):
    if not va_list:
        return 0, 0

    tsum = lambda x, y: (x[0] + y[0], x[1] + y[1])
    return reduce(tsum, va_list)


def ratio_ans_views(xml_path):
    posts = get_posts(xml_path)
    chunks = mk_chunks(posts, 25)
    questions = mapper_get_questions(chunks)
    va_chunks = mapper_get_ans_views_chunks(questions)

    va_list = flat_va_list(va_chunks)
    v_a = reducer(va_list)

    try:
        ratio_v_a = v_a[VIEWS] / v_a[ANSWERS]
    except (ZeroDivisionError, TypeError):
        return 0

    return ratio_v_a


if __name__ == '__main__':
    print(ratio_ans_views('posts.xml'))
