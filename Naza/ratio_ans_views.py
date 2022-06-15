import xml.etree.ElementTree as ET
from functools import reduce
from operator import concat

tree = ET.parse('posts.xml')
posts = tree.getroot()

QUESTION = '1'
VIEWS = 0
ANSWERS = 1


# n = cantidad de chunks
def mk_chunks(posts, n=1):
    if n < 0:
        return posts

    return [posts[i: i + len(posts) // n]
            for i in range(0, len(posts), len(posts) // n)]


# List[POSTS] -> List[QUESTIONS]
def question_filter(posts):
    post_type = lambda post: post.attrib['PostTypeId'] == QUESTION
    qposts = list(filter(post_type, posts))
    return qposts


# List[List[POSTS]] -> List[List[QUESTIONS]]
def mapper_get_questions(chunks):
    questions = list(map(question_filter, chunks))
    return questions


# QUESTION -> (QUESTION.Views, QUESTION.Answers)
def get_ans_views(qpost):
    try:
        views = int(qpost.attrib['ViewCount'])
        answers = int(qpost.attrib['AnswerCount'])
    except KeyError:
        return 0, 0
    return views, answers


# List[QUESTION]-> List[(QUESTION.Views, QUESTION.Answers)]
def get_ans_views_list(qposts):
    va_list = list(map(get_ans_views, qposts))
    return va_list


# List[List[QUESTION]]-> List[List[(QUESTION.Views, QUESTION.Answers)]]
def mapper_get_ans_views_chunks(chunks):
    va_chunks = list(map(get_ans_views_list, chunks))
    return va_chunks


# List[List[(QUESTION.Views, QUESTION.Answers)]] -> List[(QUESTION.Views, QUESTION.Answers)]
def flat_va_list(chunks):
    return reduce(concat, chunks)


# List[(QUESTION.Views, QUESTION.Answers)] -> (QUESTION.Views, QUESTION.Answers)
# Retorna suma de los primeros elemntos, y suma de los segundos elementos
def reducer(va_list):
    tsum = lambda x, y: (x[0] + y[0], x[1] + y[1])
    return reduce(tsum, va_list)


if __name__ == '__main__':

    chunks = mk_chunks(posts, 100)
    questions = mapper_get_questions(chunks)
    va_chunks = mapper_get_ans_views_chunks(questions)

    va_list = flat_va_list(va_chunks)
    v_a = reducer(va_list)
    ratio_v_a = v_a[VIEWS] / v_a[ANSWERS]

    print(ratio_v_a)
