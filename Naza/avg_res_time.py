import xml.etree.ElementTree as ET
from functools import reduce
from operator import add
from datetime import datetime
import numpy as np

tree = ET.parse('posts.xml')
posts = tree.getroot()

ID = 0
SCORE = 1
CREATION = 2
QUESTION = '1'
ANSWER = '2'


# n = cantidad de chunks
def mk_chunks(posts, n=1):
    if n < 0:
        return posts

    return [posts[i: i + len(posts) // n]
            for i in range(0, len(posts), len(posts) // n)]


# Extrae preguntas de una lista de posts
def question_filter(posts):
    post_type = lambda post: post.attrib['PostTypeId'] == QUESTION
    filtered = filter(post_type, posts)
    return list(filtered)


# Extrae respuestas de una lista de posts
def answer_filter(posts):
    post_type = lambda post: post.attrib['PostTypeId'] == ANSWER
    filtered = filter(post_type, posts)
    return list(filtered)


# Funcion mapper para filtrar en chunks
def mapper_post_type(chunks, type):
    filters = {
        '1': question_filter,
        '2': answer_filter
    }
    filtered_posts = map(filters[type], chunks)
    return list(filtered_posts)


# Extrae ID, SCORE y CREATION de un post
def get_isc(post):
    id = int(post.attrib['Id'])
    score = int(post.attrib['Score'])
    creation = post.attrib['CreationDate']
    creation = datetime.strptime(creation, '%Y-%m-%dT%H:%M:%S.%f')
    return id, score, creation


# Dada una lista de posts retorna una lista de los atributos ISC de cada post
# [(I,S,C),...,(I,S,C)]
def isc_from_posts(posts):
    return list(map(get_isc, posts))


# Funcion mapper para obtener los ISC en cada chunk
# Retorna [[(I,S,C)], ... , [(I,S,C)]]
def mapper_isc(chunks):
    return list(map(isc_from_posts, chunks))


# Dada una lista de listas, devuelve una lista "aplanada"
# List[List[X]] -> List[X]
def reducer(list):
    return reduce(add, list)


# Ordena por score y retorna 100 posts con score mas alto
def top_score(isc_flat: list) -> list:
    sorted_isc = sorted(isc_flat, key=lambda qpost: qpost[SCORE], reverse=True)
    return sorted_isc[:100]


# lista auxiliar con los id del top 100
# List[(I,S,C)] -> List[I]
def get_ids(top_isc):
    return [qpost[ID] for qpost in top_isc]


# Dada lista de respuestas, filtra las respuestas de las preguntas top100
# ans_list = List[ANS]
# top_isc = List[(I,S,C)]
def filter_ans_in_top(ans_list):
    global top_isc
    top_ids = get_ids(top_isc)
    in_top = lambda ans: int(ans.attrib['ParentId']) in top_ids
    filtered = list(filter(in_top, ans_list))
    return filtered


# Para cada chunk de respuestas se realiza el filtrado
def mapper_ans_in_top(ans_chunk):
    top_ans = list(map(filter_ans_in_top, ans_chunk))
    return list(filter(None, top_ans))


# Teniendo una lista de preguntas, y otra de respuestas
# Calculamos el tiempo promedio de respuesta
def get_answer_time(ans_list_flat, top_isc_list):
    times = np.array([])
    is_parent = lambda apost: int(apost.attrib['ParentId']) == top_isc_list[i][ID]
    to_date = lambda apost: datetime.strptime(
        apost.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f')

    for i in range(0, len(top_isc_list)):
        answers = list(filter(is_parent, ans_list_flat))
        ans_creation = list(map(to_date, answers))
        if ans_creation:
            ans_times = np.array(ans_creation) - top_isc_list[i][CREATION]
            times = np.append(times, ans_times)

    return np.mean(times)


if __name__ == '__main__':

    chunks = mk_chunks(posts, 100)

    questions = mapper_post_type(chunks, QUESTION)
    isc_list_per_chunk = mapper_isc(questions)
    isc_flat = reducer(isc_list_per_chunk)
    # esta variable se llama en mapper_ans_in_top :(
    top_isc = top_score(isc_flat)

    answers = mapper_post_type(chunks, ANSWER)
    ans_in_top = mapper_ans_in_top(answers)
    ans_flat_top = reducer(ans_in_top)
    avg_ans_time = get_answer_time(ans_flat_top, top_isc)

    print(avg_ans_time)
