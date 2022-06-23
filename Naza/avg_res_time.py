import xml.etree.ElementTree as ET
from functools import reduce
from operator import add
from datetime import datetime
from os.path import exists
import numpy as np
from map_reduce_utils import mk_chunks, get_posts, mapper_post_type


ID = 0
SCORE = 1
CREATION = 2
QUESTION = '1'
ANSWER = '2'


# Extrae ID, SCORE y CREATION de un post
def get_isc(post):
    try:
        id = int(post.attrib['Id'])
        score = int(post.attrib['Score'])
        creation = post.attrib['CreationDate']
        creation = datetime.strptime(creation, '%Y-%m-%dT%H:%M:%S.%f')
        return id, score, creation
    # KeyError -> post valido pero no tiene algun atributo
    # AttributeError -> cuando post es None
    except (ValueError, KeyError, AttributeError):
        return None


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
    if not isc_flat:
        return[]
    sorted_isc = sorted(isc_flat, key=lambda qpost: qpost[SCORE], reverse=True)
    return sorted_isc[:100]


# lista auxiliar con los id del top 100
# List[(I,S,C)] -> List[I]
def get_ids(top_isc):
    if not top_isc:
        return []
    return [qpost[ID] for qpost in top_isc]


# ans_list = List[List[ANS]]
# top_isc = List[(I,S,C)]
# Para cada chunk de respuestas se realiza el filtrado
def mapper_ans_in_top(ans_chunk, top_isc):
    if not ans_chunk or not top_isc:
        return []

    # Dada lista de respuestas, filtra las respuestas de las preguntas top100
    def filter_ans_in_top(ans_list):
        nonlocal top_isc
        top_ids = get_ids(top_isc)
        in_top = lambda ans: int(ans.attrib['ParentId']) in top_ids
        filtered = list(filter(in_top, ans_list))
        return filtered

    top_ans = list(map(filter_ans_in_top, ans_chunk))
    return list(filter(None, top_ans))


# Teniendo una lista de preguntas, y otra de respuestas
# Calculamos el tiempo promedio de respuesta
def get_answer_time(ans_list_flat, top_isc_list):
    if not ans_list_flat or not top_isc_list:
        return np.nan

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


def avg_res_time(xml_path):

    if not exists(xml_path):
        return np.nan

    posts = get_posts(xml_path)
    chunks = mk_chunks(posts, 100)
    questions = mapper_post_type(chunks, QUESTION)
    answers = mapper_post_type(chunks, ANSWER)

    isc_list_per_chunk = mapper_isc(questions)
    isc_flat = reducer(isc_list_per_chunk)
    top = top_score(isc_flat)

    ans_in_top = mapper_ans_in_top(answers, top)
    ans_flat_top = reducer(ans_in_top)
    avg_ans_time = get_answer_time(ans_flat_top, top)

    return avg_ans_time


if __name__ == '__main__':
    print(avg_res_time('posts.xml'))
