# from typing import List, Dict
import math

with db.session() as session:
        query = '''
            LOAD CSV WITH HEADERS FROM "file:///home/pavel/repos/master-degree/app/tmp/nodes_movies.csv" AS row
            CREATE (:Movie {
                title: row.title,
                Format: row.Format,
                ReleaseYear: toInteger(row.ReleaseYear),
                MPAA_Rating: row.MPAA_Rating,
                Price: toFloat(row.Price)
            })
        '''
        session.run(query)

# MATCH (n) DETACH DELETE n

def similar(
    left_classifiers: list[int],
    left_features: list[float],
    right_classifiers: list[int],
    right_features: list[float]
) -> float:
    # Проверка размеров входных данных
    left_size = len(left_classifiers)
    right_size = len(right_classifiers)
    
    if left_size == 0 or right_size == 0:
        return 0.0
    
    if left_size != len(left_features):
        raise ValueError(f"Длина left_classifiers ({left_size}) должна быть равна длине left_features ({len(left_features)})")
        
    if right_size != len(right_features):
        raise ValueError(f"Длина right_classifiers ({right_size}) должна быть равна длине right_features ({len(right_features)})")

    # Создание объединения классификаторов
    union_classifier_set = set(left_classifiers)
    union_classifier_set.update(right_classifiers)
    union_classifiers = list(union_classifier_set)

    # Получение векторов признаков
    left_vector = get_features_vector(left_classifiers, left_features, union_classifiers)
    right_vector = get_features_vector(right_classifiers, right_features, union_classifiers)

    # Вычисление косинусного сходства
    result = cosine(left_vector, right_vector)
    return 0.0 if math.isnan(result) else result


def cosine(vector_a: list[float], vector_b: list[float]) -> float:
    """Вычисляет косинусное сходство между двумя векторами."""
    dot_product = sum(a * b for a, b in zip(vector_a, vector_b))
    norm_a = math.sqrt(sum(x * x for x in vector_a))
    norm_b = math.sqrt(sum(x * x for x in vector_b))
    
    return dot_product / (norm_a * norm_b)

def get_features_vector(
    classifiers: list[int],
    classifier_features: list[float],
    union_classifiers: list[int]
) -> list[float]:
    """Создает вектор признаков на основе классификаторов и их значений."""
    classifier_to_features = dict(zip(classifiers, classifier_features))
    return [
        classifier_to_features.get(classifier, 0.0)
        for classifier in union_classifiers
]