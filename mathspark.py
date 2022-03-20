import findspark
import pyspark.rdd
from pyspark.sql import SparkSession


def init():
    findspark.init()
    session = SparkSession \
        .builder \
        .appName("test") \
        .master("local") \
        .getOrCreate()
    return session


def mv_mul(matrix_path, vector_path, session=None):
    """
    Calculates y = Ax

    Parameters
    ----------
    matrix_path : str
        Path to matrix A
    vector_path : str
        Path to vector x
    session : SparkSession, optional
    """
    if session is None:
        session = init()


    matrix_rdd = session.read.csv(matrix_path, header=True).rdd
    vector_rdd = session.read.csv(vector_path, header=True).rdd

    rb = vector_rdd.flatMap(lambda x: [((j, int(x[0])), (float(x[1]))) for j in range(100)])
    ra = matrix_rdd.map(lambda x: ((int(x[0]), int(x[1])), (float(x[2]))))

    r = ra + rb
    r = r.reduceByKey(lambda x, y: x * y) \
        .map(lambda x: (x[0][0], x[1])) \
        .reduceByKey(lambda x, y: x + y)
    return r


def selection(path, condition, session=None):
    """
    :param session:
    :param path: путь до файла
    :param condition: условие для выборки
    :return:
    """
    if session is None:
        session = init()
    rdd = session.read.csv(path, header=True).rdd
    rdd = rdd.map(lambda x: (int(x[0]), x[1]))
    rdd = rdd.flatMap(lambda x: [(x[1], x[1])] if condition(x[1]) else [])
    result = rdd.reduceByKey(lambda x, y: x)
    return result


def union(rdd_a: pyspark.rdd.RDD, rdd_b: pyspark.rdd.RDD):
    r = rdd_a + rdd_b
    r = r.map(lambda x: (x[1], x[1]))
    result = r.reduceByKey(lambda x, y: x)
    return result


def intersect(rdd_a, rdd_b):
    r = rdd_a + rdd_b
    r = r.map(lambda x: (x[1], x[1]))
    result = r.groupByKey().flatMap(lambda x: [tuple(x[1])] if len(x[1]) > 1 else [])
    return result


def difference(rdd_a, rdd_b):
    rdd_a = rdd_a.map(lambda x: (x[1], 0))
    rdd_b = rdd_b.map(lambda x: (x[1], 1))
    r = rdd_a + rdd_b
    result = r.groupByKey().flatMap(lambda x: [tuple(x[0])] if sum(x[1]) == 0 else [])
    return result


def join(rdd_a, rdd_b):
    """
    :param rdd_a: RDD, значениями в котором являются пары (a, b)
    :param rdd_b: RDD, значениями в котором являются пары (a, b)
    :return: естественное соединение (a, c) для пар (a, b) и (b, c)

    """

    rdd_a = rdd_a.map(lambda x: (x[1][1], (0, x[1][0])))
    rdd_b = rdd_b.map(lambda x: (x[1][1], (1, x[1][0])))

    r = rdd_a + rdd_b

    result = r.groupByKey().flatMap()


def aggregate(rdd, aggregator):
    # (key, (value1, value2))
    r = rdd.map(lambda x: (int(x[0]), float(x[1][0])))
    return r.reduceByKey(aggregator)
