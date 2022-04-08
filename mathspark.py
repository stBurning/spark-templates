import itertools

import pyspark.rdd
import util


def mv_mul(matrix_rdd, vector_rdd):
    """

    :param matrix_rdd:
    :param vector_rdd:
    :return:
    """

    rb = vector_rdd.flatMap(lambda x: [((j, int(x[0])), (float(x[1])))
                                       for j in range(matrix_rdd.keys()
                                                      .distinct()
                                                      .count())])
    ra = matrix_rdd.map(lambda x: ((x[0], int(x[1][0])), (float(x[1][0]))))

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
        session = util.init()
    rdd = session.read.csv(path, header=True).rdd
    rdd = rdd.map(lambda x: (int(x[0]), x[1]))
    rdd = rdd.flatMap(lambda x: [(x[1], x[1])] if condition(x[1]) else [])
    result = rdd.reduceByKey(lambda x, y: x)
    return result


def projection(rdd: pyspark.rdd.RDD, ix: list):
    rdd = rdd.map(lambda x: (tuple([x[i] for i in ix]), tuple([x[i] for i in ix])))
    return rdd.reduceByKey(lambda x, y: x)


def union(rdd_a: pyspark.rdd.RDD, rdd_b: pyspark.rdd.RDD):
    r = rdd_a + rdd_b
    r = r.map(lambda x: (x[1], x[1]))
    result = r.reduceByKey(lambda x, y: x)
    return result


def intersect(rdd_a, rdd_b):
    rdd_a = rdd_a.map(lambda x: (x[1], "A"))
    rdd_b = rdd_b.map(lambda x: (x[1], 'B'))
    r = (rdd_a + rdd_b)
    result = r.groupByKey().flatMap(lambda x: [(x[0], set(tuple(x[1])))])
    result = result.flatMap(lambda x: [x[0]] if len(x[1]) > 1 else [])
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
    def combine(l: list):
        a = []
        b = []
        for item in l:
            if item[0] == 0:
                a += [item[1]]
            else:
                b += [item[1]]
        return list(itertools.product(a, b))

    rdd_a = rdd_a.map(lambda x: (x[1][1], (0, x[1][0])))
    rdd_b = rdd_b.map(lambda x: (x[1][0], (1, x[1][1])))

    r = rdd_a + rdd_b
    result = r.groupByKey().flatMap(lambda x: [(x[0], item) for item in combine(list(x[1]))])
    return result


def aggregate(rdd, aggregator):
    # (key, (value1, value2))
    r = rdd.map(lambda x: (x[2], float(x[1])))
    return r.reduceByKey(lambda x, y: aggregator(x, y))

# # НЕ ГОТОВО
# def matmul(rdd_a, rdd_b):
#     rdd_a = rdd_a.map(lambda x: (x[1], ('M', x[0], float(x[2]))))
#     rdd_b = rdd_b.map(lambda x: (x[0], ('N', x[1], float(x[2]))))
#     r = rdd_a + rdd_b
#     r = r.reduceByKey()
