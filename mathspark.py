import itertools
import pyspark.rdd


def mul(matrix_rdd, vector_rdd):
    N = vector_rdd.keys().distinct().collect()
    vector_rdd = vector_rdd.flatMap(lambda x: [((j, x[0]), x[1]) for j in N])
    r = vector_rdd + matrix_rdd
    r = r.groupByKey().flatMap(lambda x: [(x[0], list(x[1])[0] * list(x[1])[1])] if len(x[1]) > 1 else []) \
        .map(lambda x: (x[0][0], x[1])) \
        .reduceByKey(lambda x, y: x + y)
    return r


def mv_mul(matrix_rdd: pyspark.rdd.RDD, vector_rdd: pyspark.rdd.RDD):
    """
    Умножение матрицы на вектор
    :param matrix_rdd:
    :param vector_rdd:
    :return:
    """
    count = matrix_rdd.keys().distinct().count()
    print(count)
    rb = vector_rdd.flatMap(lambda x: [((j, x[0]), (x[1])) for j in range(count)])
    ra = matrix_rdd

    r = ra + rb
    r = r.reduceByKey(lambda x, y: x * y) \
        .map(lambda x: (x[0][0], x[1])) \
        .reduceByKey(lambda x, y: x + y)
    return r


def selection(rdd: pyspark.rdd.RDD, condition):
    """
    :param rdd: путь до файла
    :param condition: условие для выборки
    :return:
    """

    rdd = rdd.map(lambda x: (int(x[0]), x[1]))
    rdd = rdd.flatMap(lambda x: [(x[1], x[1])] if condition(x[1]) else [])
    result = rdd.reduceByKey(lambda x, y: x)
    return result


def my_selection(rdd: pyspark.rdd.RDD, condition):
    """
    :param rdd: путь до файла
    :param condition: условие для выборки
    :return:
    """

    rdd = rdd.map(lambda x: (int(x[0]), x[1]))
    rdd = rdd.flatMap(lambda x: [(x[0], x[1])] if condition(x[1]) else [])
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


# tuple([x[0]])
def difference(rdd_a, rdd_b):
    rdd_a = rdd_a.map(lambda x: (x[1], 0))
    rdd_b = rdd_b.map(lambda x: (x[1], 1))
    r = rdd_a + rdd_b
    result = r.groupByKey().flatMap(lambda x: [tuple([x[0]])] if sum(x[1]) == 0 else [])
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
    r = rdd.map(lambda x: (x[1], float(x[2])))
    return r.reduceByKey(lambda x, y: aggregator(x, y))


# НЕ ГОТОВО
"""
M(I, J, V) и N(J, K, W)
Функция Map. Для каждого элемента матрицы mij породить пару
(j, (M, i, mij)). Аналогично для каждого элемента матрицы njk породить пару
(j, (N, k, njk)). Отметим, что M и N в значениях – не сами матрицы, а имена
матриц (как мы уже отмечали, говоря об аналогичной функции Map для
естественного соединения),

Функция Reduce. Для каждого ключа j проанализировать список ассоциированных с ним значений. Для каждого значения, поступившего из
M, например (M, i, mij), и каждого значения, поступившего из N, например
(N, k, njk), породить пару с ключом (i, k) и значением, равным произведению
этих элементов mij njk.
"""


def matmul(rdd_a, rdd_b):
    rdd_a = rdd_a.map(lambda x: (int(x[1]), ('M', int(x[0]), float(x[2]))))
    rdd_b = rdd_b.map(lambda x: (int(x[0]), ('N', int(x[1]), float(x[2]))))

    def combine(items):
        result = []
        M = [item for item in items if item[0] == 'M']
        N = [item for item in items if item[0] == 'N']
        for m in M:
            for n in N:
                result += [((m[1], n[1]), m[2] * n[2])]
        return result

    r = rdd_a + rdd_b
    r = r.groupByKey().flatMap(lambda x: combine(x[1])).reduceByKey(lambda x, y: x + y)
    return r
