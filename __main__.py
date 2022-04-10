import findspark
from pyspark.sql import SparkSession
import mathspark as ms

if __name__ == '__main__':
    findspark.init()

    session = SparkSession \
        .builder \
        .appName("test") \
        .master("local") \
        .getOrCreate()
    session.sparkContext.setLogLevel('ERROR')

    # vector_rdd = session.read.csv('data/vector.csv', header=True).rdd
    # vector_rdd = vector_rdd.map(lambda x: (int(x[0]), float(x[1])))
    # matrix_rdd = session.read.csv('data/matrix.csv', header=True).rdd
    # matrix_rdd = matrix_rdd.map(lambda x: ((int(x[0]), int(x[1])), float(x[2])))
    # result = ms.mv_mul(matrix_rdd, vector_rdd)
    # print(result.collect())

    # rdd = session.read.csv('data/selection.csv', header=True).rdd
    # result = ms.selection(rdd, condition=lambda x: x == 'A' or x == 'B')
    # print(result.collect())

    # # union
    # rdd_a = session.read.csv('data/a.csv', header=True).rdd
    # rdd_b = session.read.csv('data/b.csv', header=True).rdd
    # rdd_a = rdd_a.map(lambda x: (int(x[0]), x[1]))
    # rdd_b = rdd_b.map(lambda x: (int(x[0]), x[1]))
    # print(ms.union(rdd_a, rdd_b).take(20))

    # #intersection
    # rdd_a = session.read.csv('data/a.csv', header=True).rdd
    # rdd_b = session.read.csv('data/b.csv', header=True).rdd
    # print(ms.intersect(rdd_a, rdd_b).take(20))

    # # difference
    # rdd_a = session.read.csv('data/a.csv', header=True).rdd
    # rdd_b = session.read.csv('data/b.csv', header=True).rdd
    # print(ms.difference(rdd_a, rdd_b).take(20))

    # join
    # rdd_a = session.read.csv('data/А.csv', header=True).rdd
    # rdd_b = session.read.csv('data/Б.csv', header=True).rdd
    # rdd_a = rdd_a.map(lambda x: (int(x[0]), (x[1], x[2])))
    # rdd_b = rdd_b.map(lambda x: (int(x[0]), (x[1], x[2])))
    # print(ms.join(rdd_a, rdd_b).take(20))

    # # aggregation
    # rdd = session.read.csv('data/aggr.csv', header=True).rdd
    # print(ms.aggregate(rdd, lambda x, y: x + y).take(20))

    # # projection
    # rdd = session.read.csv('data/foo.csv', header=True).rdd
    # print(ms.projection(rdd, [2, 3]).take(20))

    # matmul
    rdd_a = session.read.csv('data/matrix_a.csv', header=True).rdd
    rdd_b = session.read.csv('data/matrix_b.csv', header=True).rdd
    # rdd_a = rdd_a.map(lambda x: ((int(x[0]), int(x[1])), float(x[2])))
    # rdd_b = rdd_b.map(lambda x: ((int(x[0]), int(x[1])), float(x[2])))
    matrix = ms.matmul(rdd_a, rdd_b).collect()

    import numpy as np
    result = np.zeros((4, 4))
    for item in matrix:
        result[item[0][0], item[0][1]] = item[1]
    print(result)







