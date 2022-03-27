import findspark
from pyspark.sql import SparkSession
import mathspark as ms

if __name__ == '__main__':
    findspark.init()

    ss = SparkSession \
        .builder \
        .appName("test") \
        .master("local") \
        .getOrCreate()
    ss.sparkContext.setLogLevel('ERROR')

    # result = ms.mv_mul('data/matrix.csv', 'data/vector.csv', session=ss)

    # result = ms.selection('data/foo.csv', condition=lambda x: x == 'A' or x == 'B', session=ss)

    rdd_a = ss.read.csv('data/a.csv', header=True).rdd
    rdd_b = ss.read.csv('data/b.csv', header=True).rdd
    rdd_a = rdd_a.map(lambda x: (int(x[0]), float(x[1])))
    rdd_b = rdd_b.map(lambda x: (int(x[0]), float(x[1])))
    # union
    # print(ms.union(rdd_a, rdd_b).take(20))

    # intersection
    print(ms.intersect(rdd_a, rdd_b).take(20))

    # difference
    # print(ms.difference(rdd_a, rdd_b).take(20))

    # join
    # print(ms.join(rdd_a, rdd_b).take(20))

    # # aggregation
    # rdd = ss.read.csv('data/foo.csv', header=True).rdd
    # print(ms.aggregate(rdd, lambda x, y: x + y).take(20))

    # # projection
    # rdd = ss.read.csv('data/foo.csv', header=True).rdd
    # print(ms.projection(rdd, [2, 3]).take(20))

    # # matmul
    # rdd_a = ss.read.csv('data/matrix.csv', header=True).rdd
    # rdd_b = ss.read.csv('data/matrix.csv', header=True).rdd
    # print(ms.matmul(rdd_a, rdd_b))





