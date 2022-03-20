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
    # union
    # print(ms.union(rdd_a, rdd_b).take(20))

    # intersection
    # print(ms.intersect(rdd_a, rdd_b).take(20))

    # difference
    # print(ms.difference(rdd_a, rdd_b).take(20))

    # join
    # print(ms.join(rdd_a, rdd_b).take(20))

    # aggregate
    rdd = ss.read.csv('data/foo.csv', header=True).rdd
    print(ms.aggregate(rdd, sum).take(20))


