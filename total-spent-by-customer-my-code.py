# [고객ID, 상품ID, 구매가] 고객ID별 총 구매가
from pyspark import SparkConf, SparkContext

def func(text):
    splitted = text.split(',')
    return (int(splitted[0]), float(splitted[2]))

conf = SparkConf().setMaster('local').setAppName('mycode')
sc = SparkContext(conf=conf)

input = sc.textFile("file:///home/ko/workspace/SparkCourse/customer-orders.csv")
line = input.map(lambda x: x.split(','))
keyvalues = line.map(lambda x: (int(x[0]), float(x[2]))).reduceByKey(lambda x, y: x+y)
sortreversedkv = keyvalues.map(lambda x: (x[1], x[0])).sortByKey()
results = sortreversedkv.collect()

for i, j in results:
    print("CustomerID: %d ~ \t Sum: %.2f" % (j, round(i, 2)))