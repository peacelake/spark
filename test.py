# spark-submit /users/shangj/Documents/github/python/test.py
# spark-submit ~/Documents/github/spark/test.py

from random import random
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

#Sample 1:
lines = sc.textFile("README.md").cache() #("hdfs://...")
print("Line count: %i" %lines.count())

#Sample 2:
numAs = lines.filter(lambda s: 'a' in s).count()
numBs = lines.filter(lambda s: 'b' in s).count()
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

#Sample 3:
def hasPython(line):
	return "Python" in line

pythonLines = lines.filter(hasPython) 
pythonLines.count()
pythonLines.first()

#Sample 4:
def sample(p):
    x, y = random(), random()
    return 1 if x*x + y*y < 1 else 0

NUM_SAMPLES = 1000000
count = sc.parallelize(xrange(0, NUM_SAMPLES)).map(sample) \
             .reduce(lambda a, b: a + b)
print "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES)

#Sample 6:
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
count = distData.reduce(lambda a, b: a + b)
print "Array count %i" %count



